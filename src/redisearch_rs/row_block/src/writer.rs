/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Building a block: see the [crate] docs for the byte layout it produces.

use crate::{MAGIC, MAX_NESTING_DEPTH, Tag, VERSION, bitmap_bytes};
use rlookup::{RLookup, RLookupKey, RLookupKeyFlags, RLookupRow};
use value::Value;

/// Starting capacity of a fresh [`RowBlockWriter`]'s buffer.
///
/// This only sizes the first chunk a writer builds; [`RowBlockWriter::reset`] keeps whatever
/// capacity later chunks grew it to.
const INITIAL_CAPACITY: usize = 8192;

/// Which subset of a lookup's keys a block carries as its columns.
///
/// Mirrors the predicate the RESP row serializer (`RedisModule_Reply_RLookupRow`) applies, so
/// that a chunk carries the same fields whichever encoding it ends up using. Keys a lookup has
/// tombstoned are skipped by [`RLookup::iter`] itself and need no flag to exclude them.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnFilter {
    /// A key is a column only if it carries *every* one of these flags.
    pub required: RLookupKeyFlags,
    /// A key is a column only if it carries *none* of these flags.
    pub excluded: RLookupKeyFlags,
}

impl ColumnFilter {
    /// Whether `key` is one of the columns this filter selects.
    fn accepts(&self, key: &RLookupKey<'_>) -> bool {
        key.flags.contains(self.required) && !key.flags.intersects(self.excluded)
    }
}

/// Which member of a [`Value::Trio`] a row field resolves to.
///
/// A trio bundles the three shapes a multi-value document field can reply as. Only a field the
/// row stores as a trio *directly* gets this choice: nested trios take
/// [`TrioMember::Middle`] unconditionally, which is what the RESP path does and what
/// [`RowBlockWriter::write_row`] therefore reproduces.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TrioMember {
    /// The single-value form, replied to clients predating the multi-value API.
    Left,
    /// The multi-value form.
    Middle,
    /// The expanded form, requested with `FORMAT EXPAND`.
    Right,
}

/// Why a schema could not be encoded, leaving the caller to reply in RESP instead.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchemaError {
    /// A column name longer than the `u16` the layout gives its length. Refused rather than
    /// truncated: the full name plus its terminator follows the length on the wire, so a
    /// truncated length would leave the decoder reading a name's tail as the next field.
    #[error("column name of {len} bytes exceeds the encodable maximum")]
    NameTooLong {
        /// The offending name's length in bytes, excluding its NUL terminator.
        len: usize,
    },

    /// More columns than the `u16` the header gives their count.
    #[error("more columns than the block header can count")]
    TooManyColumns,
}

/// Why a row could not be encoded.
///
/// Nothing is appended for a refused row: the block still holds exactly the rows written
/// before it, so the caller may emit it as is or discard it, but must not treat the refused
/// row as encoded. Encoding an unrepresentable value as null instead would silently destroy
/// it, which is the one outcome a wire format may never produce.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RefusedRow {
    /// A value still held an indirection after resolution — only reachable if [`Value`] grew
    /// a variant that indirects and the resolution step was not extended with it.
    #[error("value still holds an unresolved indirection")]
    UnresolvedIndirection,

    /// A string, array or map whose length does not fit the `u32` the layout gives it.
    #[error("value length of {len} exceeds the encodable maximum")]
    ValueTooLong {
        /// The offending byte length, element count or entry count.
        len: usize,
    },

    /// Nesting beyond [`MAX_NESTING_DEPTH`].
    #[error("value nests deeper than the format carries")]
    TooDeeplyNested,

    /// The lookup no longer selects exactly the columns the schema declared, so the presence
    /// bitmap cannot describe the row. Only reachable if the lookup or the filter changed
    /// between [`RowBlockWriter::write_schema`] and this row.
    #[error("lookup no longer matches the schema's column count")]
    ColumnCountChanged,
}

/// Growable output buffer for building one block.
///
/// Reused from chunk to chunk via [`RowBlockWriter::reset`], so the per-chunk allocation cost
/// is amortised to zero after the first.
///
/// Every chunk starts with [`RowBlockWriter::write_schema`], which fixes the columns, and
/// then appends rows with [`RowBlockWriter::write_row`]. [`RowBlockWriter::as_bytes`] is the
/// finished block.
///
/// Opaque to C: the writer owns a heap buffer, so C may only hold a pointer to one and pass
/// it back to the `row_block_ffi` entrypoints.
#[cheadergen::config(export, opaque)]
#[derive(Debug)]
pub struct RowBlockWriter {
    buf: Vec<u8>,
    /// Rows appended since the last reset, so a replay path can check it re-emitted every row
    /// the block held.
    nrows: usize,
    /// Recorded by [`RowBlockWriter::write_schema`] so rows cannot be selected by a different
    /// predicate than the schema was.
    filter: ColumnFilter,
    /// Columns declared in the header, for bitmap sizing.
    ncols: u16,
    /// Set once the header and schema have been written.
    header_written: bool,
}

impl Default for RowBlockWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBlockWriter {
    /// Creates a writer with no header written yet.
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(INITIAL_CAPACITY),
            nrows: 0,
            filter: ColumnFilter::default(),
            ncols: 0,
            header_written: false,
        }
    }

    /// Discards the block, keeping the allocated capacity for the next one.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.nrows = 0;
        self.filter = ColumnFilter::default();
        self.ncols = 0;
        self.header_written = false;
    }

    /// The block built so far, ready to be sent.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    /// How many rows [`RowBlockWriter::write_row`] has accepted since the last reset.
    pub const fn nrows(&self) -> usize {
        self.nrows
    }

    /// Writes the header and the schema `filter` selects from `lookup`, and returns how many
    /// columns it declares. Must be called once per chunk, before any row.
    ///
    /// A zero column count means this chunk cannot be encoded at all — see the [crate] docs —
    /// and so does any error. Either way the writer is left empty, so the caller can only
    /// reply in RESP.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if a schema has already been written.
    pub fn write_schema(
        &mut self,
        lookup: &RLookup<'_>,
        filter: ColumnFilter,
    ) -> Result<u16, SchemaError> {
        debug_assert!(
            !self.header_written,
            "a chunk's schema must be written exactly once"
        );

        match self.append_schema(lookup, filter) {
            Ok(ncols) => {
                self.filter = filter;
                self.ncols = ncols;
                self.header_written = true;
                Ok(ncols)
            }
            Err(error) => {
                self.reset();
                Err(error)
            }
        }
    }

    /// Appends the header and schema, leaving a half-written block behind on error for
    /// [`RowBlockWriter::write_schema`] to discard.
    fn append_schema(
        &mut self,
        lookup: &RLookup<'_>,
        filter: ColumnFilter,
    ) -> Result<u16, SchemaError> {
        self.buf.extend_from_slice(&MAGIC.to_le_bytes());
        self.buf.push(VERSION);

        // The column count is not known until the keys have been walked, so reserve its slot
        // and backpatch rather than iterating twice.
        let ncols_at = self.buf.len();
        self.buf.extend_from_slice(&0u16.to_le_bytes());

        let mut ncols: u16 = 0;
        for key in lookup.iter().filter(|key| filter.accepts(key)) {
            let name = key.name().to_bytes();
            let name_len = u16::try_from(name.len())
                .map_err(|_| SchemaError::NameTooLong { len: name.len() })?;
            ncols = ncols.checked_add(1).ok_or(SchemaError::TooManyColumns)?;

            self.buf.extend_from_slice(&name_len.to_le_bytes());
            self.buf.extend_from_slice(name);
            // The terminator lets a decoder resolve the name by pointing straight into the
            // block; `CStr` is why the name itself cannot contain one.
            self.buf.push(0);
        }

        self.buf[ncols_at..ncols_at + size_of::<u16>()].copy_from_slice(&ncols.to_le_bytes());
        Ok(ncols)
    }

    /// Appends one row, reading the schema's columns out of `row`.
    ///
    /// `trio` is the member a field stored as a [`Value::Trio`] resolves to; see
    /// [`TrioMember`].
    ///
    /// On error nothing is appended for the row — see [`RefusedRow`].
    ///
    /// # Panics
    ///
    /// Panics in debug builds if no schema has been written yet.
    pub fn write_row(
        &mut self,
        lookup: &RLookup<'_>,
        row: &RLookupRow<'_>,
        trio: TrioMember,
    ) -> Result<(), RefusedRow> {
        debug_assert!(
            self.header_written,
            "a row cannot be written before the chunk's schema"
        );

        // The bitmap sits at the row's first byte, so this doubles as the rollback point.
        let row_at = self.buf.len();
        self.buf.resize(row_at + bitmap_bytes(self.ncols), 0);

        match self.append_row(lookup, row, trio, row_at) {
            Ok(()) => {
                self.nrows += 1;
                Ok(())
            }
            Err(error) => {
                // Roll the half-written row back so the block ends on a row boundary and stays
                // decodable, whether the caller emits it or throws it away.
                self.buf.truncate(row_at);
                Err(error)
            }
        }
    }

    /// Appends a row's presence bits and values, with the bitmap already zeroed at `row_at`.
    fn append_row(
        &mut self,
        lookup: &RLookup<'_>,
        row: &RLookupRow<'_>,
        trio: TrioMember,
        row_at: usize,
    ) -> Result<(), RefusedRow> {
        let (filter, ncols) = (self.filter, self.ncols);

        let mut col: u16 = 0;
        for key in lookup.iter().filter(|key| filter.accepts(key)) {
            if col >= ncols {
                return Err(RefusedRow::ColumnCountChanged);
            }
            if let Some(value) = row.get(key) {
                self.buf[row_at + col as usize / 8] |= 1u8 << (col % 8);
                self.append_field(value, trio)?;
            }
            col += 1;
        }

        if col != ncols {
            return Err(RefusedRow::ColumnCountChanged);
        }
        Ok(())
    }

    /// Appends one top-level row field.
    fn append_field(&mut self, value: &Value, trio: TrioMember) -> Result<(), RefusedRow> {
        // The RESP row serializer applies the three-way trio choice exactly once, and only to
        // a value the row stores as a trio directly: its trio test does not follow references,
        // and everything below a field is emitted by the generic value serializer, whose own
        // trio case takes the middle member whatever was asked for. `append_value` is the
        // mirror of that generic path, so resolving here and recursing there is the whole of
        // the distinction.
        let resolved = match value {
            Value::Trio(members) => match trio {
                TrioMember::Left => members.left(),
                TrioMember::Middle => members.middle(),
                TrioMember::Right => members.right(),
            },
            other => return self.append_value(other, 0),
        };
        self.append_value(resolved, 0)
    }

    /// Appends one tagged value nested `depth` levels below a row field.
    fn append_value(&mut self, value: &Value, depth: u32) -> Result<(), RefusedRow> {
        if depth > MAX_NESTING_DEPTH {
            return Err(RefusedRow::TooDeeplyNested);
        }

        match resolve_nested(value) {
            Value::Number(number) => {
                self.buf.push(Tag::Number as u8);
                self.buf.extend_from_slice(&number.to_le_bytes());
            }
            Value::String(string) => self.append_string(string.as_bytes())?,
            Value::RedisString(string) => self.append_string(string.as_bytes())?,
            Value::Array(array) => {
                self.buf.push(Tag::Array as u8);
                self.append_count(array.len())?;
                for item in array.iter() {
                    self.append_value(item, depth + 1)?;
                }
            }
            Value::Map(map) => {
                self.buf.push(Tag::Map as u8);
                self.append_count(map.len())?;
                for (key, value) in map.iter() {
                    self.append_value(key, depth + 1)?;
                    self.append_value(value, depth + 1)?;
                }
            }
            // `Undefined` is what the RESP path replies as null too, and carries nothing to
            // lose.
            Value::Null | Value::Undefined => self.buf.push(Tag::Null as u8),
            // Deliberately no catch-all arm, so that a variant added to `Value` is a compile
            // error here instead of quietly taking a lossy path. These two are resolved away
            // by `resolve_nested`, which is where a new indirecting variant belongs.
            Value::Ref(_) | Value::Trio(_) => return Err(RefusedRow::UnresolvedIndirection),
        }
        Ok(())
    }

    /// Appends a [`Tag::String`] payload.
    fn append_string(&mut self, bytes: &[u8]) -> Result<(), RefusedRow> {
        self.buf.push(Tag::String as u8);
        self.append_count(bytes.len())?;
        self.buf.extend_from_slice(bytes);
        Ok(())
    }

    /// Appends one of the layout's `u32` length fields.
    fn append_count(&mut self, count: usize) -> Result<(), RefusedRow> {
        let count = u32::try_from(count).map_err(|_| RefusedRow::ValueTooLong { len: count })?;
        self.buf.extend_from_slice(&count.to_le_bytes());
        Ok(())
    }
}

/// Follows the indirection a nested value carries, the way the generic RESP value serializer
/// does: references are dereferenced, and a trio resolves to its middle member.
fn resolve_nested(mut value: &Value) -> &Value {
    loop {
        match value {
            Value::Ref(inner) => value = inner,
            Value::Trio(members) => value = members.middle(),
            _ => return value,
        }
    }
}
