/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Building a block in the [crate]-level layout.

use crate::{
    ColumnKind, MAGIC, MAX_NESTING_DEPTH, Tag, VERSION, bitmap_bytes, bitmap_get, reader::value_len,
};
use rlookup::{RLookup, RLookupKey, RLookupKeyFlags, RLookupRow};
use value::Value;

/// Sizes only a writer's first chunk; [`RowBlockWriter::reset`] keeps the capacity later chunks grew.
const INITIAL_CAPACITY: usize = 8192;

/// Maximum capacity of each byte buffer retained by a [`RowBlockWriter`].
const MAX_BUFFER_CAPACITY: usize = 32 * 1024 * 1024;

/// Reserves before writing so both payload length and speculative growth stay bounded.
fn reserve_bytes(buf: &mut Vec<u8>, additional: usize) -> Result<(), BufferFull> {
    let required = buf.len().checked_add(additional).ok_or(BufferFull)?;
    if required > MAX_BUFFER_CAPACITY {
        return Err(BufferFull);
    }
    if required > buf.capacity() {
        if buf.capacity() > MAX_BUFFER_CAPACITY / 2 {
            // Pay the final growth once rather than reallocating for each later field.
            buf.reserve_exact(MAX_BUFFER_CAPACITY - buf.len());
        } else {
            buf.reserve(additional);
        }
    }
    Ok(())
}

#[derive(Debug)]
struct BufferFull;

/// Which of a lookup's keys become columns: the same predicate `RedisModule_Reply_RLookupRow` applies, so a chunk
/// carries the same fields in either encoding.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnFilter {
    /// Flags a column key must all carry.
    pub required: RLookupKeyFlags,
    /// Flags a column key must carry none of.
    pub excluded: RLookupKeyFlags,
}

impl ColumnFilter {
    fn accepts(&self, key: &RLookupKey<'_>) -> bool {
        key.flags.contains(self.required) && !key.flags.intersects(self.excluded)
    }
}

/// Which member of a [`Value::Trio`] stored directly in a row field is written. Nested trios always take
/// [`TrioMember::Middle`], as on the RESP path.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TrioMember {
    /// The single-value form, for clients predating the multi-value API.
    Left,
    Middle,
    /// The `FORMAT EXPAND` form.
    Right,
}

/// Why a schema could not be encoded; the caller replies in RESP instead.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SchemaError {
    /// The schema would exceed the writer's byte-buffer bound.
    #[error("schema exceeds the row block byte-buffer bound")]
    BufferFull,

    /// A column name longer than its `u16` length field can hold.
    #[error("column name of {len} bytes exceeds the encodable maximum")]
    NameTooLong { len: usize },

    #[error("more columns than the block header can count")]
    TooManyColumns,
}

/// Why a row could not be encoded. Nothing is appended for a refused row, which is never written lossily instead.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RefusedRow {
    /// The row or its retagging would exceed the writer's byte-buffer bound.
    #[error("row exceeds the row block byte-buffer bound")]
    BufferFull,

    /// Only reachable if [`Value`] grows an indirecting variant `resolve_nested` does not follow.
    #[error("value still holds an unresolved indirection")]
    UnresolvedIndirection,

    /// A string, array or map length that does not fit its `u32` field.
    #[error("value length of {len} exceeds the encodable maximum")]
    ValueTooLong { len: usize },

    #[error("value nests deeper than the format carries")]
    TooDeeplyNested,

    /// The lookup gained or lost columns since [`RowBlockWriter::write_schema`].
    #[error("lookup no longer matches the schema's column count")]
    ColumnCountChanged,
}

/// Builds one block per chunk: [`RowBlockWriter::write_schema`], then [`RowBlockWriter::write_row`] per row. Reused
/// across chunks via [`RowBlockWriter::reset`].
#[cheadergen::config(export, opaque)]
#[derive(Debug)]
pub struct RowBlockWriter {
    buf: Vec<u8>,
    nrows: usize,
    /// The schema's filter, so rows are selected by the same predicate.
    filter: ColumnFilter,
    ncols: u16,
    header_written: bool,
    columns: Vec<Column>,
    rows_at: usize,
    /// The pre-row state of each column the current row changed: restored if the row is refused, and the kinds earlier
    /// rows are re-encoded from if it is accepted.
    undo: Vec<(u16, Column)>,
    /// Kept for its capacity: re-encoding builds into it and swaps it with `buf`.
    spare: Vec<u8>,
}

#[derive(Copy, Clone, Debug)]
struct Column {
    kind: ColumnKind,
    /// Until a value fixes it, [`Column::kind`] is a placeholder no row contradicts.
    fixed: bool,
    /// Offset of the kind byte, for backpatching.
    kind_at: usize,
}

impl Default for RowBlockWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RowBlockWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(INITIAL_CAPACITY),
            nrows: 0,
            filter: ColumnFilter::default(),
            ncols: 0,
            header_written: false,
            columns: Vec::new(),
            rows_at: 0,
            undo: Vec::new(),
            spare: Vec::new(),
        }
    }

    /// Discards the block, keeping the capacity.
    pub fn reset(&mut self) {
        self.buf.clear();
        self.nrows = 0;
        self.filter = ColumnFilter::default();
        self.ncols = 0;
        self.header_written = false;
        self.columns.clear();
        self.rows_at = 0;
        self.undo.clear();
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub const fn nrows(&self) -> usize {
        self.nrows
    }

    /// Writes the header and the schema `filter` selects from `lookup`, returning the column count. Zero columns or an
    /// error leave the writer empty, and the caller replies in RESP.
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
            // No rows can follow a schema with no columns, so leave the writer empty rather than half-written.
            Ok(0) => {
                self.reset();
                Ok(0)
            }
            Ok(ncols) => {
                self.filter = filter;
                self.ncols = ncols;
                self.header_written = true;
                self.rows_at = self.buf.len();
                Ok(ncols)
            }
            Err(error) => {
                self.reset();
                Err(error)
            }
        }
    }

    fn append_schema(
        &mut self,
        lookup: &RLookup<'_>,
        filter: ColumnFilter,
    ) -> Result<u16, SchemaError> {
        reserve_bytes(&mut self.buf, size_of::<u32>() + 1 + size_of::<u16>())
            .map_err(|_| SchemaError::BufferFull)?;
        self.buf.extend_from_slice(&MAGIC.to_le_bytes());
        self.buf.push(VERSION);

        // Backpatched once the keys have been walked.
        let ncols_at = self.buf.len();
        self.buf.extend_from_slice(&0u16.to_le_bytes());

        let mut ncols: u16 = 0;
        for key in lookup.iter().filter(|key| filter.accepts(key)) {
            let name = key.name().to_bytes();
            let name_len = u16::try_from(name.len())
                .map_err(|_| SchemaError::NameTooLong { len: name.len() })?;
            ncols = ncols.checked_add(1).ok_or(SchemaError::TooManyColumns)?;

            reserve_bytes(&mut self.buf, size_of::<u16>() + name.len() + 2)
                .map_err(|_| SchemaError::BufferFull)?;
            self.buf.extend_from_slice(&name_len.to_le_bytes());
            self.buf.extend_from_slice(name);
            self.buf.push(0);

            let column = Column {
                kind: ColumnKind::Typed(Tag::Null),
                fixed: false,
                kind_at: self.buf.len(),
            };
            self.buf.push(column.kind.to_byte());
            self.columns.push(column);
        }

        self.buf[ncols_at..ncols_at + size_of::<u16>()].copy_from_slice(&ncols.to_le_bytes());
        Ok(ncols)
    }

    /// Appends one row. A column's first value fixes its kind; a later value of another [`Tag`] turns it
    /// [`ColumnKind::Tagged`] and re-encodes the rows written so far (at most once per column per block). A refused
    /// row changes neither the bytes nor any column kind.
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

        let row_at = self.buf.len();
        reserve_bytes(&mut self.buf, bitmap_bytes(self.ncols))
            .map_err(|_| RefusedRow::BufferFull)?;
        self.buf.resize(row_at + bitmap_bytes(self.ncols), 0);
        self.undo.clear();

        let result = self.append_row(lookup, row, trio, row_at).and_then(|()| {
            if self.undo.iter().any(|entry| self.retags(entry)) {
                self.retag_rows_before(row_at)?;
            }
            Ok(())
        });
        match result {
            Ok(()) => {
                self.nrows += 1;
                Ok(())
            }
            Err(error) => {
                self.buf.truncate(row_at);
                for (col, before) in self.undo.drain(..) {
                    self.columns[usize::from(col)] = before;
                    self.buf[before.kind_at] = before.kind.to_byte();
                }
                Err(error)
            }
        }
    }

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
                self.append_field(col, value, trio)?;
            }
            col += 1;
        }

        if col != ncols {
            return Err(RefusedRow::ColumnCountChanged);
        }
        Ok(())
    }

    /// Appends a row field, fixing or widening its column's kind. Earlier rows are re-encoded only once the row is
    /// accepted, by [`RowBlockWriter::retag_rows_before`].
    fn append_field(
        &mut self,
        col: u16,
        value: &Value,
        trio: TrioMember,
    ) -> Result<(), RefusedRow> {
        // As in the RESP row serializer: only a trio stored directly in the field (not behind a reference) gets the
        // three-way choice; `resolve_nested` mirrors the generic value serializer for everything else.
        let top = match value {
            Value::Trio(members) => match trio {
                TrioMember::Left => members.left(),
                TrioMember::Middle => members.middle(),
                TrioMember::Right => members.right(),
            },
            other => other,
        };
        let value = resolve_nested(top);
        let tag = tag_of(value)?;

        let column = self.columns[usize::from(col)];
        let kind = match column.kind {
            _ if !column.fixed => ColumnKind::Typed(tag),
            ColumnKind::Typed(typed) if typed != tag => ColumnKind::Tagged,
            kind => kind,
        };
        if !column.fixed || kind != column.kind {
            self.undo.push((col, column));
            self.columns[usize::from(col)] = Column {
                kind,
                fixed: true,
                ..column
            };
            self.buf[column.kind_at] = kind.to_byte();
        }

        if kind == ColumnKind::Tagged {
            reserve_bytes(&mut self.buf, 1).map_err(|_| RefusedRow::BufferFull)?;
            self.buf.push(tag as u8);
        }
        self.append_payload(value, 0)
    }

    /// Whether the current row widened the column `undone` from a typed kind to [`ColumnKind::Tagged`].
    fn retags(&self, (undone, before): &(u16, Column)) -> bool {
        before.fixed
            && before.kind != ColumnKind::Tagged
            && self.columns[usize::from(*undone)].kind == ColumnKind::Tagged
    }

    /// Re-encodes the rows before `row_at` for the columns the row at `row_at` retagged. A typed value is its tagged
    /// encoding minus the tag, so this splices the old tag in front of each such value and copies the rest.
    fn retag_rows_before(&mut self, row_at: usize) -> Result<(), RefusedRow> {
        // The kinds the earlier rows were written under.
        let mut before: Vec<ColumnKind> = self.columns.iter().map(|column| column.kind).collect();
        for (col, column) in &self.undo {
            before[usize::from(*col)] = column.kind;
        }
        let mut splice: Vec<Option<Tag>> = vec![None; usize::from(self.ncols)];
        for entry in self.undo.iter().filter(|entry| self.retags(entry)) {
            if let ColumnKind::Typed(tag) = entry.1.kind {
                splice[usize::from(entry.0)] = Some(tag);
            }
        }

        let mut out = std::mem::take(&mut self.spare);
        out.clear();
        let result = (|| {
            // Missing fields need no tag. This upper bound is only a reservation hint;
            // the writes below decide whether the actual encoding fits.
            let estimate = self
                .buf
                .len()
                .saturating_add(self.nrows.saturating_mul(splice.iter().flatten().count()));
            reserve_bytes(&mut out, estimate.min(MAX_BUFFER_CAPACITY))?;
            out.extend_from_slice(&self.buf[..self.rows_at]);

            let bitmap_len = bitmap_bytes(self.ncols);
            let mut at = self.rows_at;
            while at < row_at {
                let bitmap = &self.buf[at..at + bitmap_len];
                reserve_bytes(&mut out, bitmap_len)?;
                out.extend_from_slice(bitmap);
                at += bitmap_len;
                for col in (0..self.ncols).filter(|col| bitmap_get(bitmap, *col)) {
                    let len = value_len(&self.buf[at..row_at], before[usize::from(col)])
                        .expect("the writer only appends rows it can read back");
                    let tag = splice[usize::from(col)];
                    reserve_bytes(&mut out, len + usize::from(tag.is_some()))?;
                    if let Some(tag) = tag {
                        out.push(tag as u8);
                    }
                    out.extend_from_slice(&self.buf[at..at + len]);
                    at += len;
                }
            }

            reserve_bytes(&mut out, self.buf.len() - row_at)?;
            out.extend_from_slice(&self.buf[row_at..]);
            Ok::<_, BufferFull>(())
        })();
        match result {
            Ok(()) => self.spare = std::mem::replace(&mut self.buf, out),
            Err(_) => {
                out.clear();
                self.spare = out;
                return Err(RefusedRow::BufferFull);
            }
        }
        Ok(())
    }

    fn append_value(&mut self, value: &Value, depth: u32) -> Result<(), RefusedRow> {
        let value = resolve_nested(value);
        let tag = tag_of(value)?;
        reserve_bytes(&mut self.buf, 1).map_err(|_| RefusedRow::BufferFull)?;
        self.buf.push(tag as u8);
        self.append_payload(value, depth)
    }

    fn append_payload(&mut self, value: &Value, depth: u32) -> Result<(), RefusedRow> {
        if depth > MAX_NESTING_DEPTH {
            return Err(RefusedRow::TooDeeplyNested);
        }

        match value {
            Value::Number(number) => {
                reserve_bytes(&mut self.buf, size_of::<f64>())
                    .map_err(|_| RefusedRow::BufferFull)?;
                self.buf.extend_from_slice(&number.to_le_bytes());
            }
            Value::String(string) => self.append_string(string.as_bytes())?,
            Value::RedisString(string) => self.append_string(string.as_bytes())?,
            Value::Array(array) => {
                self.append_count(array.len())?;
                for item in array.iter() {
                    self.append_value(item, depth + 1)?;
                }
            }
            Value::Map(map) => {
                self.append_count(map.len())?;
                for (key, value) in map.iter() {
                    self.append_value(key, depth + 1)?;
                    self.append_value(value, depth + 1)?;
                }
            }
            Value::Null | Value::Undefined => {}
            Value::Ref(_) | Value::Trio(_) => return Err(RefusedRow::UnresolvedIndirection),
        }
        Ok(())
    }

    fn append_string(&mut self, bytes: &[u8]) -> Result<(), RefusedRow> {
        self.append_count(bytes.len())?;
        let additional = bytes.len().checked_add(1).ok_or(RefusedRow::BufferFull)?;
        reserve_bytes(&mut self.buf, additional).map_err(|_| RefusedRow::BufferFull)?;
        self.buf.extend_from_slice(bytes);
        self.buf.push(0);
        Ok(())
    }

    fn append_count(&mut self, count: usize) -> Result<(), RefusedRow> {
        let count = u32::try_from(count).map_err(|_| RefusedRow::ValueTooLong { len: count })?;
        reserve_bytes(&mut self.buf, size_of::<u32>()).map_err(|_| RefusedRow::BufferFull)?;
        self.buf.extend_from_slice(&count.to_le_bytes());
        Ok(())
    }
}

const fn tag_of(value: &Value) -> Result<Tag, RefusedRow> {
    Ok(match value {
        Value::Number(_) => Tag::Number,
        Value::String(_) | Value::RedisString(_) => Tag::String,
        // RESP replies `Undefined` as null too.
        Value::Null | Value::Undefined => Tag::Null,
        Value::Array(_) => Tag::Array,
        Value::Map(_) => Tag::Map,
        // No catch-all arm, so a new `Value` variant is a compile error here. These two are resolved by
        // `resolve_nested`.
        Value::Ref(_) | Value::Trio(_) => return Err(RefusedRow::UnresolvedIndirection),
    })
}

/// Follows references, and resolves a trio to its middle member, as the generic RESP value serializer does.
fn resolve_nested(mut value: &Value) -> &Value {
    loop {
        match value {
            Value::Ref(inner) => value = inner,
            Value::Trio(members) => value = members.middle(),
            _ => return value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_BUFFER_CAPACITY, reserve_bytes};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn reserve_caps_capacity_even_when_geometric_growth_would_cross_the_limit() {
        let mut buf = Vec::with_capacity(MAX_BUFFER_CAPACITY / 2 + 1);
        buf.resize(buf.capacity(), 0);
        reserve_bytes(&mut buf, 1).unwrap();
        assert!(buf.capacity() <= MAX_BUFFER_CAPACITY);
        let remaining = MAX_BUFFER_CAPACITY - buf.len();
        reserve_bytes(&mut buf, remaining).unwrap();
        assert_eq!(buf.capacity(), MAX_BUFFER_CAPACITY);
        let capacity = buf.capacity();
        assert!(reserve_bytes(&mut buf, usize::MAX).is_err());
        assert_eq!(buf.capacity(), capacity);
    }
}
