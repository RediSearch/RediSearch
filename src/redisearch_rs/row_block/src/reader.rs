/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reading a block back: the inverse of [`crate::writer`], over the same byte layout.
//!
//! Two consumers: the coordinator, which decodes the blocks shards send it through
//! [`crate::RowBlockDecoder`], and the encoder's own fallback path, which replays a block it
//! just wrote as ordinary RESP rows. The input is untrusted in both: a block is bytes, and
//! there is no cheap way to prove the bytes came from this build.

use crate::{ColumnKind, MAGIC, MAX_NESTING_DEPTH, Tag, VERSION, bitmap_bytes, bitmap_get};
use std::ffi::CStr;
use value::{SharedBuffer, SharedValue, Value};

/// Why a block could not be decoded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    /// A field ran past the end of the block.
    #[error("block ends mid-field")]
    Truncated,

    /// The block does not open with [`MAGIC`].
    #[error("block opens with {magic:#010x} rather than the row block magic")]
    BadMagic {
        /// The four bytes found where the magic belongs.
        magic: u32,
    },

    /// The block is not the [`VERSION`] this build speaks.
    #[error("block declares format version {version}, which this build does not read")]
    UnsupportedVersion {
        /// The version byte found in the header.
        version: u8,
    },

    /// A schema name is not the NUL-terminated string of its declared length — either the
    /// terminator is missing or the name contains an interior NUL.
    #[error("schema name is not terminated at its declared length")]
    MalformedName,

    /// A tag byte no [`Tag`] uses. Its payload length is unknown, so the cursor cannot be
    /// advanced past it and the rest of the block is unreadable.
    #[error("value carries tag {tag}, which no version of this format writes")]
    UnknownTag {
        /// The unrecognised tag byte.
        tag: u8,
    },

    /// A string payload not followed by the NUL the layout puts after every string.
    #[error("string is not terminated at its declared length")]
    UnterminatedString,

    /// A schema kind byte no [`ColumnKind`] uses. Every value of the column would have an
    /// unknown layout, so no row can be read.
    #[error("column declares kind {kind}, which no version of this format writes")]
    UnknownColumnKind {
        /// The unrecognised kind byte.
        kind: u8,
    },

    /// A length or count field larger than the bytes left in the block could satisfy. Caught
    /// before it is used to size an allocation.
    #[error("length field of {count} exceeds what the rest of the block can hold")]
    ImplausibleCount {
        /// The offending length or element count.
        count: u32,
    },

    /// Nesting beyond [`MAX_NESTING_DEPTH`].
    #[error("value nests deeper than the format carries")]
    TooDeeplyNested,

    /// Bytes follow a schema declaring no columns. Rows would be zero bytes long there, so
    /// the trailing bytes cannot be split into rows at all — see the [crate] docs.
    #[error("block declares no columns yet carries row bytes")]
    RowsWithoutColumns,
}

/// A parsed block: its schema, plus the row bytes still to be decoded.
#[derive(Debug)]
pub struct Block<'a> {
    names: Vec<&'a CStr>,
    kinds: Vec<ColumnKind>,
    rows: &'a [u8],
}

/// One decoded row: the columns the row held a value for, in schema order.
#[derive(Debug)]
pub struct Row<'a> {
    fields: Vec<(&'a CStr, SharedValue)>,
}

impl<'a> Row<'a> {
    /// The row's present columns as name / value pairs, in schema order.
    pub fn fields(&self) -> &[(&'a CStr, SharedValue)] {
        &self.fields
    }
}

impl<'a> Block<'a> {
    /// Parses a block's header and schema, leaving its rows for [`Block::rows`].
    pub fn parse(bytes: &'a [u8]) -> Result<Self, DecodeError> {
        let mut cursor = Cursor { bytes };

        let magic = cursor.take_u32()?;
        if magic != MAGIC {
            return Err(DecodeError::BadMagic { magic });
        }
        let version = cursor.take_u8()?;
        if version != VERSION {
            return Err(DecodeError::UnsupportedVersion { version });
        }
        let ncols = cursor.take_u16()?;

        // Every column costs a length field, a terminator and a kind at the very least, so a
        // count the rest of the block cannot cover is corrupt — reject it before sizing
        // `names`.
        const MIN_BYTES_PER_COLUMN: usize = size_of::<u16>() + 2;
        if usize::from(ncols) * MIN_BYTES_PER_COLUMN > cursor.bytes.len() {
            return Err(DecodeError::Truncated);
        }

        let mut names = Vec::with_capacity(usize::from(ncols));
        let mut kinds = Vec::with_capacity(usize::from(ncols));
        for _ in 0..ncols {
            let name_len = usize::from(cursor.take_u16()?);
            let stored = cursor.take(name_len + 1)?;
            let name = CStr::from_bytes_with_nul(stored).map_err(|_| DecodeError::MalformedName)?;
            names.push(name);
            let kind = cursor.take_u8()?;
            kinds.push(ColumnKind::from_byte(kind).ok_or(DecodeError::UnknownColumnKind { kind })?);
        }

        if ncols == 0 && !cursor.bytes.is_empty() {
            return Err(DecodeError::RowsWithoutColumns);
        }

        Ok(Self {
            names,
            kinds,
            rows: cursor.bytes,
        })
    }

    /// The block's column names, in schema order.
    pub fn columns(&self) -> &[&'a CStr] {
        &self.names
    }

    /// The block's column kinds, in schema order.
    pub fn kinds(&self) -> &[ColumnKind] {
        &self.kinds
    }

    /// The bytes following the schema, for a [`RowReader`].
    pub const fn row_bytes(&self) -> &'a [u8] {
        self.rows
    }

    /// Decodes the block's rows, stopping at the first malformed one.
    pub fn rows(&self) -> Rows<'a, '_> {
        Rows {
            block: self,
            reader: RowReader::new(self.rows, &self.kinds),
        }
    }
}

/// Iterator over a [`Block`]'s rows, yielded by [`Block::rows`].
///
/// Fuses on the first error: a block that stops making sense mid-row cannot be resynchronised,
/// since row boundaries are implied by the values themselves.
#[derive(Debug)]
pub struct Rows<'a, 'block> {
    block: &'block Block<'a>,
    reader: RowReader<'a, 'block>,
}

impl<'a> Iterator for Rows<'a, '_> {
    type Item = Result<Row<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_exhausted() {
            return None;
        }
        let names = &self.block.names;
        let mut fields = Vec::new();
        let outcome = self
            .reader
            .read_row(|col, value| fields.push((names[usize::from(col)], value)));
        Some(outcome.map(|()| Row { fields }))
    }
}

impl std::iter::FusedIterator for Rows<'_, '_> {}

/// Decodes the rows section of a block one row at a time, handing each present field to a
/// caller-supplied sink instead of collecting it.
///
/// This is the primitive under both [`Rows`] and [`crate::RowBlockDecoder`]: the latter cannot
/// hold a [`Block`] across calls from C, so it rebuilds a reader over the bytes it has not
/// consumed yet on every row.
///
/// Fuses on the first error, for the reason given on [`Rows`].
#[derive(Debug)]
pub struct RowReader<'a, 'k> {
    cursor: Cursor<'a>,
    kinds: &'k [ColumnKind],
    strings: Strings<'k>,
    failed: bool,
}

/// Where a decoded string's bytes come from.
#[derive(Debug, Clone, Copy)]
enum Strings<'b> {
    /// Copied out of the block.
    Copied,
    /// Borrowed from the block, which this buffer holds — falling back to a copy for a string
    /// [`SharedBuffer::share`] cannot borrow.
    Shared(&'b SharedBuffer),
}

impl<'a, 'k> RowReader<'a, 'k> {
    /// A reader over `rows`, the bytes following the schema of a block whose columns have the
    /// given `kinds`.
    ///
    /// # Panics
    ///
    /// Panics if there are more `kinds` than a block can declare columns.
    pub fn new(rows: &'a [u8], kinds: &'k [ColumnKind]) -> Self {
        assert!(
            u16::try_from(kinds.len()).is_ok(),
            "a block's column count is a u16"
        );
        Self {
            cursor: Cursor { bytes: rows },
            kinds,
            strings: Strings::Copied,
            failed: false,
        }
    }

    /// Like [`RowReader::new`], but decoding strings as borrows of `buffer` instead of copies,
    /// so that the values keep the block alive rather than duplicating its bytes.
    ///
    /// # Panics
    ///
    /// Panics if `rows` is not part of `buffer`'s bytes, or for the reason
    /// [`RowReader::new`] does.
    pub fn sharing(rows: &'a [u8], kinds: &'k [ColumnKind], buffer: &'k SharedBuffer) -> Self {
        let whole = buffer.as_bytes().as_ptr_range();
        let part = rows.as_ptr_range();
        assert!(
            whole.start <= part.start && part.end <= whole.end,
            "the rows are not part of the shared buffer"
        );
        Self {
            strings: Strings::Shared(buffer),
            ..Self::new(rows, kinds)
        }
    }

    /// Whether no row is left to read, either because the bytes ran out on a row boundary or
    /// because an earlier row failed to decode.
    pub const fn is_exhausted(&self) -> bool {
        self.failed || self.cursor.bytes.is_empty()
    }

    /// The bytes after the last row read.
    pub const fn remaining(&self) -> &'a [u8] {
        self.cursor.bytes
    }

    /// Decodes the next row, calling `sink` with each present column's index and value, in
    /// schema order.
    ///
    /// On error `sink` may already have been called for the columns before the malformed one;
    /// the reader is then exhausted.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if the reader is already exhausted.
    pub fn read_row(&mut self, mut sink: impl FnMut(u16, SharedValue)) -> Result<(), DecodeError> {
        debug_assert!(!self.is_exhausted(), "no row is left to read");
        let outcome = self.read_row_inner(&mut sink);
        if outcome.is_err() {
            self.failed = true;
        }
        outcome
    }

    fn read_row_inner(
        &mut self,
        sink: &mut impl FnMut(u16, SharedValue),
    ) -> Result<(), DecodeError> {
        let ncols = self.kinds.len() as u16;
        let bitmap = self.cursor.take(bitmap_bytes(ncols))?;
        for (col, kind) in (0..ncols).zip(self.kinds) {
            if bitmap_get(bitmap, col) {
                let value = match kind {
                    ColumnKind::Tagged => decode_value(&mut self.cursor, self.strings, 0)?,
                    ColumnKind::Typed(tag) => {
                        decode_payload(&mut self.cursor, self.strings, *tag, 0)?
                    }
                };
                sink(col, value);
            }
        }
        Ok(())
    }
}

/// Decodes one tagged value nested `depth` levels below a row field.
fn decode_value(
    cursor: &mut Cursor<'_>,
    strings: Strings<'_>,
    depth: u32,
) -> Result<SharedValue, DecodeError> {
    let tag = cursor.take_tag()?;
    decode_payload(cursor, strings, tag, depth)
}

/// Decodes the payload of a value whose `tag` is already known, nested `depth` levels below
/// a row field.
fn decode_payload(
    cursor: &mut Cursor<'_>,
    strings: Strings<'_>,
    tag: Tag,
    depth: u32,
) -> Result<SharedValue, DecodeError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DecodeError::TooDeeplyNested);
    }

    Ok(match tag {
        Tag::Number => SharedValue::new_num(cursor.take_f64()?),
        Tag::String => {
            let bytes = cursor.take_string()?;
            let shared = match strings {
                Strings::Copied => None,
                Strings::Shared(buffer) => {
                    let offset = bytes.as_ptr() as usize - buffer.as_bytes().as_ptr() as usize;
                    let len = u32::try_from(bytes.len()).expect("a string length is a u32");
                    buffer.share(offset, len)
                }
            };
            match shared {
                Some(string) => SharedValue::new(Value::String(string)),
                None => SharedValue::new_string(bytes.to_vec()),
            }
        }
        Tag::Null => SharedValue::null_static(),
        Tag::Array => {
            let count = cursor.take_count(MIN_BYTES_PER_VALUE)?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(cursor, strings, depth + 1)?);
            }
            SharedValue::new_array(items)
        }
        Tag::Map => {
            let count = cursor.take_count(2 * MIN_BYTES_PER_VALUE)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decode_value(cursor, strings, depth + 1)?;
                let value = decode_value(cursor, strings, depth + 1)?;
                entries.push((key, value));
            }
            SharedValue::new_map(entries)
        }
    })
}

/// The length of the value of `kind` at the start of `bytes`, without decoding it.
///
/// For the writer, which re-encodes rows it already wrote when a column's kind changes.
pub(crate) fn value_len(bytes: &[u8], kind: ColumnKind) -> Result<usize, DecodeError> {
    let mut cursor = Cursor { bytes };
    match kind {
        ColumnKind::Tagged => skip_value(&mut cursor, 0)?,
        ColumnKind::Typed(tag) => skip_payload(&mut cursor, tag, 0)?,
    }
    Ok(bytes.len() - cursor.bytes.len())
}

/// Steps over one tagged value; the skipping counterpart of [`decode_value`].
fn skip_value(cursor: &mut Cursor<'_>, depth: u32) -> Result<(), DecodeError> {
    let tag = cursor.take_tag()?;
    skip_payload(cursor, tag, depth)
}

/// Steps over one payload; the skipping counterpart of [`decode_payload`].
fn skip_payload(cursor: &mut Cursor<'_>, tag: Tag, depth: u32) -> Result<(), DecodeError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DecodeError::TooDeeplyNested);
    }
    match tag {
        Tag::Number => {
            cursor.take_f64()?;
        }
        Tag::String => {
            cursor.take_string()?;
        }
        Tag::Null => {}
        Tag::Array => {
            for _ in 0..cursor.take_count(MIN_BYTES_PER_VALUE)? {
                skip_value(cursor, depth + 1)?;
            }
        }
        Tag::Map => {
            for _ in 0..cursor.take_count(2 * MIN_BYTES_PER_VALUE)? {
                skip_value(cursor, depth + 1)?;
                skip_value(cursor, depth + 1)?;
            }
        }
    }
    Ok(())
}

/// The fewest bytes a tagged value can occupy: a bare [`Tag::Null`] is its tag alone.
const MIN_BYTES_PER_VALUE: usize = 1;

/// Little-endian read head over a block's bytes.
#[derive(Debug)]
struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    /// Consumes `n` bytes.
    const fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let (taken, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(taken)
    }

    /// Consumes a fixed-width little-endian scalar.
    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let taken = self.take(N)?;
        Ok(taken.try_into().expect("`take` yields exactly `N` bytes"))
    }

    fn take_u8(&mut self) -> Result<u8, DecodeError> {
        Ok(u8::from_le_bytes(self.take_array()?))
    }

    fn take_u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    fn take_u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    fn take_f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.take_array()?))
    }

    /// Consumes a [`Tag::String`] payload, returning the string without its NUL.
    fn take_string(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.take_count(1)?;
        let stored = self.take(len + 1)?;
        match stored.split_last() {
            Some((0, string)) => Ok(string),
            _ => Err(DecodeError::UnterminatedString),
        }
    }

    /// Consumes a value's tag byte.
    fn take_tag(&mut self) -> Result<Tag, DecodeError> {
        let byte = self.take_u8()?;
        Tag::from_byte(byte).ok_or(DecodeError::UnknownTag { tag: byte })
    }

    /// Consumes one of the layout's `u32` length fields, rejecting a count the remaining bytes
    /// cannot possibly satisfy at `bytes_per_element` bytes apiece.
    ///
    /// Without this a hostile count would be handed straight to `Vec::with_capacity`, turning
    /// a few corrupt bytes into a multi-gigabyte allocation.
    fn take_count(&mut self, bytes_per_element: usize) -> Result<usize, DecodeError> {
        let count = self.take_u32()?;
        if (count as usize).saturating_mul(bytes_per_element) > self.bytes.len() {
            return Err(DecodeError::ImplausibleCount { count });
        }
        Ok(count as usize)
    }
}
