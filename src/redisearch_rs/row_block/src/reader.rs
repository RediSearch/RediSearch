/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reading a block back. Used by [`crate::RowBlockDecoder`] on the coordinator and by the shard's RESP replay fallback;
//! the input is treated as untrusted in both.

use crate::{ColumnKind, MAGIC, MAX_NESTING_DEPTH, Tag, VERSION, bitmap_bytes, bitmap_get};
use std::ffi::CStr;
use value::{SharedBuffer, SharedValue, Value};

/// Why a block could not be decoded.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("block ends mid-field")]
    Truncated,

    #[error("block opens with {magic:#010x} rather than the row block magic")]
    BadMagic { magic: u32 },

    #[error("block declares format version {version}, which this build does not read")]
    UnsupportedVersion { version: u8 },

    /// The terminator is missing, or the name contains an interior NUL.
    #[error("schema name is not terminated at its declared length")]
    MalformedName,

    /// Its payload length is unknown, so nothing after it can be read.
    #[error("value carries tag {tag}, which no version of this format writes")]
    UnknownTag { tag: u8 },

    #[error("string is not terminated at its declared length")]
    UnterminatedString,

    #[error("column declares kind {kind}, which no version of this format writes")]
    UnknownColumnKind { kind: u8 },

    /// A length or count larger than the rest of the block could hold, caught before it sizes an allocation.
    #[error("length field of {count} exceeds what the rest of the block can hold")]
    ImplausibleCount { count: u32 },

    /// Nesting beyond [`MAX_NESTING_DEPTH`].
    #[error("value nests deeper than the format carries")]
    TooDeeplyNested,

    /// Bytes follow a schema with no columns, where rows would be zero bytes long.
    #[error("block declares no columns yet carries row bytes")]
    RowsWithoutColumns,
}

/// A parsed header and schema, plus the undecoded rows.
#[derive(Debug)]
pub struct Block<'a> {
    names: Vec<&'a CStr>,
    kinds: Vec<ColumnKind>,
    rows: &'a [u8],
}

/// The present columns of one decoded row, in schema order.
#[derive(Debug)]
pub struct Row<'a> {
    fields: Vec<(&'a CStr, SharedValue)>,
}

impl<'a> Row<'a> {
    pub fn fields(&self) -> &[(&'a CStr, SharedValue)] {
        &self.fields
    }
}

impl<'a> Block<'a> {
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

        // Reject a column count the remaining bytes cannot back before sizing allocations from it.
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

    pub fn columns(&self) -> &[&'a CStr] {
        &self.names
    }

    pub fn kinds(&self) -> &[ColumnKind] {
        &self.kinds
    }

    pub const fn row_bytes(&self) -> &'a [u8] {
        self.rows
    }

    pub fn rows(&self) -> Rows<'a, '_> {
        Rows {
            block: self,
            reader: RowReader::new(self.rows, &self.kinds),
        }
    }
}

/// Iterator over a [`Block`]'s rows. Fuses on the first error: row boundaries are implied by the values, so there is
/// nothing to resynchronise to.
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

/// Decodes rows one at a time into a caller-supplied sink. Fuses on the first error, like [`Rows`].
#[derive(Debug)]
pub struct RowReader<'a, 'k> {
    cursor: Cursor<'a>,
    kinds: &'k [ColumnKind],
    strings: Strings<'k>,
    failed: bool,
}

#[derive(Debug, Clone, Copy)]
enum Strings<'b> {
    Copied,
    /// Borrowed from this buffer where [`SharedBuffer::share`] can, copied otherwise.
    Shared(&'b SharedBuffer),
}

impl<'a, 'k> RowReader<'a, 'k> {
    /// A reader over the bytes following a schema whose columns have `kinds`.
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

    /// Like [`RowReader::new`], but decoded strings borrow from `buffer` instead of copying.
    ///
    /// # Panics
    ///
    /// Panics if `rows` is not part of `buffer`'s bytes, or as [`RowReader::new`] does.
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

    /// True once the bytes ran out on a row boundary or a row failed to decode.
    pub const fn is_exhausted(&self) -> bool {
        self.failed || self.cursor.bytes.is_empty()
    }

    pub const fn remaining(&self) -> &'a [u8] {
        self.cursor.bytes
    }

    /// Decodes the next row, calling `sink` with each present column's index and value. On error `sink` may already
    /// have seen the columns before the malformed one.
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

fn decode_value(
    cursor: &mut Cursor<'_>,
    strings: Strings<'_>,
    depth: u32,
) -> Result<SharedValue, DecodeError> {
    let tag = cursor.take_tag()?;
    decode_payload(cursor, strings, tag, depth)
}

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

/// The length of the value of `kind` at the start of `bytes`, for the writer's re-encoding of earlier rows.
pub(crate) fn value_len(bytes: &[u8], kind: ColumnKind) -> Result<usize, DecodeError> {
    let mut cursor = Cursor { bytes };
    match kind {
        ColumnKind::Tagged => skip_value(&mut cursor, 0)?,
        ColumnKind::Typed(tag) => skip_payload(&mut cursor, tag, 0)?,
    }
    Ok(bytes.len() - cursor.bytes.len())
}

fn skip_value(cursor: &mut Cursor<'_>, depth: u32) -> Result<(), DecodeError> {
    let tag = cursor.take_tag()?;
    skip_payload(cursor, tag, depth)
}

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

/// A bare [`Tag::Null`] is the smallest tagged value.
const MIN_BYTES_PER_VALUE: usize = 1;

#[derive(Debug)]
struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.bytes.len() {
            return Err(DecodeError::Truncated);
        }
        let (taken, rest) = self.bytes.split_at(n);
        self.bytes = rest;
        Ok(taken)
    }

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

    fn take_string(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.take_count(1)?;
        let stored = self.take(len + 1)?;
        match stored.split_last() {
            Some((0, string)) => Ok(string),
            _ => Err(DecodeError::UnterminatedString),
        }
    }

    fn take_tag(&mut self) -> Result<Tag, DecodeError> {
        let byte = self.take_u8()?;
        Tag::from_byte(byte).ok_or(DecodeError::UnknownTag { tag: byte })
    }

    /// Consumes a `u32` count, rejecting one the remaining bytes cannot back at `bytes_per_element` apiece, so a
    /// corrupt count never sizes an allocation.
    fn take_count(&mut self, bytes_per_element: usize) -> Result<usize, DecodeError> {
        let count = self.take_u32()?;
        if (count as usize).saturating_mul(bytes_per_element) > self.bytes.len() {
            return Err(DecodeError::ImplausibleCount { count });
        }
        Ok(count as usize)
    }
}
