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
//! The primary consumer is the encoder's own fallback path, which replays a block it just
//! wrote as ordinary RESP rows. Even there the input is treated as untrusted: a block is
//! bytes, and there is no cheap way to prove the bytes came from this build.

use crate::{MAGIC, MAX_NESTING_DEPTH, Tag, VERSION, bitmap_bytes, bitmap_get};
use std::ffi::CStr;
use value::SharedValue;

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

        // Every column costs a length field and a terminator at the very least, so a count
        // the rest of the block cannot cover is corrupt — reject it before sizing `names`.
        const MIN_BYTES_PER_COLUMN: usize = size_of::<u16>() + 1;
        if usize::from(ncols) * MIN_BYTES_PER_COLUMN > cursor.bytes.len() {
            return Err(DecodeError::Truncated);
        }

        let mut names = Vec::with_capacity(usize::from(ncols));
        for _ in 0..ncols {
            let name_len = usize::from(cursor.take_u16()?);
            let stored = cursor.take(name_len + 1)?;
            let name = CStr::from_bytes_with_nul(stored).map_err(|_| DecodeError::MalformedName)?;
            names.push(name);
        }

        if ncols == 0 && !cursor.bytes.is_empty() {
            return Err(DecodeError::RowsWithoutColumns);
        }

        Ok(Self {
            names,
            rows: cursor.bytes,
        })
    }

    /// The block's column names, in schema order.
    pub fn columns(&self) -> &[&'a CStr] {
        &self.names
    }

    /// Decodes the block's rows, stopping at the first malformed one.
    pub const fn rows(&self) -> Rows<'a, '_> {
        Rows {
            block: self,
            cursor: Cursor { bytes: self.rows },
            failed: false,
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
    cursor: Cursor<'a>,
    failed: bool,
}

impl<'a> Iterator for Rows<'a, '_> {
    type Item = Result<Row<'a>, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.cursor.bytes.is_empty() {
            return None;
        }
        let row = self.decode_row();
        if row.is_err() {
            self.failed = true;
        }
        Some(row)
    }
}

impl std::iter::FusedIterator for Rows<'_, '_> {}

impl<'a> Rows<'a, '_> {
    /// Decodes the row at the cursor.
    fn decode_row(&mut self) -> Result<Row<'a>, DecodeError> {
        let ncols = u16::try_from(self.block.names.len()).expect("a block's column count is a u16");
        let bitmap = self.cursor.take(bitmap_bytes(ncols))?;

        let mut fields = Vec::new();
        for (col, name) in self.block.names.iter().enumerate() {
            let col = u16::try_from(col).expect("a block's column count is a u16");
            if bitmap_get(bitmap, col) {
                fields.push((*name, decode_value(&mut self.cursor, 0)?));
            }
        }
        Ok(Row { fields })
    }
}

/// Decodes one tagged value nested `depth` levels below a row field.
fn decode_value(cursor: &mut Cursor<'_>, depth: u32) -> Result<SharedValue, DecodeError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(DecodeError::TooDeeplyNested);
    }

    let byte = cursor.take_u8()?;
    let tag = Tag::from_byte(byte).ok_or(DecodeError::UnknownTag { tag: byte })?;

    Ok(match tag {
        Tag::Number => SharedValue::new_num(cursor.take_f64()?),
        Tag::String => {
            let len = cursor.take_count(1)?;
            SharedValue::new_string(cursor.take(len)?.to_vec())
        }
        Tag::Null => SharedValue::null_static(),
        Tag::Array => {
            let count = cursor.take_count(MIN_BYTES_PER_VALUE)?;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                items.push(decode_value(cursor, depth + 1)?);
            }
            SharedValue::new_array(items)
        }
        Tag::Map => {
            let count = cursor.take_count(2 * MIN_BYTES_PER_VALUE)?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = decode_value(cursor, depth + 1)?;
                let value = decode_value(cursor, depth + 1)?;
                entries.push((key, value));
            }
            SharedValue::new_map(entries)
        }
    })
}

/// The fewest bytes an encoded value can occupy: a bare [`Tag::Null`] is its tag alone.
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
