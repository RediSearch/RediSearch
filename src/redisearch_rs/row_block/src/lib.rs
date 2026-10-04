/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compact binary encoding for a chunk of aggregation rows on the internal shard-to-coordinator path.
//!
//! RESP repeats every field name in every row and costs the coordinator one reply object per value. A block carries the
//! names once, then the rows, inside a single RESP bulk string.
//!
//! # Layout
//!
//! All integers are little-endian.
//!
//! ```text
//! header   magic u32 | version u8 | ncols u16
//! schema   ncols x { name_len u16, name bytes, NUL, kind u8 }
//! rows     nrows x {
//!            presence bitmap  ceil(ncols/8) bytes
//!            per present column, in schema order: one value, encoded as its column's kind says
//!          }
//! ```
//!
//! Names and string payloads are NUL-terminated so the coordinator can use them in place.
//!
//! A value is a [`Tag`] byte and its payload. A [`ColumnKind::Typed`] column drops the tag; only a
//! [`ColumnKind::Tagged`] column, whose values differ in type, pays it per value. Array and map elements are always
//! tagged, since their types vary within one value. An absent field has its presence bit clear; a null is a present
//! value, as in RESP.
//!
//! The row count is implicit: rows run to the end of the buffer. A block with no rows is therefore just its header and
//! schema, and a schema with no columns cannot carry rows at all, so callers reply in RESP when
//! [`RowBlockWriter::write_schema`] reports no columns.
//!
//! # Compatibility
//!
//! Writer and reader are always the same build (internal path, no negotiation). [`VERSION`] changes with any change to
//! the layout, the tags or the kinds, and a reader rejects any other version.

pub mod decoder;
pub mod reader;
pub mod writer;

pub use decoder::RowBlockDecoder;
pub use reader::{Block, DecodeError, Row, RowReader, Rows};
pub use writer::{ColumnFilter, RefusedRow, RowBlockWriter, SchemaError, TrioMember};

/// Spells `RSBR` as little-endian bytes.
pub const MAGIC: u32 = 0x5253_4252;

pub const VERSION: u8 = 4;

/// The deepest [`Tag::Array`] / [`Tag::Map`] nesting a block may carry. The reader enforces it against hostile input,
/// and the writer refuses deeper values so that every block it writes is one the reader accepts.
pub const MAX_NESTING_DEPTH: u32 = 128;

/// A value's type, and the payload that follows it.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Tag {
    /// Payload: an [`f64`].
    Number = 1,
    /// Payload: a [`u32`] byte length, that many bytes (any, NULs included), then a NUL.
    String = 2,
    /// No payload.
    Null = 3,
    /// Payload: a [`u32`] element count, then that many tagged values.
    Array = 4,
    /// Payload: a [`u32`] entry count (not key-plus-value count), then that many tagged key / tagged value pairs.
    Map = 5,
}

impl Tag {
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Number),
            2 => Some(Self::String),
            3 => Some(Self::Null),
            4 => Some(Self::Array),
            5 => Some(Self::Map),
            _ => None,
        }
    }
}

/// How a column's values are encoded, declared by its schema kind byte.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    /// Every value carries its [`Tag`]. Written as `0`, which no tag uses.
    Tagged,
    /// Every value is a bare payload of this tag. Written as the tag's byte.
    Typed(Tag),
}

impl ColumnKind {
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Tagged),
            _ => match Tag::from_byte(byte) {
                Some(tag) => Some(Self::Typed(tag)),
                None => None,
            },
        }
    }

    pub const fn to_byte(self) -> u8 {
        match self {
            Self::Tagged => 0,
            Self::Typed(tag) => tag as u8,
        }
    }
}

const fn bitmap_bytes(ncols: u16) -> usize {
    (ncols as usize).div_ceil(8)
}

const fn bitmap_get(bitmap: &[u8], col: u16) -> bool {
    bitmap[col as usize / 8] & (1u8 << (col % 8)) != 0
}
