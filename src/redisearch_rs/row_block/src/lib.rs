/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compact binary encoding for a chunk of aggregation rows on the internal
//! coordinator<->shard path.
//!
//! RESP is a fine envelope but a poor record format: it is self-describing per value, so a
//! chunk of N rows costs one reply object per value *and* repeats every field name N times.
//! Measured on a 6-shard cluster with 668K intermediate rows, that came to ~344 bytes and
//! ~90 coordinator allocations per row, of which ~120 bytes/row was field names alone.
//!
//! A block carries the field names once, then the rows, and rides inside a single RESP bulk
//! string - so the coordinator receives one reply object per chunk instead of ~15 per row,
//! and hiredis needs no modification.
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
//! [`MAGIC`] and [`VERSION`] open the header. The NUL after each schema name lets a reader
//! hand a pointer straight into the block to the `RLookup` key lookups, whose FFI contract
//! requires NUL-terminated names.
//!
//! A value is a [`Tag`] byte followed by that tag's payload. The kind byte, a [`ColumnKind`],
//! lets a column drop the tag: a [`ColumnKind::Typed`] column holds bare payloads of its one
//! tag, and only a [`ColumnKind::Tagged`] column — one whose values differ in type from row
//! to row — pays a tag byte per value. Elements of arrays and maps are always tagged, at any
//! depth: their types vary within a single value (a `TOLIST` of mixed values), and a typed
//! element layout would need a kind per nesting level for a byte per element saved on values
//! that are rare on the hot path.
//!
//! A column absent from a row has its presence bit clear and occupies no bytes. A column the
//! row holds a null for is present, so the coordinator sees a null rather than a missing
//! field, exactly as with RESP; a column holding nothing but nulls is typed [`Tag::Null`],
//! whose payload is empty.
//!
//! The writer learns a column's kind from the values it is handed, without a second pass
//! over the pipeline: the first value fixes it, and a value of another type later in the
//! chunk turns the column [`ColumnKind::Tagged`], re-encoding the rows already written. See
//! [`RowBlockWriter::write_row`].
//!
//! The row count is implicit: a reader consumes rows until the buffer is exhausted, so a
//! writer never has to backpatch a count it does not know up front. A chunk with no rows is
//! therefore a header and schema with nothing after it, and is a valid block.
//!
//! The same implicit count is why a schema with no columns cannot be encoded: such rows are
//! zero bytes long, and no reader could tell one from a thousand. Callers must reply in RESP
//! when [`RowBlockWriter::write_schema`] reports no columns.
//!
//! # Compatibility
//!
//! The byte layout is a contract between the shard that writes a block and the coordinator
//! that decodes it with [`RowBlockDecoder`]. [`VERSION`] is bumped whenever the tag set, the
//! kind set or the layout changes: a reader rejects any other version, and the two sides are
//! always the same build in practice (internal path, no negotiation).

pub mod decoder;
pub mod reader;
pub mod writer;

pub use decoder::RowBlockDecoder;
pub use reader::{Block, DecodeError, Row, RowReader, Rows};
pub use writer::{ColumnFilter, RefusedRow, RowBlockWriter, SchemaError, TrioMember};

/// Opens a block's header. Spells `RSBR` when read as little-endian bytes.
pub const MAGIC: u32 = 0x5253_4252;

/// Follows [`MAGIC`]. See the crate-level *Compatibility* notes for when it changes.
pub const VERSION: u8 = 3;

/// The deepest [`Tag::Array`] / [`Tag::Map`] nesting a block may carry.
///
/// Both sides need the same bound: the decoder to keep a hostile block from recursing the
/// stack away, and the encoder so that any block it produces is one the decoder accepts —
/// the replay path in `row_block_ffi` reads back what this crate just wrote.
///
/// Aggregation output nests a handful of levels at most, so refusing beyond this costs a
/// chunk the block encoding and nothing else.
pub const MAX_NESTING_DEPTH: u32 = 128;

/// Type tag preceding every encoded value, and the payload that follows it.
///
/// A [`Tag::Map`]'s count is its number of *entries*, not the flattened key-plus-value
/// count, so that a reader can size an entry-shaped builder from it directly.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Tag {
    /// Payload: an [`f64`].
    Number = 1,
    /// Payload: a [`u32`] byte length, then that many bytes. Not required to be UTF-8, and
    /// may contain NUL bytes.
    String = 2,
    /// No payload.
    Null = 3,
    /// Payload: a [`u32`] element count, then that many tagged values.
    Array = 4,
    /// Payload: a [`u32`] entry count, then that many tagged key / tagged value pairs.
    Map = 5,
}

impl Tag {
    /// The tag `byte` encodes, or `None` if no tag has that value.
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

/// How a column's values are encoded, declared once per column by the schema's kind byte.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    /// Every value is a [`Tag`] byte followed by its payload. Written as `0`, which no tag
    /// uses.
    Tagged,
    /// Every value is a bare payload of this tag, with no tag byte. Written as the tag's own
    /// byte.
    Typed(Tag),
}

impl ColumnKind {
    /// The kind `byte` encodes, or `None` if no kind has that value.
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Tagged),
            _ => match Tag::from_byte(byte) {
                Some(tag) => Some(Self::Typed(tag)),
                None => None,
            },
        }
    }

    /// The schema byte encoding this kind.
    pub const fn to_byte(self) -> u8 {
        match self {
            Self::Tagged => 0,
            Self::Typed(tag) => tag as u8,
        }
    }
}

/// The number of presence-bitmap bytes a row with `ncols` columns carries.
const fn bitmap_bytes(ncols: u16) -> usize {
    (ncols as usize).div_ceil(8)
}

/// Whether the bit for column `col` is set in a row's presence `bitmap`.
///
/// `bitmap` must be at least [`bitmap_bytes`] long for the block's column count, and `col`
/// must be below that count.
const fn bitmap_get(bitmap: &[u8], col: u16) -> bool {
    bitmap[col as usize / 8] & (1u8 << (col % 8)) != 0
}
