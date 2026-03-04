/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-offset map for tokenized text fields.
//!
//! This crate is a Rust port of `src/byte_offsets.c`.
//!
//! During indexing, each token in a document is assigned a byte offset (the
//! position of the token's first byte within the original document text) and a
//! *token position* (its ordinal within the document, starting at 1).  A
//! [`ByteOffsets`] struct stores these offsets for multiple fields in a single
//! compact, serializable structure.
//!
//! # Encoding
//!
//! Byte offsets are stored as a flat stream of **delta-encoded varints** shared
//! across all fields.  Position *n* in the stream holds the delta from position
//! *n-1* to position *n* (or from 0 for position 1).  Each [`OffsetField`]
//! records which slice of the stream belongs to it via a
//! [`first_tok_pos`][OffsetField::first_tok_pos] /
//! [`last_tok_pos`][OffsetField::last_tok_pos] range.
//!
//! [`ByteOffsetWriter`] accumulates offsets during indexing and produces the
//! raw varint bytes.  [`ByteOffsetIter`] decodes them on the read path.

use std::io::{self, Cursor, Read, Write};

/// Token position range for a single field within the shared byte-offset stream.
pub struct OffsetField {
    /// Field identifier.
    pub field_id: u32,
    /// Token position (1-based) of the first token belonging to this field.
    pub first_tok_pos: u32,
    /// Token position (1-based) of the last token belonging to this field.
    pub last_tok_pos: u32,
}

/// Byte-offset map for the tokens of one document.
///
/// Stores, for each indexed field, the delta-encoded byte offsets of each
/// token within the original document text.  The actual offsets live in a
/// single flat varint stream ([`offsets`][ByteOffsets::offsets]) shared by all
/// fields.
pub struct ByteOffsets {
    /// Per-field metadata (field id, first/last token position).
    pub fields: Vec<OffsetField>,
    /// Delta-encoded byte offsets, stored as raw varint bytes.
    offsets: Vec<u8>,
}

impl ByteOffsets {
    /// Create an empty [`ByteOffsets`].
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Append a field to the offset map.
    pub fn add_field(&mut self, field_id: u32, first_tok_pos: u32, last_tok_pos: u32) {
        self.fields.push(OffsetField {
            field_id,
            first_tok_pos,
            last_tok_pos,
        });
    }

    /// Replace the raw varint offset bytes.
    ///
    /// Typically called with the result of [`ByteOffsetWriter::into_bytes`].
    pub fn set_offset_bytes(&mut self, bytes: Vec<u8>) {
        self.offsets = bytes;
    }

    /// Serialize the struct into `w`.
    ///
    /// # Wire format
    ///
    /// ```text
    /// u8          numFields
    /// for each field:
    ///   u8        field_id  (truncated to 8 bits)
    ///   u32 LE    first_tok_pos
    ///   u32 LE    last_tok_pos
    /// u32 LE      byte length of the varint offset stream
    /// [u8]        raw varint bytes
    /// ```
    pub fn serialize<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&[self.fields.len() as u8])?;
        for field in &self.fields {
            w.write_all(&[field.field_id as u8])?;
            w.write_all(&field.first_tok_pos.to_le_bytes())?;
            w.write_all(&field.last_tok_pos.to_le_bytes())?;
        }
        w.write_all(&(self.offsets.len() as u32).to_le_bytes())?;
        w.write_all(&self.offsets)?;
        Ok(())
    }

    /// Deserialize a [`ByteOffsets`] from `data`.
    pub fn load(data: &[u8]) -> io::Result<Self> {
        let mut r = Cursor::new(data);

        let mut buf1 = [0u8; 1];
        r.read_exact(&mut buf1)?;
        let num_fields = buf1[0] as usize;

        let mut fields = Vec::with_capacity(num_fields);
        let mut buf4 = [0u8; 4];
        for _ in 0..num_fields {
            r.read_exact(&mut buf1)?;
            let field_id = buf1[0] as u32;

            r.read_exact(&mut buf4)?;
            let first_tok_pos = u32::from_le_bytes(buf4);

            r.read_exact(&mut buf4)?;
            let last_tok_pos = u32::from_le_bytes(buf4);

            fields.push(OffsetField {
                field_id,
                first_tok_pos,
                last_tok_pos,
            });
        }

        r.read_exact(&mut buf4)?;
        let offsets_len = u32::from_le_bytes(buf4) as usize;

        let mut offsets = vec![0u8; offsets_len];
        if offsets_len > 0 {
            r.read_exact(&mut offsets)?;
        }

        Ok(Self { fields, offsets })
    }

    /// Return an iterator over the byte offsets for `field_id`.
    ///
    /// Returns `None` if the field is not present.
    ///
    /// # Pre-advance
    ///
    /// Because all fields share a single flat varint stream, the iterator must
    /// first *skip* past any tokens that belong to earlier fields.  It reads
    /// `first_tok_pos - 1` varints (accumulating their deltas into
    /// `last_value`) before yielding the first offset.  This mirrors the
    /// pre-advance loop in the C implementation.
    pub fn iterate(&self, field_id: u32) -> Option<ByteOffsetIter<'_>> {
        let field = self.fields.iter().find(|f| f.field_id == field_id)?;

        let mut cursor = Cursor::new(self.offsets.as_slice());
        let mut last_value: u32 = 0;
        let mut cur_pos: u32 = 1;

        // Skip past the tokens that precede this field's first token.
        while cur_pos < field.first_tok_pos {
            match varint::read::<u32, _>(&mut cursor) {
                Ok(delta) => {
                    last_value = last_value.wrapping_add(delta);
                    cur_pos += 1;
                }
                Err(_) => break,
            }
        }

        // Decrement so that next() can increment before reading.
        let cur_pos = cur_pos.saturating_sub(1);

        Some(ByteOffsetIter {
            cursor,
            last_value,
            cur_pos,
            end_pos: field.last_tok_pos,
        })
    }
}

impl Default for ByteOffsets {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator that yields the absolute byte offsets for a specific field's tokens.
pub struct ByteOffsetIter<'a> {
    cursor: Cursor<&'a [u8]>,
    /// Accumulated absolute byte offset (last value yielded).
    last_value: u32,
    /// Current token position within the document.
    cur_pos: u32,
    /// Token position of the last token belonging to this field.
    end_pos: u32,
}

impl ByteOffsetIter<'_> {
    /// Advance to the next token and return its byte offset.
    ///
    /// Returns `None` when all tokens for this field have been consumed.
    pub fn next(&mut self) -> Option<u32> {
        self.cur_pos += 1;
        if self.cur_pos > self.end_pos {
            return None;
        }
        let delta: u32 = varint::read(&mut self.cursor).ok()?;
        self.last_value = self.last_value.wrapping_add(delta);
        Some(self.last_value)
    }

    /// The current token position in the document (1-based).
    pub const fn cur_pos(&self) -> u32 {
        self.cur_pos
    }
}

/// Accumulates byte offsets during indexing, encoding them as delta varints.
///
/// Byte offsets must be written in non-decreasing order (i.e. in document
/// order).  Internally the offsets are stored as deltas, so writing
/// already-sorted offsets keeps each delta small and therefore the encoded
/// size compact.
///
/// When indexing is complete, call [`into_bytes`][ByteOffsetWriter::into_bytes]
/// and pass the result to [`ByteOffsets::set_offset_bytes`].
pub struct ByteOffsetWriter {
    vw: varint::VectorWriter,
}

impl ByteOffsetWriter {
    /// Create a new writer with a small initial capacity.
    pub fn new() -> Self {
        Self {
            vw: varint::VectorWriter::new(16),
        }
    }

    /// Append a byte offset to the stream.
    ///
    /// `offset` should be greater than or equal to the previously written
    /// offset.
    pub fn write(&mut self, offset: u32) -> io::Result<()> {
        self.vw.write(offset)?;
        Ok(())
    }

    /// Consume the writer and return the raw varint bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.vw.bytes().to_vec()
    }
}

impl Default for ByteOffsetWriter {
    fn default() -> Self {
        Self::new()
    }
}
