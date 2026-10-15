/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The byte offset of every token of a document, kept so that its text fields can be
//! highlighted without tokenizing them again.
//!
//! Tokens are numbered by position from 1, continuing across the text fields of the
//! document in the order they were indexed. A [`ByteOffsets`] records which positions
//! each field's tokens occupy and, in position order, the byte offset of each token in
//! the value it came from.

mod legacy;

pub use legacy::TruncatedError;
pub use opaque::OpaqueFieldByteOffsets;

use varint::VectorWriter;

/// The byte offset of each token of one document, and the token positions of each of its
/// text fields.
#[derive(Debug, Default, PartialEq, Eq)]
#[cheadergen::config(rename = "RSByteOffsets")]
pub struct ByteOffsets {
    fields: Vec<FieldTokens>,
    /// The offsets as [`VectorWriter`] encodes them: the `n`-th varint is the difference
    /// between the offsets of the tokens at positions `n` and `n - 1`.
    encoded: Box<[u8]>,
}

/// One field's share of the encoded offsets: `offsets` of them, for the tokens at the
/// positions from `first` on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FieldTokens {
    field_id: u16,
    first: u32,
    offsets: u32,
}

impl ByteOffsets {
    /// Creates an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves room for at least `additional` more fields.
    pub fn reserve_fields(&mut self, additional: usize) {
        self.fields.reserve_exact(additional);
    }

    /// Records that the field `field_id` owns the next `offsets` of the offsets taken by
    /// [`take_offsets`](Self::take_offsets), those of its tokens at the positions from
    /// `first` on.
    ///
    /// A field whose offsets are not stored, such as a multi-value one, owns none, even
    /// if it has tokens. Fields must be added in position order.
    pub fn add_field(&mut self, field_id: u16, first: u32, offsets: u32) {
        debug_assert!(
            self.fields
                .last()
                .is_none_or(|prev| prev.first.saturating_add(prev.offsets) <= first),
            "field {field_id} at position {first} overlaps the field before it"
        );
        self.fields.push(FieldTokens {
            field_id,
            first,
            offsets,
        });
    }

    /// Replaces the offsets with the ones written to `writer`, leaving `writer` with no
    /// bytes. The `n`-th value written must be the offset of the token at position `n`.
    pub fn take_offsets(&mut self, writer: &mut VectorWriter) {
        self.encoded = std::mem::take(writer.bytes_mut()).into_boxed_slice();
    }

    /// Iterates over the offsets of the first field recorded as `field_id`, or returns
    /// `None` if no field was.
    pub fn field_offsets(&self, field_id: u16) -> Option<FieldByteOffsets<'_>> {
        let index = self
            .fields
            .iter()
            .position(|field| field.field_id == field_id)?;
        // The offsets of the fields added before this one come first.
        // Saturating: past the end, decoding stops anyway.
        let skip = self.fields[..index]
            .iter()
            .fold(0u32, |skip, field| skip.saturating_add(field.offsets));
        Some(FieldByteOffsets::new(
            &self.encoded,
            self.fields[index],
            skip,
        ))
    }
}

/// Yields the byte offsets of the tokens of one field of a [`ByteOffsets`], in position
/// order.
///
/// Each call to [`next`](Iterator::next) moves the
/// [`token_position`](Self::token_position) one token forward and yields that token's
/// offset, until the position passes the field's last one. A call that finds the encoded
/// offsets cut short still moves the position, and yields nothing; once they have run
/// out, `next` yields nothing and leaves the position as it is. Before the first call,
/// the position is the one preceding the field's first token.
#[derive(Debug, Clone)]
pub struct FieldByteOffsets<'a> {
    /// The encoded offsets that follow [`offset`](Self::offset)'s.
    rest: &'a [u8],
    /// The last decoded offset.
    offset: u32,
    position: u32,
    /// How many of the field's offsets are still to be yielded.
    remaining: u32,
}

impl<'a> FieldByteOffsets<'a> {
    /// Starts at `field`'s offsets, which follow the `skip` offsets of the fields before
    /// it.
    fn new(encoded: &'a [u8], field: FieldTokens, skip: u32) -> Self {
        let mut offsets = Self {
            rest: encoded,
            offset: 0,
            // Wrapping, as in C: a legacy field at position 0 yields it first.
            position: field.first.wrapping_sub(1),
            remaining: field.offsets,
        };
        for _ in 0..skip {
            if !offsets.decode_next() {
                break;
            }
        }
        offsets
    }

    /// The position of the token whose offset was yielded last; see [`FieldByteOffsets`].
    pub const fn token_position(&self) -> u32 {
        self.position
    }

    /// Decodes the next offset into [`offset`](Self::offset). Returns `false`, and ends
    /// the iteration, if the encoded offsets run out instead.
    fn decode_next(&mut self) -> bool {
        match varint::read::<u32, _>(&mut self.rest) {
            Ok(delta) => {
                self.offset = self.offset.wrapping_add(delta);
                true
            }
            Err(_) => {
                self.rest = &[];
                false
            }
        }
    }
}

impl Iterator for FieldByteOffsets<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.rest.is_empty() {
            return None;
        }
        self.position = self.position.wrapping_add(1);
        self.remaining = self.remaining.checked_sub(1)?;
        self.decode_next().then_some(self.offset)
    }
}

mod opaque {
    use super::FieldByteOffsets;
    use c_ffi_utils::opaque::Size;

    /// A [`FieldByteOffsets`] that C can hold by value, e.g. on its stack.
    #[cheadergen::config(export, rename = "RSByteOffsetIterator")]
    #[repr(C, align(8))]
    pub struct OpaqueFieldByteOffsets(Size<32>);

    c_ffi_utils::opaque!(FieldByteOffsets<'_>, OpaqueFieldByteOffsets);
}
