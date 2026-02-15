/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod doc_ids_only;
pub mod fields_offsets;
pub mod fields_only;
pub mod freqs_fields;
pub mod freqs_offsets;
pub mod freqs_only;
pub mod full;
pub mod numeric;
pub mod offsets_only;
pub mod raw_doc_ids_only;

use std::io::{Cursor, Seek, Write};

use ffi::t_docId;

use crate::{IndexBlock, RSIndexResult};

/// Trait used to correctly derive the delta needed for different encoders
pub trait IdDelta
where
    Self: Sized,
{
    /// Try to convert a `u64` into this delta type. If the `u64` will be too big for this delta
    /// type, then `None` should be returned. Returning `None` will cause the [`crate::InvertedIndex`]
    /// to make a new block so that it can have a zero delta.
    fn from_u64(delta: u64) -> Option<Self>;

    /// The delta value when the [`crate::InvertedIndex`] makes a new block and needs a delta equal to `0`.
    fn zero() -> Self;
}

impl IdDelta for u32 {
    fn from_u64(delta: u64) -> Option<Self> {
        delta.try_into().ok()
    }

    fn zero() -> Self {
        0
    }
}

/// Encoder to write a record into an index
pub trait Encoder {
    /// Document ids are represented as `u64`s and stored using delta-encoding.
    ///
    /// Some encoders can't encode arbitrarily large id deltas—e.g. they might be limited to `u32::MAX` or
    /// another subset of the `u64` value range.
    ///
    /// This associated type can be used by each encoder to specify which range they support, thus
    /// allowing the inverted index to work around their limitations accordingly (i.e. by creating new blocks).
    type Delta: IdDelta;

    /// Does this encoder allow the same document to appear in the index multiple times. We need to
    /// allow duplicates to support multi-value JSON fields.
    ///
    /// Defaults to `false`.
    const ALLOW_DUPLICATES: bool = false;

    /// The suggested number of entries that can be written in a single block. Defaults to 100.
    const RECOMMENDED_BLOCK_ENTRIES: u16 = 100;

    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
        writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;

    /// Returns the base value that should be used for any delta calculations
    fn delta_base(block: &IndexBlock) -> t_docId {
        block.last_doc_id
    }
}

/// Trait to model that an encoder can be decoded by a decoder.
pub trait DecodedBy: Encoder {
    type Decoder: Decoder;
}

impl<E: Encoder + Decoder> DecodedBy for E {
    type Decoder = E;
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()>;

    /// Creates a new instance of [`RSIndexResult`] which this decoder can decode into.
    fn base_result<'index>() -> RSIndexResult<'index>;

    /// Like `[Decoder::decode]`, but it returns a new instance of the result instead of filling
    /// an existing one.
    fn decode_new<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index>> {
        let mut result = Self::base_result();
        Self::decode(cursor, base, &mut result)?;

        Ok(result)
    }

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Decoders can override the default implementation to provide a more efficient one by reading the
    /// document ID first and skipping ahead if the ID does not match the target, saving decoding
    /// the rest of the record.
    ///
    /// Returns `false` if end of the cursor was reached before finding a document equal, or bigger,
    /// than the target.
    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        mut base: t_docId,
        target: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        loop {
            match Self::decode(cursor, base, result) {
                Ok(_) if result.doc_id >= target => {
                    return Ok(true);
                }
                Ok(_) => {
                    base = result.doc_id;
                    continue;
                }
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns the base value to use for any delta calculations
    fn base_id(_block: &IndexBlock, last_doc_id: t_docId) -> t_docId {
        last_doc_id
    }
}

/// Marker trait for decoders producing numeric results.
pub trait NumericDecoder: Decoder {}
/// Marker trait for decoders producing term results.
pub trait TermDecoder: Decoder {}

/// The capacity of the block vector used by [`crate::InvertedIndex`].
pub type BlockCapacity = u32;
