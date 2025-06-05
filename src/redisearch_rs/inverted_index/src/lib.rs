/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Read, Seek, Write};

pub use ffi::{RSIndexResult, t_docId};

/// A delta is the difference between document IDs. It is mostly used to save space in the index
/// because document IDs are usually sequential and the difference between them are small. With the
/// help of encoding, we can optionally store the difference (delta) efficiently instead of the full document
/// ID.
pub struct Delta(usize);

impl Delta {
    /// Make a new delta value
    pub fn new(delta: usize) -> Self {
        Delta(delta)
    }
}

impl From<Delta> for usize {
    fn from(delta: Delta) -> Self {
        delta.0
    }
}

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode<W: Write + Seek>(
        writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize>;
}

pub enum DecoderResult {
    /// The record was successfully decoded.
    Record(RSIndexResult),
    /// The record was filtered out and should not be returned.
    FilteredOut,
}

/// Decoder to read records from an index
pub trait Decoder {
    /// Decode the next record from the reader. If any delta values are decoded, then they should
    /// add to the `base` document ID to get the actual document ID.
    ///
    /// Returns `Ok(None)` if there is nothing left on the reader to decode.
    fn decode<R: Read>(
        &self,
        reader: &mut R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>>;

    /// Like `[Decoder::decode]`, but it skips all entries whose document ID is lower than `target`.
    ///
    /// Returns `None` if no record has a document ID greater than or equal to `target`.
    fn seek<R: Read + Seek>(
        &self,
        reader: &mut R,
        base: t_docId,
        target: t_docId,
    ) -> std::io::Result<Option<RSIndexResult>> {
        loop {
            match self.decode(reader, base)? {
                Some(DecoderResult::Record(record)) if record.docId >= target => {
                    return Ok(Some(record));
                }
                Some(DecoderResult::Record(_)) | Some(DecoderResult::FilteredOut) => continue,
                None => return Ok(None),
            }
        }
    }
}
