/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, SeekFrom, Write};

use qint::{qint_decode, qint_encode};
use rqe_core::DocId;

use crate::{
    Decoder, Encoder, TermDecoder,
    full::{decode_term_record_offsets, offsets},
};
use index_result::RSIndexResult;

/// Encode and decode the offsets of a term record.
///
/// The delta and offsets lengths are encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.

#[derive(Debug)]
pub struct OffsetsOnly;

impl Encoder for OffsetsOnly {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(record.is_term());

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written = qint_encode(&mut writer, [delta, offsets_sz])?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for OffsetsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, offsets_sz] = decoded_values;

        decode_term_record_offsets(cursor, base, delta, 0, 1, offsets_sz, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::build_term().build()
    }

    #[inline(always)]
    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        mut base: DocId,
        target: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<Option<u16>> {
        let mut advanced: u16 = 0;
        let offsets_sz = loop {
            let [delta, offsets_sz] = match qint_decode::<2, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            advanced += 1;

            base += delta as DocId;

            if base >= target {
                break offsets_sz;
            }

            // Skip the offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(cursor, base, 0, 0, 1, offsets_sz, result)?;
        Ok(Some(advanced))
    }
}

impl TermDecoder for OffsetsOnly {}
