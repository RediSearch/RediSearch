/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, SeekFrom, Write};

use ffi::t_docId;
use qint::{qint_decode, qint_encode};

use crate::{
    Decoder, Encoder, RSIndexResult, RSResultData, TermDecoder,
    full::{decode_term_record_offsets, offsets},
};

/// Encode and decode the delta, frequency, and offsets of a term record.
///
/// The delta, frequency, and offsets length are encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
pub struct FreqsOffsets;

impl Encoder for FreqsOffsets {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq, offsets_sz])?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FreqsOffsets {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, freq, offsets_sz] = decoded_values;

        decode_term_record_offsets(cursor, base, delta, 0, freq, offsets_sz, result)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::term()
    }

    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        mut base: t_docId,
        target: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let (freq, offsets_sz) = loop {
            let [delta, freq, offsets_sz] = match qint_decode::<3, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };

            base += delta as t_docId;

            if base >= target {
                break (freq, offsets_sz);
            }

            // Skip offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(cursor, base, 0, 0, freq, offsets_sz, result)?;
        Ok(true)
    }
}

impl TermDecoder for FreqsOffsets {}
