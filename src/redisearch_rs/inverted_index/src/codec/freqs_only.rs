/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, Write};

use qint::{qint_decode, qint_encode};
use rqe_core::DocId;

use crate::{Decoder, Encoder, TermDecoder};
use index_result::RSIndexResult;

/// Encode and decode only the delta and frequencies of a record, without any other data.
/// The delta and frequency are encoded using [qint encoding](qint).

#[derive(Debug)]
pub struct FreqsOnly;

impl Encoder for FreqsOnly {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let bytes_written = qint_encode(&mut writer, [delta, record.freq])?;
        Ok(bytes_written)
    }
}

impl Decoder for FreqsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, freq] = decoded_values;

        result.doc_id = base + delta as DocId;
        result.freq = freq;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::build_virt().build()
    }

    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        mut base: DocId,
        target: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        let freq = loop {
            let [delta, freq] = match qint_decode::<2, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };

            base += delta as DocId;

            if base >= target {
                break freq;
            }
        };

        result.doc_id = base;
        result.freq = freq;
        Ok(true)
    }
}

impl TermDecoder for FreqsOnly {}
