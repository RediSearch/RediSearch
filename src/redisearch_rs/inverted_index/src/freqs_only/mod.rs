/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Read, Seek, Write};

use ffi::t_docId;
use qint::{qint_decode, qint_encode};

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult};

/// Encode and decode only the delta ID and frequencies of a record, without any other data.
/// The delta and frequency are encoded using [qint encoding](qint).
pub struct FreqsOnly;

impl Encoder for FreqsOnly {
    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        // Truncating delta to u32 as qint_encode expects u32 values.
        // The inverted index will create new block if the delta exceeds u32::MAX.
        let bytes_written = qint_encode(&mut writer, [delta.0 as u32, record.freq])?;
        Ok(bytes_written)
    }
}

impl Decoder for FreqsOnly {
    fn decode<R: Read>(
        &self,
        reader: &mut R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(reader)?;
        let [delta, freq] = decoded_values;

        let record = RSIndexResult::freqs_only(base + delta as u64, freq);
        Ok(Some(DecoderResult::Record(record)))
    }
}
