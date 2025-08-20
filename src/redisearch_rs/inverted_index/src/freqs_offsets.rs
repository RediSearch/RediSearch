/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, Write};

use ffi::t_docId;
use qint::{qint_decode, qint_encode};

use crate::{
    Decoder, Encoder, RSIndexResult, RSResultData,
    full::{decode_term_record_offsets, offsets},
};

/// Encode and decode the delta, frequency, and offsets of a term record.
///
/// The delta, frequency, and offsets length are encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
#[derive(Default)]
pub struct FreqsOffsets;

impl Encoder for FreqsOffsets {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq, record.offsets_sz])?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }

    fn decoder() -> impl Decoder {
        Self
    }
}

impl Decoder for FreqsOffsets {
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, freq, offsets_sz] = decoded_values;

        let record = decode_term_record_offsets(cursor, base, delta, 0, freq, offsets_sz)?;
        Ok(record)
    }
}
