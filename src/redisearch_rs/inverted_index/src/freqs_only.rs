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

use crate::{Decoder, Encoder, RSIndexResult};

/// Encode and decode only the delta and frequencies of a record, without any other data.
/// The delta and frequency are encoded using [qint encoding](qint).
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
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, freq] = decoded_values;

        result.doc_id = base + delta as t_docId;
        result.freq = freq;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::virt()
    }
}
