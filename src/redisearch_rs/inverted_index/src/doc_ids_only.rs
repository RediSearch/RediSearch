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
use varint::VarintEncode;

use crate::{Decoder, Encoder, RSIndexResult, TermDecoder};

/// Encode and decode only the delta document ID of a record, without any other data.
/// The delta is encoded using [varint encoding](varint).
pub struct DocIdsOnly;

impl Encoder for DocIdsOnly {
    type Delta = u32;
    const RECOMMENDED_BLOCK_ENTRIES: u16 = 1000;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let bytes_written = delta.write_as_varint(&mut writer)?;
        Ok(bytes_written)
    }
}

impl Decoder for DocIdsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let delta = u32::read_as_varint(cursor)?;

        result.doc_id = base + delta as t_docId;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::term()
    }
}

impl TermDecoder for DocIdsOnly {}
