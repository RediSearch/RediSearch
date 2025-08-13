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

use crate::{Decoder, Encoder, RSIndexResult};

/// Encode and decode only the delta document ID of a record, without any other data.
/// The delta is encoded using [varint encoding](varint).
#[derive(Default)]
pub struct DocIdsOnly;

impl Encoder for DocIdsOnly {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let bytes_written = delta.write_as_varint(&mut writer)?;
        Ok(bytes_written)
    }
}

impl Decoder for DocIdsOnly {
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a, 'static>> {
        let delta = u32::read_as_varint(cursor)?;

        let record = RSIndexResult::term().doc_id(base + delta as t_docId);
        Ok(record)
    }
}
