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

use crate::{DecodedBy, Decoder, Encoder, IndexBlock, RSIndexResult};

/// Encode and decode only the raw document ID delta without any compression.
///
/// The delta is encoded as a raw 4-byte value.
/// This is different from the regular [`crate::doc_ids_only::DocIdsOnly`] encoder which uses varint encoding.
#[derive(Default)]
pub struct RawDocIdsOnly;

impl Encoder for RawDocIdsOnly {
    type Delta = u32;
    const RECOMMENDED_BLOCK_ENTRIES: usize = 1000;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_ne_bytes())?;
        // Wrote delta as raw 4-bytes word
        Ok(4)
    }
}

impl DecodedBy for RawDocIdsOnly {
    type Decoder = Self;

    fn decoder() -> Self::Decoder {
        Self
    }
}

impl Decoder for RawDocIdsOnly {
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index>> {
        let mut delta_bytes = [0u8; 4];
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);

        let record = RSIndexResult::term().doc_id(base + delta as t_docId);
        Ok(record)
    }

    fn base_id(block: &IndexBlock, _last_doc_id: t_docId) -> t_docId {
        block.first_doc_id
    }
}
