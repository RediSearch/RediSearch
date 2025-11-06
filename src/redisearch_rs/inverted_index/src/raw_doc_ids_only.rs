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

use crate::{Decoder, Encoder, IndexBlock, RSIndexResult, TermDecoder};

/// Encode and decode only the raw document ID delta without any compression.
///
/// The delta is encoded as a raw 4-byte value.
/// This is different from the regular [`crate::doc_ids_only::DocIdsOnly`] encoder which uses varint encoding.
pub struct RawDocIdsOnly;

impl Encoder for RawDocIdsOnly {
    type Delta = u32;
    const RECOMMENDED_BLOCK_ENTRIES: u16 = 1000;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_ne_bytes())?;
        // Wrote delta as raw 4-bytes word
        Ok(4)
    }

    fn delta_base(block: &IndexBlock) -> t_docId {
        block.first_doc_id
    }
}

impl Decoder for RawDocIdsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let mut delta_bytes = [0u8; 4];
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);

        result.doc_id = base + delta as t_docId;
        Ok(())
    }

    fn base_id(block: &IndexBlock, _last_doc_id: t_docId) -> t_docId {
        block.first_doc_id
    }

    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        target: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        // Check if the very next record is the target before starting a binary search
        let mut delta_bytes = [0u8; 4];
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);
        let mut doc_id = base + delta as t_docId;

        if doc_id >= target {
            result.doc_id = doc_id;
            return Ok(true);
        }

        // Start binary search
        let start = cursor.position() / 4;
        let end = cursor.get_ref().len() as u64 / 4;
        let mut left = start;
        let mut right = end;

        while left < right {
            let mid = left + (right - left) / 2;
            cursor.set_position(mid * 4);
            std::io::Read::read_exact(cursor, &mut delta_bytes)?;
            let delta = u32::from_ne_bytes(delta_bytes);
            doc_id = base + delta as t_docId;

            if doc_id < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Make sure we don't go past the end of the encoded input
        if left >= end {
            return Ok(false);
        }

        // Read the final value
        cursor.set_position(left * 4);
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);
        doc_id = base + delta as t_docId;

        result.doc_id = doc_id;
        Ok(true)
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::term()
    }
}

impl TermDecoder for RawDocIdsOnly {}
