/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, Write};

use rqe_core::DocId;

use crate::{Decoder, DocIdsDecoder, Encoder, IndexBlock, TermDecoder};
use index_result::RSIndexResult;

/// Encode and decode only the raw document ID delta without any compression.
///
/// The delta is encoded as a raw 4-byte value.
/// This is different from the regular [`crate::doc_ids_only::DocIdsOnly`] encoder which uses varint encoding.

#[derive(Debug)]
pub struct RawDocIdsOnly;

const RAW_DOC_ID_DELTA_BYTES: usize = std::mem::size_of::<u32>();

impl Encoder for RawDocIdsOnly {
    type Delta = u32;
    const RECOMMENDED_BLOCK_ENTRIES: u16 = 1000;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        writer.write_all(&delta.to_ne_bytes())?;
        Ok(RAW_DOC_ID_DELTA_BYTES)
    }

    fn delta_base(block: &IndexBlock) -> DocId {
        block.first_doc_id
    }
}

impl Decoder for RawDocIdsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let mut delta_bytes = [0u8; RAW_DOC_ID_DELTA_BYTES];
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);

        result.doc_id = base + delta as DocId;
        Ok(())
    }

    fn base_id(block: &IndexBlock, _last_doc_id: DocId) -> DocId {
        block.first_doc_id
    }

    #[inline(always)]
    fn seek<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: DocId,
        target: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<Option<u16>> {
        let entry_width = RAW_DOC_ID_DELTA_BYTES as u64;
        let start_ordinal = cursor.position() / entry_width;

        // Check if the very next record is the target before starting a binary search
        let mut delta_bytes = [0u8; RAW_DOC_ID_DELTA_BYTES];
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);
        let mut doc_id = base + delta as DocId;

        if doc_id >= target {
            result.doc_id = doc_id;
            return Ok(Some(0));
        }

        // Start binary search
        let start = cursor.position() / entry_width;
        let end = cursor.get_ref().len() as u64 / entry_width;
        let mut left = start;
        let mut right = end;

        while left < right {
            let mid = left + (right - left) / 2;
            cursor.set_position(mid * entry_width);
            std::io::Read::read_exact(cursor, &mut delta_bytes)?;
            let delta = u32::from_ne_bytes(delta_bytes);
            doc_id = base + delta as DocId;

            if doc_id < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // Make sure we don't go past the end of the encoded input
        if left >= end {
            return Ok(None);
        }

        // Read the final value
        cursor.set_position(left * entry_width);
        std::io::Read::read_exact(cursor, &mut delta_bytes)?;
        let delta = u32::from_ne_bytes(delta_bytes);
        doc_id = base + delta as DocId;

        result.doc_id = doc_id;
        Ok(Some((left - start_ordinal) as u16))
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::build_term().build()
    }
}

impl TermDecoder for RawDocIdsOnly {}
impl DocIdsDecoder for RawDocIdsOnly {}
