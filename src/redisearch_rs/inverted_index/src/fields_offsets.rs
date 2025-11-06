/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, SeekFrom, Write};

use ffi::{t_docId, t_fieldMask};
use qint::{qint_decode, qint_encode};
use varint::VarintEncode;

use crate::{
    Decoder, Encoder, RSIndexResult, RSResultData, TermDecoder,
    full::{decode_term_record_offsets, offsets},
};

/// Encode and decode the delta, field mask and offsets of a term record.
///
/// This encoder supports field masks fitting in a `u32`.
/// Use [`FieldsOffsetsWide`] for `u128` field masks.
///
/// The delta, field mask and offsets lengths are encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
pub struct FieldsOffsets;

impl Encoder for FieldsOffsets {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let field_mask = record
                .field_mask
                .try_into()
                .expect("Need to use the wide variant of the FieldsOffsets encoder to support field masks bigger than u32");

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written = qint_encode(&mut writer, [delta, field_mask, offsets_sz])?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FieldsOffsets {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, field_mask, offsets_sz] = decoded_values;

        decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
            result,
        )
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
        let (field_mask, offsets_sz) = loop {
            let [delta, field_mask, offsets_sz] = match qint_decode::<3, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };

            base += delta as t_docId;

            if base >= target {
                break (field_mask, offsets_sz);
            }

            // Skip the offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(
            cursor,
            base,
            0,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
            result,
        )?;
        Ok(true)
    }
}

/// Encode and decode the delta, field mask and offsets of a term record.
///
/// This encoder supports larger field masks fitting in a `u128`.
/// Use [`FieldsOffsets`] for `u32` field masks.
///
/// The delta and offsets lengths are encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
pub struct FieldsOffsetsWide;

impl Encoder for FieldsOffsetsWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written = qint_encode(&mut writer, [delta, offsets_sz])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FieldsOffsetsWide {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, offsets_sz] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(cursor)?;

        decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
            result,
        )
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
        let (field_mask, offsets_sz) = loop {
            let [delta, offsets_sz] = match qint_decode::<2, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            let field_mask = t_fieldMask::read_as_varint(cursor)?;

            base += delta as t_docId;

            if base >= target {
                break (field_mask, offsets_sz);
            }

            // Skip the offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(
            cursor,
            base,
            0,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
            result,
        )?;
        Ok(true)
    }
}

impl TermDecoder for FieldsOffsets {}
impl TermDecoder for FieldsOffsetsWide {}
