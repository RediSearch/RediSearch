/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::io::{Cursor, Seek, Write};

use ffi::{t_docId, t_fieldMask};
use qint::{qint_decode, qint_encode};
use varint::VarintEncode;

use crate::{
    Decoder, Encoder, RSIndexResult, RSResultType,
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
#[derive(Default)]
pub struct FieldsOffsets;

impl Encoder for FieldsOffsets {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.result_type, RSResultType::Term));

        let field_mask = record
                .field_mask
                .try_into()
                .expect("Need to use the wide variant of the FieldsOffsets encoder to support field masks bigger than u32");

        let mut bytes_written = qint_encode(&mut writer, [delta, field_mask, record.offsets_sz])?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FieldsOffsets {
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, field_mask, offsets_sz] = decoded_values;

        let record = decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
        )?;
        Ok(record)
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
#[derive(Default)]
pub struct FieldsOffsetsWide;

impl Encoder for FieldsOffsetsWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.result_type, RSResultType::Term));

        let mut bytes_written = qint_encode(&mut writer, [delta, record.offsets_sz])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FieldsOffsetsWide {
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, offsets_sz] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(cursor)?;

        let record = decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            1,
            offsets_sz,
        )?;
        Ok(record)
    }
}
