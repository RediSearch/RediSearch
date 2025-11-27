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

use crate::{Decoder, Encoder, RSIndexResult, TermDecoder};

/// Encode and decode the delta and field mask of a record.
///
/// This encoder supports field masks fitting in a `u32`.
/// Use [`FieldsOnlyWide`] for `u128` field masks.
///
/// The delta and field mask are encoded using [qint encoding](qint).
///
/// This encoder only supports delta values that fit in a `u32`.
pub struct FieldsOnly;

impl Encoder for FieldsOnly {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let field_mask = record
                .field_mask
                .try_into()
                .expect("Need to use the wide variant of the FieldsOnly encoder to support field masks bigger than u32");
        let bytes_written = qint_encode(&mut writer, [delta, field_mask])?;
        Ok(bytes_written)
    }
}

impl Decoder for FieldsOnly {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, field_mask] = decoded_values;

        result.doc_id = base + delta as t_docId;
        result.field_mask = field_mask as t_fieldMask;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::term()
    }
}

/// Encode and decode the delta and field mask of a record.
///
/// This encoder supports larger field masks fitting in a `u128`.
/// Use [`FieldsOnly`] for `u32` field masks.
///
/// The delta and the field mask are encoded using [varint encoding](varint).
///
/// This encoder only supports delta values that fit in a `u32`.
pub struct FieldsOnlyWide;

impl Encoder for FieldsOnlyWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let mut bytes_written = delta.write_as_varint(&mut writer)?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;
        Ok(bytes_written)
    }
}

impl Decoder for FieldsOnlyWide {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let delta = u32::read_as_varint(cursor)?;
        let field_mask = u128::read_as_varint(cursor)?;

        result.doc_id = base + delta as t_docId;
        result.field_mask = field_mask;
        Ok(())
    }

    fn base_result<'index>() -> RSIndexResult<'index> {
        RSIndexResult::term()
    }
}

impl TermDecoder for FieldsOnly {}
impl TermDecoder for FieldsOnlyWide {}
