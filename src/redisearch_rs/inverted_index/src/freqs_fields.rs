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

use crate::{Decoder, Encoder, RSIndexResult};

/// Encode and decode the delta, frequency and field mask of a record.
///
/// This encoder supports field masks fitting in a `u32`.
/// Use [`FreqsFieldsWide`] for `u128` field masks.
///
/// The delta, frequency, field mask are encoded using [qint encoding](qint).
///
/// This encoder only supports delta values that fit in a `u32`.
#[derive(Default)]
pub struct FreqsFields;

impl Encoder for FreqsFields {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let field_mask = record
                .field_mask
                .try_into()
                .expect("Need to use the wide variant of the FreqsFields encoder to support field masks bigger than u32");
        let bytes_written = qint_encode(&mut writer, [delta, record.freq, field_mask])?;

        Ok(bytes_written)
    }
}

impl Decoder for FreqsFields {
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, freq, field_mask] = decoded_values;

        let record = RSIndexResult::term()
            .doc_id(base + delta as t_docId)
            .field_mask(field_mask as t_fieldMask)
            .frequency(freq);
        Ok(record)
    }
}

/// Encode and decode the delta, frequency and field mask of a record.
///
/// This encoder supports larger field masks fitting in a `u128`.
/// Use [`FreqsFields`] for `u32` field masks.
///
/// The delta and frequency are encoded using [qint encoding](qint).
/// The field mask is then encoded using [varint encoding](varint).
///
/// This encoder only supports delta values that fit in a `u32`.
#[derive(Default)]
pub struct FreqsFieldsWide;

impl Encoder for FreqsFieldsWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;
        Ok(bytes_written)
    }
}

impl Decoder for FreqsFieldsWide {
    fn decode<'a>(
        &self,
        cursor: &mut Cursor<&'a [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'a>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<2, _>(cursor)?;
        let [delta, freq] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(cursor)?;

        let record = RSIndexResult::term()
            .doc_id(base + delta as t_docId)
            .field_mask(field_mask)
            .frequency(freq);
        Ok(record)
    }
}
