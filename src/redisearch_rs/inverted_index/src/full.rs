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

use crate::{Decoder, Encoder, RSIndexResult, RSOffsetVectorRef, RSResultData};

/// Encode and decode the delta, frequency, field mask and offsets of a term record.
///
/// This encoder supports field masks fitting in a `u32`.
/// Use [`FullWide`] for `u128` field masks.
///
/// The delta, frequency, field mask and offsets lengths are encoded using [qint encoding](qint).
///
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
#[derive(Default)]
pub struct Full;

/// Return a slice of the offsets vector from a term record.
///
/// # Safety
///
/// record must have `result_type` set to `RSResultType::Term`.
#[inline(always)]
pub fn offsets<'a>(record: &'a RSIndexResult<'_, '_>) -> &'a [u8] {
    // SAFETY: caller ensured the proper result_type.
    let offsets = record.as_term().unwrap().offsets();
    assert_eq!(offsets.len(), record.offsets_sz as usize);

    offsets
}

impl Encoder for Full {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let field_mask = record
                .field_mask
                .try_into()
                .expect("Need to use the wide variant of the Full encoder to support field masks bigger than u32");

        let mut bytes_written = qint_encode(
            &mut writer,
            [delta, record.freq, field_mask, record.offsets_sz],
        )?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

/// Create a [`RSIndexResult`] from the given parameters and read its offsets from the reader.
#[inline(always)]
pub fn decode_term_record_offsets<'index>(
    cursor: &mut Cursor<&'index [u8]>,
    base: t_docId,
    delta: u32,
    field_mask: t_fieldMask,
    freq: u32,
    offsets_sz: u32,
) -> std::io::Result<RSIndexResult<'index, 'static>> {
    // borrow the offsets vector from the cursor
    let start = cursor.position() as usize;
    let end = start + offsets_sz as usize;
    let data = {
        let offsets = cursor.get_ref();
        if end > offsets.len() {
            // record wrongly claims to have `offsets_sz` offsets but the actual array is shorter.
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "offsets vector is too short",
            ));
        }
        // SAFETY: We just checked that `end` is in bound.
        let offsets = unsafe { offsets.get_unchecked(start..end) };
        offsets.as_ptr() as *mut _
    };

    cursor.set_position(end as u64);

    let offsets = RSOffsetVectorRef::with_data(data, offsets_sz);

    let record = RSIndexResult::term_with_term_ptr(
        std::ptr::null_mut(),
        offsets,
        base + delta as t_docId,
        field_mask,
        freq,
    );

    Ok(record)
}

impl Decoder for Full {
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index, 'static>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<4, _>(cursor)?;
        let [delta, freq, field_mask, offsets_sz] = decoded_values;

        let record = decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            freq,
            offsets_sz,
        )?;
        Ok(record)
    }
}

/// Encode and decode the delta, frequency, field mask and offsets of a term record.
///
/// This encoder supports larger field masks fitting in a `u128`.
/// Use [`Full`] for `u32` field masks.
///
/// The delta, frequency, and offsets lengths are encoded using [qint encoding](qint).
/// The field mask is then encoded using [varint encoding](varint).
///
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`.
#[derive(Default)]
pub struct FullWide;

impl Encoder for FullWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        &mut self,
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq, record.offsets_sz])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FullWide {
    fn decode<'index>(
        &self,
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
    ) -> std::io::Result<RSIndexResult<'index, 'static>> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, freq, offsets_sz] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(cursor)?;

        let record = decode_term_record_offsets(cursor, base, delta, field_mask, freq, offsets_sz)?;
        Ok(record)
    }
}
