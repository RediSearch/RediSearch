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

use crate::{Decoder, Encoder, RSIndexResult, RSOffsetVector, RSResultData, TermDecoder};

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
pub struct Full;

/// Return a slice of the offsets vector from a term record.
///
/// # Safety
///
/// record must have `result_type` set to `RSResultType::Term`.
#[inline(always)]
pub const fn offsets<'a>(record: &'a RSIndexResult<'_>) -> &'a [u8] {
    // SAFETY: caller ensured the proper result_type.
    let term = record.as_term().unwrap();

    term.offsets()
}

impl Encoder for Full {
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
                .expect("Need to use the wide variant of the Full encoder to support field masks bigger than u32");

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written =
            qint_encode(&mut writer, [delta, record.freq, field_mask, offsets_sz])?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

/// Create a [`RSIndexResult`] from the given parameters and read its offsets from the reader.
///
/// # Safety
///
/// 1. `result.is_term()` must be true when this function is called.
#[inline(always)]
pub fn decode_term_record_offsets<'index>(
    cursor: &mut Cursor<&'index [u8]>,
    base: t_docId,
    delta: u32,
    field_mask: t_fieldMask,
    freq: u32,
    offsets_sz: u32,
    result: &mut RSIndexResult<'index>,
) -> std::io::Result<()> {
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

    let offsets = RSOffsetVector::with_data(data, offsets_sz);

    result.doc_id = base + delta as t_docId;
    result.field_mask = field_mask;
    result.freq = freq;

    // SAFETY: caller must ensure `result` is a term record.
    let term = unsafe { result.as_term_unchecked_mut() };
    term.set_offsets(offsets);

    Ok(())
}

impl Decoder for Full {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<4, _>(cursor)?;
        let [delta, freq, field_mask, offsets_sz] = decoded_values;

        decode_term_record_offsets(
            cursor,
            base,
            delta,
            field_mask as t_fieldMask,
            freq,
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
        let (freq, field_mask, offsets_sz) = loop {
            let [delta, freq, field_mask, offsets_sz] = match qint_decode::<4, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };

            base += delta as t_docId;

            if base >= target {
                break (freq, field_mask as t_fieldMask, offsets_sz);
            }

            // Skip offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(
            cursor,
            base,
            0,
            field_mask as t_fieldMask,
            freq,
            offsets_sz,
            result,
        )?;
        Ok(true)
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
pub struct FullWide;

impl Encoder for FullWide {
    type Delta = u32;

    fn encode<W: Write + Seek>(
        mut writer: W,
        delta: Self::Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.data, RSResultData::Term(_)));

        let offsets = offsets(record);
        let offsets_sz = offsets.len() as u32;

        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq, offsets_sz])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FullWide {
    #[inline(always)]
    fn decode<'index>(
        cursor: &mut Cursor<&'index [u8]>,
        base: t_docId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<()> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(cursor)?;
        let [delta, freq, offsets_sz] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(cursor)?;

        decode_term_record_offsets(cursor, base, delta, field_mask, freq, offsets_sz, result)
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
        let (freq, field_mask, offsets_sz) = loop {
            let [delta, freq, offsets_sz] = match qint_decode::<3, _>(cursor) {
                Ok((decoded_values, _bytes_consumed)) => decoded_values,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            let field_mask = t_fieldMask::read_as_varint(cursor)?;

            base += delta as t_docId;

            if base >= target {
                break (freq, field_mask, offsets_sz);
            }

            // Skip offsets
            cursor.seek(SeekFrom::Current(offsets_sz as i64))?;
        };

        decode_term_record_offsets(cursor, base, 0, field_mask, freq, offsets_sz, result)?;
        Ok(true)
    }
}

impl TermDecoder for Full {}
impl TermDecoder for FullWide {}
