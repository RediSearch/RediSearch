/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    io::{Read, Seek, Write},
    mem::ManuallyDrop,
};

use ffi::{t_docId, t_fieldMask};
use qint::{qint_decode, qint_encode};
use varint::VarintEncode;

use crate::{
    Decoder, DecoderResult, Encoder, RSIndexResult, RSOffsetVector, RSResultType, RSTermRecord,
};

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
fn offsets(record: &RSIndexResult) -> &[u8] {
    // SAFETY: caller ensured the proper result_type.
    let term = unsafe { &record.data.term };
    if term.offsets.data.is_null() {
        &[]
    } else {
        assert_eq!(term.offsets.len, record.offsets_sz);

        // SAFETY: We checked that data is not NULL and `len` is guaranteed to be a valid length for the data pointer.
        unsafe {
            std::slice::from_raw_parts(term.offsets.data as *const u8, term.offsets.len as usize)
        }
    }
}

impl Encoder for Full {
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

/// Create a [`RSIndexResult`] from the given parameters and read its offsets fron the reader.
#[inline(always)]
fn decode_term_record_offsets<R: Read>(
    reader: &mut R,
    base: t_docId,
    delta: u32,
    field_mask: t_fieldMask,
    freq: u32,
    offsets_sz: u32,
) -> std::io::Result<RSIndexResult> {
    let mut record = RSIndexResult::term()
        .doc_id(base + delta as t_docId)
        .field_mask(field_mask)
        .frequency(freq);
    record.offsets_sz = offsets_sz;

    // read the offsets vector
    // FIXME: leaked
    let mut offsets = Vec::with_capacity(offsets_sz as usize);
    // SAFETY: We just allocated this size in the vector.
    unsafe { offsets.set_len(offsets_sz as usize) };
    reader.read_exact(&mut offsets)?;
    let offsets_ptr = Box::into_raw(offsets.into_boxed_slice()) as *mut _;

    record.data.term = ManuallyDrop::new(RSTermRecord {
        term: std::ptr::null_mut(),
        offsets: RSOffsetVector {
            data: offsets_ptr,
            len: offsets_sz,
        },
    });

    Ok(record)
}

impl Decoder for Full {
    fn decode<R: Read>(&self, reader: &mut R, base: t_docId) -> std::io::Result<DecoderResult> {
        let (decoded_values, _bytes_consumed) = qint_decode::<4, _>(reader)?;
        let [delta, freq, field_mask, offsets_sz] = decoded_values;

        let record = decode_term_record_offsets(
            reader,
            base,
            delta,
            field_mask as t_fieldMask,
            freq,
            offsets_sz,
        )?;
        Ok(DecoderResult::Record(record))
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
        assert!(matches!(record.result_type, RSResultType::Term));

        let mut bytes_written = qint_encode(&mut writer, [delta, record.freq, record.offsets_sz])?;
        bytes_written += record.field_mask.write_as_varint(&mut writer)?;

        let offsets = offsets(record);
        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for FullWide {
    fn decode<R: Read>(&self, reader: &mut R, base: t_docId) -> std::io::Result<DecoderResult> {
        let (decoded_values, _bytes_consumed) = qint_decode::<3, _>(reader)?;
        let [delta, freq, offsets_sz] = decoded_values;
        let field_mask = t_fieldMask::read_as_varint(reader)?;

        let record = decode_term_record_offsets(reader, base, delta, field_mask, freq, offsets_sz)?;
        Ok(DecoderResult::Record(record))
    }
}
