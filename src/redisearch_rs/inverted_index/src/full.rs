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

use ffi::t_docId;
use qint::{qint_decode, qint_encode};

use crate::{
    Decoder, DecoderResult, Delta, Encoder, RSIndexResult, RSOffsetVector, RSResultType,
    RSTermRecord,
};

/// Encode and decode the detla, frequency, field mask and offsets of a term record.
///
/// The delta, frequency, field mask, and offsets lengths are first encoded using [qint encoding](qint).
/// The offsets themselves are then written directly.
///
/// This encoder only supports delta values that fit in a `u32`, and field masks that fit in a `u32`.
#[derive(Default)]
pub struct Full;

impl Encoder for Full {
    /// # Panics
    /// Panics if `delta` or `field_mask` cannot fit in a `u32`.
    #[inline(never)]
    fn encode<W: Write + Seek>(
        &self,
        mut writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        assert!(matches!(record.result_type, RSResultType::Term));

        let delta = delta
            .0
            .try_into()
            .expect("Full encoder only supports deltas that fit in u32");
        let field_mask = record
            .field_mask
            .try_into()
            .expect("Full encoder only supports field masks that fit in u32");
        let mut bytes_written = qint_encode(
            &mut writer,
            [delta, record.freq, field_mask, record.offsets_sz],
        )?;

        // SAFETY: We asserted the result_type above.
        let term = unsafe { &record.data.term };
        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let offsets = unsafe {
            std::slice::from_raw_parts(term.offsets.data as *const u8, term.offsets.len as usize)
        };

        bytes_written += writer.write(offsets)?;

        Ok(bytes_written)
    }
}

impl Decoder for Full {
    #[inline(never)]
    fn decode<R: Read>(&self, reader: &mut R, base: t_docId) -> std::io::Result<DecoderResult> {
        let (decoded_values, _bytes_consumed) = qint_decode::<4, _>(reader)?;
        let [delta, freq, field_mask, offsets_sz] = decoded_values;

        let mut record =
            RSIndexResult::token_record(base + delta as u64, field_mask as u128, freq, offsets_sz);

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

        Ok(DecoderResult::Record(record))
    }
}
