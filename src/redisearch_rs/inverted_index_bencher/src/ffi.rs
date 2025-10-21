/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    alloc::{Layout, alloc},
    ptr::NonNull,
};

use buffer::{Buffer, BufferReader, BufferWriter};

mod bindings {
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(unsafe_op_in_unsafe_fn)]
    #![allow(improper_ctypes)]
    #![allow(dead_code)]
    #![allow(clippy::ptr_offset_with_cast)]
    #![allow(clippy::useless_transmute)]

    use inverted_index::{NumericFilter, t_docId, t_fieldMask};

    // Type aliases for C bindings - types without lifetimes for C interop
    pub type RSIndexResult = inverted_index::RSIndexResult<'static>;
    pub type RSOffsetVector = inverted_index::RSOffsetVector<'static>;
    pub type IndexDecoderCtx = inverted_index::ReadFilter<'static>;

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

/// An extra wrapper around the ['Buffer`] which allows the C code to grow it correctly and for its
/// memory to be cleaned up when it goes out of scope.
pub struct TestBuffer(pub Buffer);

impl TestBuffer {
    /// Create a new `TestBuffer` with a specified capacity. The capacity given should be big
    /// enough so that the buffer will never need to grow during benchmarking. We want this
    /// because we don't want to measure the time it takes to grow the buffer. Failing to make
    /// the buffer big enough will result in a `SIGSEGV` signal when the benchmark is run.
    pub fn with_capacity(cap: usize) -> Self {
        let layout = Layout::array::<u8>(cap).unwrap();
        let data = unsafe { alloc(layout) };
        let buffer = unsafe { Buffer::new(NonNull::new(data).unwrap(), 0, cap) };

        Self(buffer)
    }
}

impl Drop for TestBuffer {
    fn drop(&mut self) {
        let layout = Layout::array::<u8>(self.0.0.cap).unwrap();
        unsafe { std::alloc::dealloc(self.0.0.data as *mut u8, layout) };
    }
}

impl std::fmt::Debug for TestBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

pub fn encode_numeric(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_numeric(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_numeric(
    buffer: &mut Buffer,
    base_id: u64,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_NumericFilter() };
    let mut result = inverted_index::RSIndexResult::numeric(0.0);

    let returned = unsafe { bindings::read_numeric(&mut block_reader, &ctx, &mut result) };

    (returned, result)
}

pub fn encode_full(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
    wide: bool,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    if wide {
        unsafe { bindings::encode_full_wide(buffer_writer.as_mut_ptr() as _, delta, record) }
    } else {
        unsafe { bindings::encode_full(buffer_writer.as_mut_ptr() as _, delta, record) }
    }
}

pub fn encode_freqs_only(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_freqs_only(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_freq_offsets_flags(
    buffer: &mut Buffer,
    base_id: u64,
    wide: bool,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = if wide {
        unsafe { bindings::read_freq_offsets_flags_wide(&mut block_reader, &ctx, &mut result) }
    } else {
        unsafe { bindings::read_freq_offsets_flags(&mut block_reader, &ctx, &mut result) }
    };

    (returned, result)
}

pub fn read_freqs(buffer: &mut Buffer, base_id: u64) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_NumericFilter() };
    let mut result = inverted_index::RSIndexResult::virt().doc_id(base_id);

    let returned = unsafe { bindings::read_freqs(&mut block_reader, &ctx, &mut result) };

    (returned, result)
}

pub fn encode_freqs_fields(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
    wide: bool,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    if wide {
        unsafe {
            bindings::encode_freqs_fields_wide(buffer_writer.as_mut_ptr() as _, delta, record)
        }
    } else {
        unsafe { bindings::encode_freqs_fields(buffer_writer.as_mut_ptr() as _, delta, record) }
    }
}

pub fn read_freqs_flags(
    buffer: &mut Buffer,
    base_id: u64,
    wide: bool,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = if wide {
        unsafe { bindings::read_freqs_flags_wide(&mut block_reader, &ctx, &mut result) }
    } else {
        unsafe { bindings::read_freqs_flags(&mut block_reader, &ctx, &mut result) }
    };

    (returned, result)
}

pub fn encode_fields_only(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
    wide: bool,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    if wide {
        unsafe { bindings::encode_fields_only_wide(buffer_writer.as_mut_ptr() as _, delta, record) }
    } else {
        unsafe { bindings::encode_fields_only(buffer_writer.as_mut_ptr() as _, delta, record) }
    }
}

pub fn read_flags(
    buffer: &mut Buffer,
    base_id: u64,
    wide: bool,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = if wide {
        unsafe { bindings::read_flags_wide(&mut block_reader, &ctx, &mut result) }
    } else {
        unsafe { bindings::read_flags(&mut block_reader, &ctx, &mut result) }
    };

    (returned, result)
}

pub fn encode_doc_ids_only(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_docs_ids_only(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_doc_ids_only(
    buffer: &mut Buffer,
    base_id: u64,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = unsafe { bindings::read_doc_ids_only(&mut block_reader, &ctx, &mut result) };
    (returned, result)
}

pub fn encode_fields_offsets(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
    wide: bool,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    if wide {
        unsafe {
            bindings::encode_fields_offsets_wide(buffer_writer.as_mut_ptr() as _, delta, record)
        }
    } else {
        unsafe { bindings::encode_fields_offsets(buffer_writer.as_mut_ptr() as _, delta, record) }
    }
}

pub fn read_fields_offsets(
    buffer: &mut Buffer,
    base_id: u64,
    wide: bool,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = if wide {
        unsafe { bindings::read_fields_offsets_wide(&mut block_reader, &ctx, &mut result) }
    } else {
        unsafe { bindings::read_fields_offsets(&mut block_reader, &ctx, &mut result) }
    };

    (returned, result)
}

pub fn encode_offsets_only(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_offsets_only(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_offsets_only(
    buffer: &mut Buffer,
    base_id: u64,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = unsafe { bindings::read_offsets_only(&mut block_reader, &ctx, &mut result) };

    (returned, result)
}

pub fn encode_freqs_offsets(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_freqs_offsets(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_freqs_offsets(
    buffer: &mut Buffer,
    base_id: u64,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = unsafe { bindings::read_freqs_offsets(&mut block_reader, &ctx, &mut result) };

    (returned, result)
}

pub fn encode_raw_doc_ids_only(
    buffer: &mut TestBuffer,
    record: &mut bindings::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_raw_doc_ids_only(buffer_writer.as_mut_ptr() as _, delta, record) }
}

pub fn read_raw_doc_ids_only(
    buffer: &mut Buffer,
    base_id: u64,
) -> (bool, inverted_index::RSIndexResult<'_>) {
    let mut buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(buffer_reader.as_mut_ptr() as _, base_id) };
    let ctx = unsafe { bindings::NewIndexDecoderCtx_MaskFilter(1) };
    let mut result = inverted_index::RSIndexResult::term().doc_id(base_id);

    let returned = unsafe { bindings::read_raw_doc_ids_only(&mut block_reader, &ctx, &mut result) };

    (returned, result)
}

#[cfg(test)]
// `miri` can't handle FFI.
#[cfg(not(miri))]
mod tests {
    use super::*;
    use ffi::RSQueryTerm;
    use ffi::t_fieldMask;
    use inverted_index::RSOffsetVector;

    // The encode C implementation relies on these symbols. Re-export them to ensure they are not discarded by the linker.
    #[allow(unused_imports)]
    pub use types_ffi::RSOffsetVector_GetData;
    #[allow(unused_imports)]
    pub use varint_ffi::WriteVarintFieldMask;

    #[test]
    #[ignore]
    fn test_encode_numeric() {
        // Test cases for all the different numeric encodings. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // Tiny int tests
            (2.0, 0, vec![0b010_00_000]),
            (7.0, 2, vec![0b111_00_001, 2]),
            (0.0, 256, vec![0b000_00_010, 0, 1]),
            // Pos int tests
            (16.0, 1, vec![0b000_10_001, 1, 16]),
            (256.0, 0, vec![0b001_10_000, 0, 1]),
            // Neg int tests
            (-16.0, 1, vec![0b000_11_001, 1, 16]),
            (-256.0, 0, vec![0b001_11_000, 0, 1]),
            // Pos small float test
            (3.125, 1, vec![0b000_01_001, 1, 0, 0, 72, 64]),
            (3.125, 0, vec![0b000_01_000, 0, 0, 72, 64]),
            // Neg small float test
            (-3.125, 1, vec![0b010_01_001, 1, 0, 0, 72, 64]),
            (-3.125, 0, vec![0b010_01_000, 0, 0, 72, 64]),
            // Pos infinite float test
            (f64::INFINITY, 1, vec![0b001_01_001, 1]),
            (f64::INFINITY, 0, vec![0b001_01_000]),
            // Neg infinite float test
            (f64::NEG_INFINITY, 1, vec![0b011_01_001, 1]),
            (f64::NEG_INFINITY, 0, vec![0b011_01_000]),
            // Pos big float test
            (
                3.124,
                1,
                vec![0b100_01_001, 1, 203, 161, 69, 182, 243, 253, 8, 64],
            ),
            (
                3.124,
                0,
                vec![0b100_01_000, 203, 161, 69, 182, 243, 253, 8, 64],
            ),
            // Neg big float test
            (
                -3.124,
                1,
                vec![0b110_01_001, 1, 203, 161, 69, 182, 243, 253, 8, 64],
            ),
            (
                -3.124,
                0,
                vec![0b110_01_000, 203, 161, 69, 182, 243, 253, 8, 64],
            ),
        ];

        for (input, delta, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(16);

            let mut record = inverted_index::RSIndexResult::numeric(input).doc_id(1_000);

            let _buffer_grew_size = encode_numeric(&mut buffer, &mut record, delta);

            assert_eq!(
                buffer.0.as_slice(),
                expected_encoding,
                "does not match for input: {}",
                input
            );

            let base_id = 1_000 - delta;
            let (returned, decoded_result) = read_numeric(&mut buffer.0, base_id);

            assert!(returned);
            assert_eq!(
                decoded_result, record,
                "does not match for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_encode_freqs_only() {
        // Test cases for the frequencies only encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (frequency, delta, expected encoding)
            (0, 0, vec![0, 0, 0]),
            (0, 1, vec![0, 1, 0]),
            (2, 0, vec![0, 0, 2]),
            (2, 1, vec![0, 1, 2]),
            (256, 0, vec![4, 0, 0, 1]),
            (256, 256, vec![5, 0, 1, 0, 1]),
            (2, 65536, vec![2, 0, 0, 1, 2]),
            (
                u16::MAX as u32 + 1,
                u16::MAX as u64 + 1,
                vec![10, 0, 0, 1, 0, 0, 1],
            ),
            (2, u32::MAX as u64, vec![3, 255, 255, 255, 255, 2]),
            (
                u32::MAX,
                u32::MAX as u64,
                vec![15, 255, 255, 255, 255, 255, 255, 255, 255],
            ),
        ];
        let doc_id = 4294967296;

        for (freq, delta, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());
            let mut record = inverted_index::RSIndexResult::virt()
                .doc_id(doc_id)
                .frequency(freq);

            let _buffer_grew_size = encode_freqs_only(&mut buffer, &mut record, delta);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freqs(&mut buffer.0, base_id);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    #[test]
    fn test_encode_freqs_fields() {
        // Test cases for the freqs field encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, frequency, field mask, expected encoding)
            (0, 1, 1, vec![0, 0, 1, 1]),
            (
                10,
                5,
                u32::MAX as t_fieldMask,
                vec![48, 10, 5, 255, 255, 255, 255],
            ),
            (256, 1, 1, vec![1, 0, 1, 1, 1]),
            (65536, 1, 1, vec![2, 0, 0, 1, 1, 1]),
            (u16::MAX as u64, 1, 1, vec![1, 255, 255, 1, 1]),
            (u32::MAX as u64, 1, 1, vec![3, 255, 255, 255, 255, 1, 1]),
            (
                u32::MAX as u64,
                u32::MAX,
                u32::MAX as t_fieldMask,
                vec![
                    63, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, freq, field_mask, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term()
                .doc_id(doc_id)
                .field_mask(field_mask)
                .frequency(freq);

            let _buffer_grew_size = encode_freqs_fields(&mut buffer, &mut record, delta, false);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freqs_flags(&mut buffer.0, base_id, false);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    #[test]
    fn test_encode_freqs_fields_wide() {
        // Test cases for the freqs field wide encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, frequency, field mask, expected encoding)
            (0, 1, 1, vec![0, 0, 1, 1]),
            (
                10,
                5,
                u32::MAX as t_fieldMask,
                vec![0, 10, 5, 142, 254, 254, 254, 127],
            ),
            (256, 1, 1, vec![1, 0, 1, 1, 1]),
            (65536, 1, 1, vec![2, 0, 0, 1, 1, 1]),
            (u16::MAX as u64, 1, 1, vec![1, 255, 255, 1, 1]),
            (u32::MAX as u64, 1, 1, vec![3, 255, 255, 255, 255, 1, 1]),
            // field mask larger than 32 bits, only supported on 64-bit systems
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX,
                u32::MAX as t_fieldMask,
                vec![
                    15, 255, 255, 255, 255, 255, 255, 255, 255, 142, 254, 254, 254, 127,
                ],
            ),
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX,
                u128::MAX,
                vec![
                    15, 255, 255, 255, 255, 255, 255, 255, 255, 130, 254, 254, 254, 254, 254, 254,
                    254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 127,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, freq, field_mask, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term()
                .doc_id(doc_id)
                .field_mask(field_mask)
                .frequency(freq);

            let _buffer_grew_size = encode_freqs_fields(&mut buffer, &mut record, delta, true);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freqs_flags(&mut buffer.0, base_id, true);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    #[test]
    fn test_encode_fields_only() {
        // Test cases for the fields encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, field mask, expected encoding)
            (0, 1, vec![0, 0, 1]),
            (
                10,
                u32::MAX as t_fieldMask,
                vec![12, 10, 255, 255, 255, 255],
            ),
            (256, 1, vec![1, 0, 1, 1]),
            (65536, 1, vec![2, 0, 0, 1, 1]),
            (u16::MAX as u64, 1, vec![1, 255, 255, 1]),
            (u32::MAX as u64, 1, vec![3, 255, 255, 255, 255, 1]),
            (
                u32::MAX as u64,
                u32::MAX as t_fieldMask,
                vec![15, 255, 255, 255, 255, 255, 255, 255, 255],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, field_mask, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term()
                .doc_id(doc_id)
                .field_mask(field_mask);

            let _buffer_grew_size = encode_fields_only(&mut buffer, &mut record, delta, false);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_flags(&mut buffer.0, base_id, false);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    #[test]
    fn test_encode_fields_only_wide() {
        // Test cases for the wide fields encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, field mask, expected encoding)
            (0, 1, vec![0, 1]),
            (
                10,
                u32::MAX as t_fieldMask,
                vec![10, 142, 254, 254, 254, 127],
            ),
            (256, 1, vec![129, 0, 1]),
            (65536, 1, vec![130, 255, 0, 1]),
            (u16::MAX as u64, 1, vec![130, 254, 127, 1]),
            (u32::MAX as u64, 1, vec![142, 254, 254, 254, 127, 1]),
            (
                u32::MAX as u64,
                u32::MAX as t_fieldMask,
                vec![142, 254, 254, 254, 127, 142, 254, 254, 254, 127],
            ),
            // field mask larger than 32 bits
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX as t_fieldMask,
                vec![142, 254, 254, 254, 127, 142, 254, 254, 254, 127],
            ),
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u128::MAX,
                vec![
                    142, 254, 254, 254, 127, 130, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                    254, 254, 254, 254, 254, 254, 254, 127,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, field_mask, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term()
                .doc_id(doc_id)
                .field_mask(field_mask);

            let _buffer_grew_size = encode_fields_only(&mut buffer, &mut record, delta, true);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_flags(&mut buffer.0, base_id, true);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    #[test]
    fn test_doc_ids_only() {
        // Test cases for the docs ids only encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, expected encoding)
            (0, vec![0]),
            (10, vec![10]),
            (256, vec![129, 0]),
            (65536, vec![130, 255, 0]),
            (u16::MAX as u64, vec![130, 254, 127]),
            (u32::MAX as u64, vec![142, 254, 254, 254, 127]),
        ];

        let doc_id = 4294967296;

        for (delta, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term().doc_id(doc_id);

            let _buffer_grew_size = encode_doc_ids_only(&mut buffer, &mut record, delta);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_doc_ids_only(&mut buffer.0, base_id);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }

    /// Helper to compare only the fields of a term record that are actually encoded.
    #[derive(Debug)]
    struct TermRecordCompare<'index>(&'index inverted_index::RSIndexResult<'index>);

    impl<'index> PartialEq for TermRecordCompare<'index> {
        fn eq(&self, other: &Self) -> bool {
            assert!(matches!(self.0.kind(), inverted_index::RSResultKind::Term));

            if !(self.0.doc_id == other.0.doc_id
                && self.0.dmd == other.0.dmd
                && self.0.field_mask == other.0.field_mask
                && self.0.freq == other.0.freq
                && self.0.kind() == other.0.kind()
                && self.0.metrics == other.0.metrics)
            {
                return false;
            }

            // do not compare `weight` as it's not encoded

            // SAFETY: we asserted the type above
            let a_term_record = self.0.as_term().unwrap();
            // SAFETY: we checked that other has the same type as self
            let b_term_record = other.0.as_term().unwrap();

            let a_offsets = a_term_record.offsets();

            let b_offsets = b_term_record.offsets();

            if a_offsets != b_offsets {
                return false;
            }

            // do not compare `RSTermRecord` as it's not encoded

            a_term_record.is_copy() == b_term_record.is_copy()
        }
    }

    #[test]
    fn test_encode_full() {
        // Test cases for the full encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, frequency, field mask, term offsets vector, expected encoding)
            (0, 1, 1, vec![1i8, 2, 3], vec![0, 0, 1, 1, 3, 1, 2, 3]),
            (
                10,
                5,
                u32::MAX as t_fieldMask,
                vec![1i8, 2, 3, 4],
                vec![48, 10, 5, 255, 255, 255, 255, 4, 1, 2, 3, 4],
            ),
            (256, 1, 1, vec![1, 2, 3], vec![1, 0, 1, 1, 1, 3, 1, 2, 3]),
            (
                65536,
                1,
                1,
                vec![1, 2, 3],
                vec![2, 0, 0, 1, 1, 1, 3, 1, 2, 3],
            ),
            (
                u16::MAX as u64,
                1,
                1,
                vec![1, 2, 3],
                vec![1, 255, 255, 1, 1, 3, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                1,
                1,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 1, 1, 3, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                u32::MAX,
                u32::MAX as t_fieldMask,
                vec![1; 100],
                vec![
                    63, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 100, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, freq, field_mask, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, field_mask, freq,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_full(&mut buffer, &mut record, delta, false);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freq_offsets_flags(&mut buffer.0, base_id, false);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_full_wide() {
        // Test cases for the full wide encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.

        // The encode C implementation relies on this symbol. Re-export it to ensure it is not discarded by the linker.
        #[allow(unused_imports)]
        pub use varint_ffi::WriteVarintFieldMask;

        let tests = [
            // (delta, frequency, field mask, term offsets vector, expected encoding)
            (0, 1, 1, vec![1i8, 2, 3], vec![0, 0, 1, 3, 1, 1, 2, 3]),
            (
                10,
                5,
                u32::MAX as t_fieldMask,
                vec![1i8, 2, 3, 4],
                vec![0, 10, 5, 4, 142, 254, 254, 254, 127, 1, 2, 3, 4],
            ),
            (256, 1, 1, vec![1, 2, 3], vec![1, 0, 1, 1, 3, 1, 1, 2, 3]),
            (
                65536,
                1,
                1,
                vec![1, 2, 3],
                vec![2, 0, 0, 1, 1, 3, 1, 1, 2, 3],
            ),
            (
                u16::MAX as u64,
                1,
                1,
                vec![1, 2, 3],
                vec![1, 255, 255, 1, 3, 1, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                1,
                1,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 1, 3, 1, 1, 2, 3],
            ),
            // field mask larger than 32 bits
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX,
                u32::MAX as t_fieldMask,
                vec![1; 100],
                vec![
                    15, 255, 255, 255, 255, 255, 255, 255, 255, 100, 142, 254, 254, 254, 127, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ],
            ),
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX,
                u128::MAX,
                vec![1; 100],
                vec![
                    15, 255, 255, 255, 255, 255, 255, 255, 255, 100, 130, 254, 254, 254, 254, 254,
                    254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 127, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, freq, field_mask, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, field_mask, freq,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_full(&mut buffer, &mut record, delta, true);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freq_offsets_flags(&mut buffer.0, base_id, true);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_fields_offsets() {
        // Test cases for the fields/offsets encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, field mask, term offsets vector, expected encoding)
            (0, 1, vec![1i8, 2, 3], vec![0, 0, 1, 3, 1, 2, 3]),
            (
                10,
                u32::MAX as t_fieldMask,
                vec![1i8, 2, 3, 4],
                vec![12, 10, 255, 255, 255, 255, 4, 1, 2, 3, 4],
            ),
            (256, 1, vec![1, 2, 3], vec![1, 0, 1, 1, 3, 1, 2, 3]),
            (65536, 1, vec![1, 2, 3], vec![2, 0, 0, 1, 1, 3, 1, 2, 3]),
            (
                u16::MAX as u64,
                1,
                vec![1, 2, 3],
                vec![1, 255, 255, 1, 3, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                1,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 1, 3, 1, 2, 3],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, field_mask, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, field_mask, 1,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_fields_offsets(&mut buffer, &mut record, delta, false);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_fields_offsets(&mut buffer.0, base_id, false);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_fields_offsets_wide() {
        // Test cases for the fields/offsets wide encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, field mask, term offsets vector, expected encoding)
            (0, 1, vec![1i8, 2, 3], vec![0, 0, 3, 1, 1, 2, 3]),
            (
                10,
                u32::MAX as t_fieldMask,
                vec![1i8, 2, 3, 4],
                vec![0, 10, 4, 142, 254, 254, 254, 127, 1, 2, 3, 4],
            ),
            (256, 1, vec![1, 2, 3], vec![1, 0, 1, 3, 1, 1, 2, 3]),
            (65536, 1, vec![1, 2, 3], vec![2, 0, 0, 1, 3, 1, 1, 2, 3]),
            (
                u16::MAX as u64,
                1,
                vec![1, 2, 3],
                vec![1, 255, 255, 3, 1, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                1,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 3, 1, 1, 2, 3],
            ),
            // field mask larger than 32 bits
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u32::MAX as t_fieldMask,
                vec![1; 100],
                vec![
                    3, 255, 255, 255, 255, 100, 142, 254, 254, 254, 127, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ],
            ),
            #[cfg(target_pointer_width = "64")]
            (
                u32::MAX as u64,
                u128::MAX as t_fieldMask,
                vec![1; 100],
                vec![
                    3, 255, 255, 255, 255, 100, 130, 254, 254, 254, 254, 254, 254, 254, 254, 254,
                    254, 254, 254, 254, 254, 254, 254, 254, 127, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                ],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, field_mask, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, field_mask, 1,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_fields_offsets(&mut buffer, &mut record, delta, true);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_fields_offsets(&mut buffer.0, base_id, true);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_offsets_only() {
        // Test cases for the offsets only encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, term offsets vector, expected encoding)
            (0, vec![1i8, 2, 3], vec![0, 0, 3, 1, 2, 3]),
            (10, vec![1i8, 2, 3, 4], vec![0, 10, 4, 1, 2, 3, 4]),
            (256, vec![1, 2, 3], vec![1, 0, 1, 3, 1, 2, 3]),
            (65536, vec![1, 2, 3], vec![2, 0, 0, 1, 3, 1, 2, 3]),
            (
                u16::MAX as u64,
                vec![1, 2, 3],
                vec![1, 255, 255, 3, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 3, 1, 2, 3],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, 0, 1,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_offsets_only(&mut buffer, &mut record, delta);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_offsets_only(&mut buffer.0, base_id);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_freqs_offsets() {
        // Test cases for the freqs offsets encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, freq, term offsets vector, expected encoding)
            (0, 1, vec![1i8, 2, 3], vec![0, 0, 1, 3, 1, 2, 3]),
            (10, 2, vec![1i8, 2, 3, 4], vec![0, 10, 2, 4, 1, 2, 3, 4]),
            (256, 3, vec![1, 2, 3], vec![1, 0, 1, 3, 3, 1, 2, 3]),
            (65536, 4, vec![1, 2, 3], vec![2, 0, 0, 1, 4, 3, 1, 2, 3]),
            (
                u16::MAX as u64,
                5,
                vec![1, 2, 3],
                vec![1, 255, 255, 5, 3, 1, 2, 3],
            ),
            (
                u32::MAX as u64,
                6,
                vec![1, 2, 3],
                vec![3, 255, 255, 255, 255, 6, 3, 1, 2, 3],
            ),
        ];
        let doc_id = 4294967296;

        for (delta, freq, offsets, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            const TEST_STR: &str = "test";
            let test_str_ptr = TEST_STR.as_ptr() as *mut _;
            let mut term = RSQueryTerm {
                str_: test_str_ptr,
                len: TEST_STR.len(),
                idf: 5.0,
                id: 1,
                flags: 0,
                bm25_idf: 10.0,
            };

            let offsets_ptr = offsets.as_ptr() as *mut _;
            let rs_offsets = RSOffsetVector::with_data(offsets_ptr, offsets.len() as _);

            let mut record = inverted_index::RSIndexResult::term_with_term_ptr(
                &mut term, rs_offsets, doc_id, 0, freq,
            )
            .weight(1.0);

            let _buffer_grew_size = encode_freqs_offsets(&mut buffer, &mut record, delta);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_freqs_offsets(&mut buffer.0, base_id);
            assert!(returned);
            assert_eq!(
                TermRecordCompare(&decoded_result),
                TermRecordCompare(&record)
            );
        }
    }

    #[test]
    fn test_encode_raw_doc_ids_only() {
        // Test cases for the raw doc ids only encoder and decoder. These cases can be moved to the Rust
        // implementation tests verbatim.
        let tests = [
            // (delta, expected encoding)
            (0, vec![0, 0, 0, 0]),
            (10, vec![10, 0, 0, 0]),
            (256, vec![0, 1, 0, 0]),
            (65536, vec![0, 0, 1, 0]),
            (u16::MAX as u64, vec![255, 255, 0, 0]),
            (u32::MAX as u64, vec![255, 255, 255, 255]),
        ];

        let doc_id = 4294967296;

        for (delta, expected_encoding) in tests {
            let mut buffer = TestBuffer::with_capacity(expected_encoding.len());

            let mut record = inverted_index::RSIndexResult::term().doc_id(doc_id);

            let _buffer_grew_size = encode_raw_doc_ids_only(&mut buffer, &mut record, delta);
            assert_eq!(buffer.0.as_slice(), expected_encoding);

            let base_id = doc_id - delta;
            let (returned, decoded_result) = read_raw_doc_ids_only(&mut buffer.0, base_id);
            assert!(returned);
            assert_eq!(decoded_result, record);
        }
    }
}
