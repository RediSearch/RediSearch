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

    use inverted_index::{RSIndexResult, RSOffsetVector, t_docId, t_fieldMask};

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

/// An extra wrapper around the ['Buffer`] which allows the C code to grow it correctly and for its
/// memory to be cleaned up when it goes out of scope.
pub struct TestBuffer(Buffer);

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

pub fn encode_numeric(
    buffer: &mut TestBuffer,
    record: &mut inverted_index::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(&mut buffer.0);

    unsafe { bindings::encode_numeric(&mut buffer_writer as *const _ as *mut _, delta, record) }
}

pub fn read_numeric(buffer: &mut Buffer, base_id: u64) -> (bool, inverted_index::RSIndexResult) {
    let buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(&buffer_reader as *const _ as *mut _, base_id) };
    let mut ctx = unsafe { bindings::NewIndexDecoderCtx_NumericFilter() };
    let mut result = inverted_index::RSIndexResult::numeric(0.0);

    let returned = unsafe { bindings::read_numeric(&mut block_reader, &mut ctx, &mut result) };

    (returned, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
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

            let mut record = inverted_index::RSIndexResult::numeric(input);
            record.doc_id = 1_000;

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
}
