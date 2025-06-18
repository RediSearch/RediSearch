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

pub fn encode_numeric(
    buffer: &mut Buffer,
    record: &mut inverted_index::RSIndexResult,
    delta: u64,
) -> usize {
    let mut buffer_writer = BufferWriter::new(buffer);

    unsafe { bindings::encode_numeric(&mut buffer_writer as *const _ as *mut _, delta, record) }
}

pub fn read_numeric(buffer: &mut Buffer, base_id: u64) -> (bool, inverted_index::RSIndexResult) {
    let buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(&buffer_reader as *const _ as *mut _, base_id) };
    let mut ctx = unsafe { bindings::NewIndexDecoderCtx_NumericFilter() };
    let mut result = inverted_index::RSIndexResult::numeric(0.0);

    let filtered = unsafe { bindings::read_numeric(&mut block_reader, &mut ctx, &mut result) };

    (filtered, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_numeric() {
        let mut buffer = Buffer::from_array([0; 66]);
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
            (-f64::INFINITY, 1, vec![0b011_01_001, 1]),
            (-f64::INFINITY, 0, vec![0b011_01_000]),
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
            // Reset buffer so that writes happen at the start
            buffer.reset();

            let mut record = inverted_index::RSIndexResult::numeric(input);
            record.doc_id = 1_000;

            let buffer_grew_size = encode_numeric(&mut buffer, &mut record, delta);

            assert_eq!(buffer_grew_size, 0, "buffer had enough space");
            assert_eq!(
                buffer.as_slice(),
                expected_encoding,
                "does not match for input: {}",
                input
            );

            let base_id = 1_000 - delta;
            let (filtered, decoded_result) = read_numeric(&mut buffer, base_id);

            assert!(!filtered);
            assert_eq!(
                decoded_result, record,
                "does not match for input: {}",
                input
            );
        }
    }
}
