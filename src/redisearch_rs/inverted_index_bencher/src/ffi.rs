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

pub fn encode_numeric(buffer: &mut Buffer, record: &mut inverted_index::RSIndexResult) -> usize {
    let mut buffer_writer = BufferWriter::new_at(buffer, 0);

    unsafe { bindings::encode_numeric(&mut buffer_writer as *const _ as *mut _, 0, record) }
}

pub fn read_numeric(buffer: &mut Buffer) -> (bool, inverted_index::RSIndexResult) {
    let buffer_reader = BufferReader::new(buffer);
    let mut block_reader =
        unsafe { bindings::NewIndexBlockReader(&buffer_reader as *const _ as *mut _, 0) };
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
        let mut buffer = Buffer::from_array([0; 8]);
        let tests = [(6.0, [0b110_00_000])];

        for (input, expected) in tests {
            // Reset buffer so that writes happen at the start
            buffer.reset();

            let mut record = inverted_index::RSIndexResult::numeric(input);

            let buffer_grew_size = encode_numeric(&mut buffer, &mut record);

            assert_eq!(buffer_grew_size, 0, "buffer had enough space");
            assert_eq!(buffer.as_slice(), expected);

            let (filtered, decoded_result) = read_numeric(&mut buffer);

            assert!(!filtered);
            assert_eq!(decoded_result, record);
        }
    }
}
