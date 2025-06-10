use std::ptr::NonNull;

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

pub fn encode_numeric(buffer: &mut [u8], record: &mut inverted_index::RSIndexResult) -> usize {
    let mut buffer =
        unsafe { Buffer::new(NonNull::new(buffer.as_mut_ptr()).unwrap(), 0, buffer.len()) };
    let mut buffer_writer = BufferWriter::new_at(&mut buffer, 0);

    unsafe { bindings::encode_numeric(&mut buffer_writer as *const _ as *mut _, 0, record) }
}

pub fn read_numeric(buffer: &mut [u8]) -> (bool, inverted_index::RSIndexResult) {
    let mut buffer =
        unsafe { Buffer::new(NonNull::new(buffer.as_mut_ptr()).unwrap(), 0, buffer.len()) };
    let buffer_reader = BufferReader::new(&mut buffer);
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
        let mut record = inverted_index::RSIndexResult::numeric(6.0);

        let mut buffer = vec![0u8; 8];

        let buffer_grew_size = encode_numeric(&mut buffer, &mut record);

        assert_eq!(buffer_grew_size, 0, "buffer had enough space of 1024 bytes");
        assert_eq!(buffer, [0b110_00_000, 0, 0, 0, 0, 0, 0, 0]);

        let (filtered, decoded_result) = read_numeric(&mut buffer);

        assert!(!filtered);
        assert_eq!(decoded_result, record);
    }
}
