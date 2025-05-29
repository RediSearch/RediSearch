use std::io::Write;

pub use ffi::{t_docId, RSIndexResult};

/// Encoder to write a record into an index
pub trait Encoder {
    /// Write the record to the writer and return the number of bytes written. The delta is the
    /// pre-computed difference between the current document ID and the last document ID written.
    fn encode(writer: impl Write, delta: t_docId, record: &RSIndexResult)
        -> std::io::Result<usize>;
}

pub struct EncodeDocIdsOnly;

impl Encoder for EncodeDocIdsOnly {
    fn encode(
        mut writer: impl Write,
        delta: t_docId,
        _record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        let var_enc = varint_encode_u64(delta);
        let bytes_written = writer.write(&var_enc)?;

        Ok(bytes_written)
    }
}

const MSB: u8 = 0b1000_0000;

/// Does varint encoding for a u64
fn varint_encode_u64(mut value: u64) -> Vec<u8> {
    let mut buffer = Vec::new();

    while value >= 128 {
        buffer.push((value as u8) | MSB);
        value >>= 7;
    }

    buffer.push(value as u8);
    buffer.reverse();

    buffer
}
