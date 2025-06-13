use std::io::Cursor;

use crate::{Decoder, DecoderResult, Delta, Encoder, Numeric, RSIndexResult};
use pretty_assertions::assert_eq;

#[test]
fn numeric_tiny_int() {
    let inputs = [
        (
            0,
            2.0,
            1,
            vec![0b010_00_000], // Header bits 7→0: |010|00|000| = value:2, type:TINY, delta_bytes:0
        ),
        (
            2,
            7.0,
            2,
            vec![
                0b111_00_001, // Header bits 7→0: |111|00|001| = value:7, type:TINY, delta_bytes:1
                0b0000_0010,  // Delta: 2
            ],
        ),
        (
            256,
            0.0,
            3,
            vec![
                0b000_00_010, // Header bits 7→0: |000|00|010| = value:0, type:TINY, delta_bytes:2
                0b0000_0000,  // Delta byte 0 (LSB)
                0b0000_0001,  // Delta byte 1 (MSB) → 256 = 0x0100
            ],
        ),
    ];

    for input in inputs {
        let (delta, value, expected_bytes_written, expected_buffer) = input;
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::numeric(1000, value);

        let bytes_written =
            Numeric::encode(&mut buf, Delta(delta), &record).expect("to encode numeric record");

        let buf = buf.into_inner();
        assert_eq!(bytes_written, expected_bytes_written);
        assert_eq!(buf, expected_buffer);

        let prev_doc_id = 1000 - (delta as u64);
        let DecoderResult::Record(record_decoded) = Numeric
            .decode(buf.as_slice(), prev_doc_id)
            .expect("to decode numeric record")
            .expect("to read a record from the buffer")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(record_decoded, record);
    }
}
