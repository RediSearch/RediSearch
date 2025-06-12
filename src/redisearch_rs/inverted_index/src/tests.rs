use std::io::Cursor;

use crate::{Encoder, Numeric, RSIndexResult};

#[test]
fn numeric_tiny_int() {
    let inputs = [
        (
            crate::Delta(0),
            RSIndexResult::numeric(2.0),
            1,
            vec![0b010_00_000], // Header bits 7→0: |010|00|000| = value:2, type:TINY, delta_bytes:0
        ),
        (
            crate::Delta(2),
            RSIndexResult::numeric(7.0),
            2,
            vec![
                0b111_00_001, // Header bits 7→0: |111|00|001| = value:7, type:TINY, delta_bytes:1
                0b0000_0010,  // Delta: 2
            ],
        ),
        (
            crate::Delta(256),
            RSIndexResult::numeric(0.0),
            3,
            vec![
                0b000_00_010, // Header bits 7→0: |000|00|010| = value:0, type:TINY, delta_bytes:2
                0b0000_0000,  // Delta byte 0 (LSB)
                0b0000_0001,  // Delta byte 1 (MSB) → 256 = 0x0100
            ],
        ),
    ];

    for input in inputs {
        let mut buf = Cursor::new(Vec::new());

        let (delta, record, expected_bytes_written, expected_buffer) = input;

        let bytes_written =
            Numeric::encode(&mut buf, delta, &record).expect("to encode numeric record");

        assert_eq!(bytes_written, expected_bytes_written);
        assert_eq!(buf.into_inner(), expected_buffer);
    }
}
