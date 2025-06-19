use std::io::Cursor;

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, numeric::Numeric};
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

#[test]
fn numeric_pos_int() {
    let inputs = [
        (
            1,
            16.0,
            3,
            vec![
                0b000_10_001, // Header bits 7→0: |000|10|001| = value_bytes:0, type:POS_INT, delta_bytes:1
                0b0000_0001,  // Delta: 1
                0b0001_0000,  // Value: 16
            ],
        ),
        (
            0,
            256.0,
            3,
            vec![
                0b001_10_000, // Header bits 7→0: |001|10|000| = value_bytes:1, type:POS_INT, delta_bytes:0
                0b0000_0000,  // Value 0 (LSB)
                0b0000_0001,  // Value 1 (MSB) → 256 = 0x0100
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

#[test]
fn numeric_neg_int() {
    let inputs = [
        (
            1,
            -16.0,
            3,
            vec![
                0b000_11_001, // Header bits 7→0: |000|11|001| = value_bytes:0, type:NEG_INT, delta_bytes:1
                0b0000_0001,  // Delta: 1
                0b0001_0000,  // Value: 16
            ],
        ),
        (
            0,
            -256.0,
            3,
            vec![
                0b001_11_000, // Header bits 7→0: |001|10|000| = value_bytes:1, type:NEG_INT, delta_bytes:0
                0b0000_0000,  // Value 0 (LSB)
                0b0000_0001,  // Value 1 (MSB) → 256 = 0x0100
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

#[test]
fn numeric_float() {
    let inputs = [
        // Positive small float
        (
            1,
            3.125,
            6,
            vec![
                0b000_01_001, // Header bits 7→0: |000|01|001| = ?:0, type:FLOAT, delta_bytes:1
                0b0000_0001,  // Delta: 1
                0,            // Value: 3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        // Negative small float
        (
            0,
            -3.125,
            5,
            vec![
                0b010_01_000, // Header bits 7→0: |010|01|000| = ?:0, type:FLOAT, delta_bytes:0
                0,            // Value: -3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        // Positive infinity
        (
            0,
            f64::INFINITY,
            1,
            vec![
                0b001_01_000, // Header bits 7→0: |001|01|000| = ?:0, type:FLOAT, delta_bytes:0
            ],
        ),
        // Negative infinity
        (
            0,
            f64::NEG_INFINITY,
            1,
            vec![
                0b011_01_000, // Header bits 7→0: |011|01|000| = ?:0, type:FLOAT, delta_bytes:0
            ],
        ),
        // Positive big float
        (
            0,
            3.124,
            9,
            vec![
                0b100_01_000, // Header bits 7→0: |100|01|000| = ?:0, type:FLOAT, delta_bytes:1
                203,          // Value: 3.124 in IEEE 754 format
                161,
                69,
                182,
                243,
                253,
                8,
                64,
            ],
        ),
        // Negative big float
        (
            0,
            -3.124,
            9,
            vec![
                0b110_01_000, // Header bits 7→0: |100|01|000| = ?:0, type:FLOAT, delta_bytes:1
                203,          // Value: -3.124 in IEEE 754 format
                161,
                69,
                182,
                243,
                253,
                8,
                64,
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
        assert_eq!(
            bytes_written, expected_bytes_written,
            "failed for value: {:?}",
            value
        );
        assert_eq!(buf, expected_buffer, "failed for value: {:?}", value);

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
