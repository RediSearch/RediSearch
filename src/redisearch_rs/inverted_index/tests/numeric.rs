/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, numeric::Numeric};
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use std::io::Cursor;

/// Tests for integer values between 0 and 7 which should use the [TINY header](super#tiny-type) format.
#[test]
fn numeric_tiny_int() {
    let inputs = [
        (
            0,
            2.0,
            1,
            vec![0b010_00_000], // TINY type, value: 2, no delta bytes
        ),
        (
            2,
            7.0,
            2,
            vec![
                0b111_00_001, // TINY type, value: 7, delta_bytes: 1
                2,            // Delta: 2
            ],
        ),
        (
            72_057_594_037_927_935,
            0.0,
            8,
            vec![
                0b000_00_111, // TINY type, value: 0, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
            ],
        ),
    ];

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

/// Tests for positive integers bigger than 7 which should use the [INT_POS header](super#pos-int-type) format.
#[test]
fn numeric_pos_int() {
    let inputs = [
        (
            1,
            16.0,
            3,
            vec![
                0b000_10_001, // INT_POS type, value_bytes: 0 (+1), delta_bytes: 1
                1,            // Delta: 1
                16,           // Value: 16
            ],
        ),
        (
            0,
            256.0,
            3,
            vec![
                0b001_10_000, // INT_POS type, value_bytes: 1 (+1), delta_bytes: 0
                0,            // Value 0 (LSB)
                1,            // Value 1 (MSB) → 256 = 0x0100
            ],
        ),
        (
            72_057_594_037_927_935,
            u64::MAX as _,
            16,
            vec![
                0b111_10_111, // INT_POS type, value_bytes: 7 (+1), delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                255, // Value
                255,
                255,
                255,
                255,
                255,
                255,
                255,
            ],
        ),
    ];

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

/// Tests for negative integers which should use the [INT_NEG header](super#neg-int-type) format.
#[test]
fn numeric_neg_int() {
    let inputs = [
        (
            1,
            -16.0,
            3,
            vec![
                0b000_11_001, // INT_NEG type, value_bytes: 0 (+1), delta_bytes: 1
                1,            // Delta: 1
                16,           // Value: 16
            ],
        ),
        (
            0,
            -256.0,
            3,
            vec![
                0b001_11_000, // INT_NEG type, value_bytes: 1 (+1), delta_bytes: 0
                0,            // Value 0 (LSB)
                1,            // Value 1 (MSB) → 256 = 0x0100
            ],
        ),
        (
            72_057_594_037_927_935,
            -(u64::MAX as f64),
            16,
            vec![
                0b111_11_111, // INT_NEG type, value_bytes: 7 (+1), delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                255, // Value
                255,
                255,
                255,
                255,
                255,
                255,
                255,
            ],
        ),
    ];

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

/// Tests for float values which should use the [FLOAT header](super#float-type) format.:w
#[test]
fn numeric_float() {
    let inputs = [
        (
            0,
            3.125,
            5,
            vec![
                0b000_01_000, // FLOAT type, !f64, !negative, !infinite, delta_bytes: 0
                0,            // Value: 3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        (
            72_057_594_037_927_935,
            3.125,
            12,
            vec![
                0b000_01_111, // FLOAT type, !f64, !negative, !infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                0, // Value: 3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        (
            0,
            -3.125,
            5,
            vec![
                0b010_01_000, // FLOAT type, !f64, negative, !infinite, delta_bytes: 0
                0,            // Value: 3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        (
            72_057_594_037_927_935,
            -3.125,
            12,
            vec![
                0b010_01_111, // FLOAT type, !f64, negative, !infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                0, // Value: 3.125 in IEEE 754 format
                0,
                72,
                64,
            ],
        ),
        (
            0,
            f64::INFINITY,
            1,
            vec![
                0b001_01_000, // FLOAT type, !f64, !negative, infinite, delta_bytes: 0
            ],
        ),
        (
            72_057_594_037_927_935,
            f64::INFINITY,
            8,
            vec![
                0b001_01_111, // FLOAT type, !f64, !negative, infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
            ],
        ),
        (
            0,
            f64::NEG_INFINITY,
            1,
            vec![
                0b011_01_000, // FLOAT type, !f64, negative, infinite, delta_bytes: 0
            ],
        ),
        (
            72_057_594_037_927_935,
            f64::NEG_INFINITY,
            8,
            vec![
                0b011_01_111, // FLOAT type, !f64, negative, infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
            ],
        ),
        (
            0,
            3.124,
            9,
            vec![
                0b100_01_000, // FLOAT type, f64, !negative, !infinite, delta_bytes: 0
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
        (
            72_057_594_037_927_935,
            3.124,
            16,
            vec![
                0b100_01_111, // FLOAT type, f64, !negative, !infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                203, // Value: 3.124 in IEEE 754 format
                161,
                69,
                182,
                243,
                253,
                8,
                64,
            ],
        ),
        (
            0,
            -3.124,
            9,
            vec![
                0b110_01_000, // FLOAT type, f64, negative, !infinite, delta_bytes: 0
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
        (
            72_057_594_037_927_935,
            -3.124,
            16,
            vec![
                0b110_01_111, // FLOAT type, f64, negative, !infinite, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                203, // Value: 3.124 in IEEE 754 format
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

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

fn test_numeric_encode_decode(
    delta: usize,
    value: f64,
    expected_bytes_written: usize,
    expected_buffer: Vec<u8>,
) {
    let mut buf = Cursor::new(Vec::new());
    let record = RSIndexResult::numeric(u64::MAX, value);

    let numeric = Numeric::new();
    let bytes_written = numeric
        .encode(&mut buf, Delta::new(delta), &record)
        .expect("to encode numeric record");

    assert_eq!(
        bytes_written, expected_bytes_written,
        "failed for value: {}",
        value
    );
    assert_eq!(
        buf.get_ref(),
        &expected_buffer,
        "failed for value: {}",
        value
    );

    buf.set_position(0);

    let prev_doc_id = u64::MAX - (delta as u64);
    let DecoderResult::Record(record_decoded) = numeric
        .decode(&mut buf, prev_doc_id)
        .expect("to decode numeric record")
    else {
        panic!("Record was filtered out incorrectly")
    };

    assert_eq!(record_decoded, record, "failed for value: {}", value);
}

#[test]
fn encode_f64_with_compression() {
    let mut buf = Cursor::new(Vec::new());
    let record = RSIndexResult::numeric(0, 3.124);

    let numeric = Numeric::new().with_float_compression();
    let _bytes_written = numeric
        .encode(&mut buf, Delta::new(0), &record)
        .expect("to encode numeric record");

    assert_eq!(
        buf.get_ref(),
        &[
            0b000_01_000, // FLOAT type, !f64, !negative, !infinite, delta_bytes: 0
            158,          // Value: 3.124 in IEEE 754 format after f32 conversion
            239,
            71,
            64,
        ],
        "should use a f32 instead"
    );

    buf.set_position(0);

    let DecoderResult::Record(record_decoded) = numeric
        .decode(&mut buf, 0)
        .expect("to decode numeric record")
    else {
        panic!("Record was filtered out incorrectly")
    };

    let diff = record_decoded.as_numeric().unwrap().0 - record.as_numeric().unwrap().0;
    let diff = diff.abs();

    assert!(diff < 0.01);
}

#[test]
fn test_empty_buffer() {
    let mut buffer = Cursor::new(Vec::new());
    let res = Numeric::new().decode(&mut buffer, 0);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
#[should_panic(expected = "numeric encoder will only be called for numeric records")]
fn encoding_non_numeric_record() {
    let mut buffer = Cursor::new(Vec::new());
    let record = RSIndexResult::virt(10);

    let _result = Numeric::new().encode(&mut buffer, Delta::new(0), &record);
}

#[test]
fn encoding_to_fixed_buffer() {
    let mut buffer = Cursor::new([0; 2]);
    let record = RSIndexResult::numeric(1, 100.0);

    let result = Numeric::new().encode(&mut buffer, Delta::new(1), &record);

    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn encoding_to_slow_writer() {
    use std::io::{Seek, Write};

    struct SlowWriter<W> {
        inner: W,
        call_count: usize,
    }

    impl<W> SlowWriter<W> {
        const BYTES_PER_WRITE: usize = 2;

        fn new(inner: W) -> Self {
            Self {
                inner,
                call_count: 0,
            }
        }
    }

    impl<W: Write> Write for SlowWriter<W> {
        fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
            self.call_count += 1;
            let mut written = 0;

            for buf in bufs {
                if written >= Self::BYTES_PER_WRITE {
                    break;
                }

                let remaining = Self::BYTES_PER_WRITE - written;
                let to_write = buf.len().min(remaining);

                self.inner.write_all(&buf[..to_write])?;
                written += to_write;
            }

            Ok(written)
        }

        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let to_write = buf.len().min(Self::BYTES_PER_WRITE);
            self.inner.write(&buf[..to_write])
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }

    impl<W> Seek for SlowWriter<W> {
        fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
            unimplemented!("nothing in this test should call this")
        }
    }

    let mut buffer = SlowWriter::new(Vec::new());
    let record = RSIndexResult::numeric(10, 3.124);

    let result = Numeric::new()
        .encode(&mut buffer, Delta::new(0), &record)
        .expect("to encode the complete record");

    assert_eq!(result, 9);
    assert_eq!(
        buffer.inner,
        [
            0b100_01_000, // FLOAT type, f64, !negative, !infinite, delta_bytes: 0
            203,          // Value: 3.124 in IEEE 754 format
            161,
            69,
            182,
            243,
            253,
            8,
            64,
        ]
    );
    assert_eq!(
        buffer.call_count, 5,
        "9 bytes needed to be written in chunks of 2"
    );
}

proptest! {
    #[test]
    fn numeric_encode_decode_integers(
        delta in 0u64..72_057_594_037_927_935u64,
        value in u64::MIN..u64::MAX,
    ) {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::numeric(u64::MAX, value as _);

        let numeric = Numeric::new();
        let _bytes_written =
            numeric.encode(&mut buf, Delta::new(delta as _), &record).expect("to encode numeric record");

        buf.set_position(0);
        let prev_doc_id = u64::MAX - delta;

        let DecoderResult::Record(record_decoded) = numeric
            .decode(&mut buf, prev_doc_id)
            .expect("to decode numeric record")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(record_decoded, record, "failed for value: {}", value);
    }

    #[test]
    fn numeric_encode_decode_floats(
        delta in 0u64..72_057_594_037_927_935u64,
        value in f64::MIN..f64::MAX,
    ) {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::numeric(u64::MAX, value);

        let numeric = Numeric::new();
        let _bytes_written =
            numeric.encode(&mut buf, Delta::new(delta as _), &record).expect("to encode numeric record");

        buf.set_position(0);
        let prev_doc_id = u64::MAX - delta;

        let DecoderResult::Record(record_decoded) = numeric
            .decode(&mut buf, prev_doc_id)
            .expect("to decode numeric record")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(record_decoded, record, "failed for value: {}", value);
    }
}
