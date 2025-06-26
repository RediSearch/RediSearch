/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, numeric::Numeric};
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

    let bytes_written =
        Numeric::encode(&mut buf, Delta(delta), &record).expect("to encode numeric record");

    let buf = buf.into_inner();
    assert_eq!(
        bytes_written, expected_bytes_written,
        "failed for value: {}",
        value
    );
    assert_eq!(buf, expected_buffer, "failed for value: {}", value);

    let prev_doc_id = u64::MAX - (delta as u64);
    let DecoderResult::Record(record_decoded) = Numeric
        .decode(buf.as_slice(), prev_doc_id)
        .expect("to decode numeric record")
        .expect("to read a record from the buffer")
    else {
        panic!("Record was filtered out incorrectly")
    };

    assert_eq!(record_decoded, record, "failed for value: {}", value);
}

proptest! {
    #[test]
    fn numeric_encode_decode(
        delta in 0u64..72_057_594_037_927_935u64,
        value in f64::MIN..f64::MAX,
    ) {
        let mut buf = Cursor::new(Vec::new());
        let record = RSIndexResult::numeric(u64::MAX, value);

        let _bytes_written =
            Numeric::encode(&mut buf, Delta(delta as _), &record).expect("to encode numeric record");

        let buf = buf.into_inner();

        let prev_doc_id = u64::MAX - delta;

        let DecoderResult::Record(record_decoded) = Numeric
            .decode(buf.as_slice(), prev_doc_id)
            .expect("to decode numeric record")
            .expect("to read a record from the buffer")
        else {
            panic!("Record was filtered out incorrectly")
        };

        assert_eq!(record_decoded, record, "failed for value: {}", value);
    }
}
