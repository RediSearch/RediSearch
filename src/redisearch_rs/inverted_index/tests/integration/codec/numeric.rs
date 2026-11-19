/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_result::RSIndexResult;
use inverted_index::{
    Decoder, Encoder, IdDelta,
    numeric::{Numeric, NumericDelta, NumericEncoder, NumericFloatCompression},
};
use pretty_assertions::assert_eq;
use std::io::Cursor;

/// Encode `value` with `E` and decode it again, returning what a reader sees.
fn round_trip<E: Encoder<Delta = NumericDelta> + Decoder>(value: f64) -> f64 {
    let mut buf = Cursor::new(Vec::new());
    let record = RSIndexResult::build_numeric(value).doc_id(1).build();
    E::encode(&mut buf, NumericDelta::from_u64(0).unwrap(), &record).expect("to encode");

    let buf = buf.into_inner();
    let mut buf = Cursor::new(buf.as_ref());
    let decoded = E::decode_new(&mut buf, 1).expect("to decode");

    decoded.as_numeric().expect("a numeric record")
}

/// `stored_value` must predict exactly what an encode/decode round trip produces, and
/// the two are separate code paths — see `Value::to_f64`.
fn assert_stored_value_matches_round_trip(value: f64) {
    for (name, predicted, actual) in [
        (
            "Numeric",
            Numeric::stored_value(value),
            round_trip::<Numeric>(value),
        ),
        (
            "NumericFloatCompression",
            NumericFloatCompression::stored_value(value),
            round_trip::<NumericFloatCompression>(value),
        ),
    ] {
        assert_eq!(
            predicted.to_bits(),
            actual.to_bits(),
            "{name}: stored_value({value}) predicted {predicted} but a round trip yields {actual}"
        );
        // Storing a stored value must be a no-op, or repeated preparation could keep
        // shifting statistics.
        let twice = match name {
            "Numeric" => Numeric::stored_value(predicted),
            _ => NumericFloatCompression::stored_value(predicted),
        };
        assert_eq!(
            twice.to_bits(),
            predicted.to_bits(),
            "{name}: stored_value is not idempotent for {value}"
        );
    }
}

#[test]
fn stored_value_matches_encode_decode_for_edge_cases() {
    for value in [
        0.0,
        -0.0, // stored as a tiny int, so it comes back as +0.0
        1.0,
        7.0,
        8.0,
        -1.0,
        -8.0,
        0.5,
        -0.5,
        3.124,                   // compressible: within the 0.01 threshold
        100.500001,              // collapses onto 100.5 as an f32
        1e-9, // NOT compressible: relative error is tiny, absolute is smaller still
        9_007_199_254_740_993.0, // 2^53 + 1
        4_503_599_627_370_496.0, // a 52-bit geohash-shaped value
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        assert_stored_value_matches_round_trip(value);
    }
}

/// The two write entry points must be interchangeable down to the bytes, or an index
/// written through the prepared path would decode differently.
fn assert_encode_prepared_matches_encode(value: f64, delta: u64) {
    fn compare<E: Encoder<Delta = NumericDelta> + NumericEncoder>(value: f64, delta: u64) {
        let record = RSIndexResult::build_numeric(value).doc_id(1).build();

        let mut from_record = Cursor::new(Vec::new());
        let record_bytes = E::encode(
            &mut from_record,
            NumericDelta::from_u64(delta).unwrap(),
            &record,
        )
        .expect("to encode");

        let mut from_prepared = Cursor::new(Vec::new());
        let prepared_bytes = E::encode_prepared(
            &mut from_prepared,
            NumericDelta::from_u64(delta).unwrap(),
            E::prepare(value),
        )
        .expect("to encode");

        assert_eq!(
            from_record.into_inner(),
            from_prepared.into_inner(),
            "encode and encode_prepared disagree for value {value}, delta {delta}"
        );
        assert_eq!(record_bytes, prepared_bytes);
    }

    compare::<Numeric>(value, delta);
    compare::<NumericFloatCompression>(value, delta);
}

#[test]
fn encode_prepared_matches_encode_for_edge_cases() {
    for value in [
        0.0,
        -0.0,
        7.0,
        -8.0,
        3.124,
        100.500001,
        1e-9,
        9_007_199_254_740_993.0,
        f64::MAX,
        f64::MIN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        for delta in [0, 1, 255, 72_057_594_037_927_935] {
            assert_encode_prepared_matches_encode(value, delta);
        }
    }
}

#[test]
fn stored_value_maps_negative_zero_to_positive_zero() {
    // Both encoders take the tiny-integer path for zero, so the sign is dropped.
    for stored in [
        Numeric::stored_value(-0.0),
        NumericFloatCompression::stored_value(-0.0),
    ] {
        assert!(
            stored.is_sign_positive() && stored == 0.0,
            "-0.0 should be stored as +0.0, got {stored}"
        );
    }
}

/// Magnitudes that round to zero collapse onto the *canonical* zero, whatever their
/// sign.
///
/// Compression stores an f32 whenever the loss is under its threshold, and for a
/// subnormal magnitude that f32 is zero. Keeping the sign flag there would produce a
/// `-0.0` stored value: numerically equal to `+0.0`, so no filter could separate the
/// two, yet distinct enough to fool a consumer that keys values by bit pattern.
#[test]
fn compression_collapse_to_zero_is_canonical() {
    for input in [
        5e-324,
        -5e-324,
        f64::MIN_POSITIVE,
        -f64::MIN_POSITIVE,
        1e-60,
        -1e-60,
    ] {
        let stored = NumericFloatCompression::stored_value(input);
        assert_eq!(stored, 0.0, "{input} should collapse onto zero");
        assert!(
            stored.is_sign_positive(),
            "{input} collapsed onto a negative zero"
        );

        // Without compression such a magnitude is kept exactly, so no collapse and no
        // canonicalization question.
        assert_eq!(Numeric::stored_value(input).to_bits(), input.to_bits());
    }
}

#[test]
fn stored_value_of_nan_stays_nan() {
    // NaN survives as NaN (the payload may differ), so it can never move a bound:
    // every comparison against it is false.
    assert!(Numeric::stored_value(f64::NAN).is_nan());
    assert!(NumericFloatCompression::stored_value(f64::NAN).is_nan());
}

#[test]
fn compression_collapses_neighbouring_values_to_one_stored_value() {
    // The MOD-16877 shape: distinct f64s inside one f32 bucket collapse to a single
    // stored value under compression.
    let values: Vec<f64> = (0..8).map(|i| 100.5 + i as f64 * 1e-7).collect();
    let stored: Vec<f64> = values
        .iter()
        .map(|&v| NumericFloatCompression::stored_value(v))
        .collect();

    assert!(
        stored.windows(2).all(|w| w[0] == w[1]),
        "expected one stored value, got {stored:?}"
    );
    assert_eq!(stored[0], 100.5);

    // Without compression they stay distinct.
    let exact: Vec<f64> = values.iter().map(|&v| Numeric::stored_value(v)).collect();
    assert_eq!(exact, values);
}

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
        (
            0,
            0.0,
            1,
            vec![0b000_00_000], // TINY type, value: 0, no delta bytes
        ),
        (
            0,
            -0.0,
            1,
            vec![0b000_00_000], // TINY type, value: 0, no delta bytes
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
            0,
            -1.0,
            2,
            vec![
                0b000_11_000, // INT_NEG type, value_bytes: 0 (+1), delta_bytes: 0
                1,            // Value: 1
            ],
        ),
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

/// Exact in an `f32`, but needs more decimal places than a decimal can hold.
const F32_EXACT: f64 = 0.0009765625;

/// Has no short decimal form, so only an `f64` holds it.
const F64_ONLY: f64 = 0.1 + 0.2;

/// Tests for float values which should use the [FLOAT header](super#float-type) format.
#[test]
fn numeric_float() {
    let inputs = [
        (
            0,
            F32_EXACT,
            5,
            vec![
                0b000_01_000, // FLOAT type, !f64, !negative, !infinite, delta_bytes: 0
                0,            // Value: 2^-10 in IEEE 754 format
                0,
                128,
                58,
            ],
        ),
        (
            72_057_594_037_927_935,
            F32_EXACT,
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
                0, // Value: 2^-10 in IEEE 754 format
                0,
                128,
                58,
            ],
        ),
        (
            0,
            -F32_EXACT,
            5,
            vec![
                0b010_01_000, // FLOAT type, !f64, negative, !infinite, delta_bytes: 0
                0,            // Value: 2^-10 in IEEE 754 format
                0,
                128,
                58,
            ],
        ),
        (
            72_057_594_037_927_935,
            -F32_EXACT,
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
                0, // Value: 2^-10 in IEEE 754 format
                0,
                128,
                58,
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
            F64_ONLY,
            9,
            vec![
                0b100_01_000, // FLOAT type, f64, !negative, !infinite, delta_bytes: 0
                52,           // Value: 0.1 + 0.2 in IEEE 754 format
                51,
                51,
                51,
                51,
                51,
                211,
                63,
            ],
        ),
        (
            72_057_594_037_927_935,
            F64_ONLY,
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
                52, // Value: 0.1 + 0.2 in IEEE 754 format
                51,
                51,
                51,
                51,
                51,
                211,
                63,
            ],
        ),
        (
            0,
            -F64_ONLY,
            9,
            vec![
                0b110_01_000, // FLOAT type, f64, negative, !infinite, delta_bytes: 0
                52,           // Value: 0.1 + 0.2 in IEEE 754 format
                51,
                51,
                51,
                51,
                51,
                211,
                63,
            ],
        ),
        (
            72_057_594_037_927_935,
            -F64_ONLY,
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
                52, // Value: 0.1 + 0.2 in IEEE 754 format
                51,
                51,
                51,
                51,
                51,
                211,
                63,
            ],
        ),
    ];

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

/// Tests for short decimals, which should use the [decimal value](super#decimal-value) format.
#[test]
fn numeric_decimal() {
    let inputs = [
        (
            0,
            0.3,
            3,
            vec![
                0b101_01_000, // FLOAT type, positive decimal, delta_bytes: 0
                0b00_001_000, // scale: 1, mantissa bytes: 0 (+1)
                3,            // Mantissa: 3
            ],
        ),
        (
            0,
            // Exact in an f32 too, but the decimal is shorter.
            0.5,
            3,
            vec![
                0b101_01_000, // FLOAT type, positive decimal, delta_bytes: 0
                0b00_001_000, // scale: 1, mantissa bytes: 0 (+1)
                5,            // Mantissa: 5
            ],
        ),
        (
            1,
            -17.05,
            5,
            vec![
                0b111_01_001, // FLOAT type, negative decimal, delta_bytes: 1
                1,            // Delta
                0b00_010_001, // scale: 2, mantissa bytes: 1 (+1)
                169,          // Mantissa: 1705
                6,
            ],
        ),
        (
            72_057_594_037_927_935,
            -73.993896,
            13,
            vec![
                0b111_01_111, // FLOAT type, negative decimal, delta_bytes: 7
                255,          // Delta
                255,
                255,
                255,
                255,
                255,
                255,
                0b00_110_011, // scale: 6, mantissa bytes: 3 (+1)
                168,          // Mantissa: 73993896
                14,
                105,
                4,
            ],
        ),
    ];

    for (delta, value, expected_bytes_written, expected_buffer) in inputs {
        test_numeric_encode_decode(delta, value, expected_bytes_written, expected_buffer);
    }
}

/// A decimal that is no shorter than the `f64` keeps the `f64`, which decodes without a
/// division.
#[test]
fn decimal_not_shorter_keeps_f64() {
    // A 7-byte mantissa plus the info byte matches the 8 bytes of an f64.
    let value: f64 = 1_234_567_891_234_567.0 / 1e7;
    assert_eq!(
        (value * 1e7).round() / 1e7,
        value,
        "the value must have an exact decimal form for this test to mean anything"
    );

    let mut buf = Cursor::new(Vec::new());
    let record = RSIndexResult::build_numeric(value).build();
    let written = Numeric::encode(&mut buf, NumericDelta::from_u64(0).unwrap(), &record)
        .expect("to encode numeric record");

    assert_eq!(written, 9);
    assert_eq!(buf.get_ref()[0], 0b100_01_000, "should stay an f64");
}

/// Whether `header` marks a [decimal value](super#decimal-value), of either sign.
const fn is_decimal_header(header: u8) -> bool {
    header & 0b101_11_000 == 0b101_01_000
}

/// Exact powers of ten for building expected decimals. `f64::powi` is not required to be
/// exact, and Miri deliberately perturbs it, so expected values must not use it.
const POWERS_OF_TEN: [f64; 8] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7];

/// A positive [decimal](super#decimal-value) record assembled byte by byte, so the decoder
/// can be fed every delta and mantissa width the format describes, including mantissas wider
/// than an encoder writes. Returns the record, its delta and the value a reader must see.
fn handmade_decimal(delta_bytes: usize, mantissa_bytes: usize) -> (Vec<u8>, u64, f64) {
    const SCALE: u8 = 7;
    // Distinct non-zero bytes, so a field read from the wrong offset decodes wrong.
    const DELTA: [u8; 8] = [0x9b, 0x8a, 0x79, 0x68, 0x57, 0x46, 0x35, 0x24];
    const MANTISSA: [u8; 8] = [0x21, 0x43, 0x65, 0x87, 0xa9, 0xcb, 0xed, 0x8f];

    let mut record = vec![0b101_01_000 | delta_bytes as u8];
    record.extend_from_slice(&DELTA[..delta_bytes]);
    record.push((SCALE << 3) | (mantissa_bytes as u8 - 1));
    record.extend_from_slice(&MANTISSA[..mantissa_bytes]);

    let mut delta = [0; 8];
    delta[..delta_bytes].copy_from_slice(&DELTA[..delta_bytes]);
    let mut mantissa = [0; 8];
    mantissa[..mantissa_bytes].copy_from_slice(&MANTISSA[..mantissa_bytes]);
    let value = u64::from_le_bytes(mantissa) as f64 / POWERS_OF_TEN[SCALE as usize];

    (record, u64::from_le_bytes(delta), value)
}

/// A decimal decodes the same whether it ends the buffer or more bytes follow it, which
/// decides whether the decoder can read it in one fixed-size load.
#[test]
fn decimal_decodes_the_same_with_and_without_trailing_bytes() {
    let base = 1;
    for delta_bytes in 0..=7 {
        for mantissa_bytes in 1..=8 {
            let (record, delta, value) = handmade_decimal(delta_bytes, mantissa_bytes);
            // Every length from the record alone to enough for the widest load. The trailing
            // bytes are all ones, so any of them leaking into a field changes the result.
            for trailing in 0..=16 {
                let mut buf = record.clone();
                buf.resize(record.len() + trailing, 0xff);
                let mut cursor = Cursor::new(buf.as_slice());

                let decoded =
                    Numeric::decode_new(&mut cursor, base).expect("to decode numeric record");

                let case = format!(
                    "delta bytes: {delta_bytes}, mantissa bytes: {mantissa_bytes}, \
                     trailing bytes: {trailing}"
                );
                assert_eq!(decoded.doc_id, base + delta, "{case}");
                assert_eq!(
                    decoded.as_numeric().unwrap().to_bits(),
                    value.to_bits(),
                    "{case}"
                );
                assert_eq!(cursor.position(), record.len() as u64, "{case}");
            }
        }
    }
}

/// Every record of a block of decimals decodes back, both the early ones that have plenty of
/// bytes after them and the last few that sit close to the end of the block.
#[test]
fn decimal_block_decodes_every_record() {
    // Deltas of every byte width, widest first, so that the shortest records come last.
    let records = [
        (0x12_3456_789a_bcde, 1234567.891234),
        (0x1234_5678_9abc, -123456.78901),
        (0x12_3456_789a, -73.993896),
        (0x1234_5678, 1234.5678),
        (0x12_3456, -17.05),
        (0x1234, 0.3),
        (0x12, 12.34),
        (0, -0.5),
    ];

    let mut buf = Cursor::new(Vec::new());
    let mut starts = Vec::new();
    for (delta, value) in records {
        starts.push(buf.position() as usize);
        let record = RSIndexResult::build_numeric(value).build();
        Numeric::encode(&mut buf, NumericDelta::from_u64(delta).unwrap(), &record)
            .expect("to encode numeric record");
    }
    let buf = buf.into_inner();

    for (&start, (_, value)) in starts.iter().zip(records) {
        assert!(
            is_decimal_header(buf[start]),
            "{value} must be stored as a decimal for this test to mean anything"
        );
    }
    // The decoder loads 16 bytes at once when it can; the block must have records on both
    // sides of that limit for this test to mean anything.
    let far_from_end = starts
        .iter()
        .filter(|&&start| buf.len() - start > 16)
        .count();
    assert!(0 < far_from_end && far_from_end < starts.len());

    let mut cursor = Cursor::new(buf.as_slice());
    let mut doc_id = 0;
    for (delta, value) in records {
        let decoded = Numeric::decode_new(&mut cursor, doc_id).expect("to decode numeric record");
        doc_id += delta;

        assert_eq!(decoded.doc_id, doc_id, "failed for value: {value}");
        assert_eq!(
            decoded.as_numeric().unwrap().to_bits(),
            value.to_bits(),
            "failed for value: {value}"
        );
    }
    assert_eq!(cursor.position(), buf.len() as u64);
}

fn test_numeric_encode_decode(
    delta: u64,
    value: f64,
    expected_bytes_written: usize,
    expected_buffer: Vec<u8>,
) {
    let mut buf = Cursor::new(Vec::new());
    let record = RSIndexResult::build_numeric(value).doc_id(u64::MAX).build();

    let bytes_written = Numeric::encode(&mut buf, NumericDelta::from_u64(delta).unwrap(), &record)
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

    let prev_doc_id = u64::MAX - delta;
    let buf = buf.into_inner();
    let mut buf = Cursor::new(buf.as_ref());

    let record_decoded =
        Numeric::decode_new(&mut buf, prev_doc_id).expect("to decode numeric record");

    assert_eq!(record_decoded, record, "failed for value: {}", value);
}

#[test]
fn encode_f64_with_compression() {
    let mut buf = Cursor::new(Vec::new());
    // No short decimal form, so the lossy f32 is the smallest representation.
    let record = RSIndexResult::build_numeric(std::f64::consts::PI).build();

    let _bytes_written =
        NumericFloatCompression::encode(&mut buf, NumericDelta::from_u64(0).unwrap(), &record)
            .expect("to encode numeric record");

    assert_eq!(
        buf.get_ref(),
        &[
            0b000_01_000, // FLOAT type, !f64, !negative, !infinite, delta_bytes: 0
            219,          // Value: pi in IEEE 754 format after f32 conversion
            15,
            73,
            64,
        ],
        "should use a f32 instead"
    );

    buf.set_position(0);

    let buf = buf.into_inner();
    let mut buf = Cursor::new(buf.as_ref());

    let record_decoded = Numeric::decode_new(&mut buf, 0).expect("to decode numeric record");

    let diff = record_decoded.as_numeric().unwrap() - record.as_numeric().unwrap();
    let diff = diff.abs();

    assert!(diff < 0.01);
}

#[test]
fn test_empty_buffer() {
    let buffer = Vec::new();
    let mut buffer = Cursor::new(buffer.as_ref());
    let res = Numeric::decode_new(&mut buffer, 0);

    assert_eq!(res.is_err(), true);
    let kind = res.unwrap_err().kind();
    assert_eq!(kind, std::io::ErrorKind::UnexpectedEof);
}

#[test]
#[should_panic(expected = "numeric encoder will only be called for numeric records")]
fn encoding_non_numeric_record() {
    let mut buffer = Cursor::new(Vec::new());
    let record = RSIndexResult::build_virt().doc_id(10).build();

    let _result = Numeric::encode(&mut buffer, NumericDelta::from_u64(0).unwrap(), &record);
}

#[test]
fn encoding_to_fixed_buffer() {
    let mut buffer = Cursor::new([0; 2]);
    let record = RSIndexResult::build_numeric(100.0).doc_id(1).build();

    let result = Numeric::encode(&mut buffer, NumericDelta::from_u64(1).unwrap(), &record);

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
    let record = RSIndexResult::build_numeric(F64_ONLY).doc_id(10).build();

    let result = Numeric::encode(&mut buffer, NumericDelta::from_u64(0).unwrap(), &record)
        .expect("to encode the complete record");

    assert_eq!(result, 9);
    assert_eq!(
        buffer.inner,
        [
            0b100_01_000, // FLOAT type, f64, !negative, !infinite, delta_bytes: 0
            52,           // Value: 0.1 + 0.2 in IEEE 754 format
            51,
            51,
            51,
            51,
            51,
            211,
            63,
        ]
    );
    assert_eq!(
        buffer.call_count, 5,
        "9 bytes needed to be written in chunks of 2"
    );
}

#[test]
fn numeric_delta_overflow() {
    let delta = NumericDelta::from_u64(u32::MAX as u64 + 1)
        .expect("Delta still fits in numeric encoder")
        .inner();

    assert_eq!(delta, u32::MAX as u64 + 1);

    let delta = NumericDelta::from_u64((1 << 56) - 1)
        .expect("Delta still fits in numeric encoder")
        .inner();

    assert_eq!(delta, (1 << 56) - 1);

    let delta = NumericDelta::from_u64(1 << 56);

    assert_eq!(
        delta, None,
        "Delta will overflow, so should request a new block for encoding"
    );
}
mod property_based {
    #![cfg(not(miri))]
    use super::*;
    use pretty_assertions::assert_eq;
    use proptest::strategy::Strategy;
    use std::ops::RangeInclusive;

    /// Integers below `limit` whose byte width is spread evenly over `widths`, where a
    /// uniform draw would almost always be as wide as `limit` allows. Width 0 is zero.
    fn of_any_width(widths: RangeInclusive<u32>, limit: u64) -> impl Strategy<Value = u64> {
        widths.prop_flat_map(move |bytes| match bytes {
            0 => 0..1,
            _ => 1u64 << (8 * (bytes - 1))..(1u64 << (8 * bytes)).min(limit),
        })
    }

    proptest::proptest! {
        #[test]
        fn numeric_encode_decode_integers(
            delta in 0u64..72_057_594_037_927_935u64,
            value in u64::MIN..u64::MAX,
        ) {
            let mut buf = Cursor::new(Vec::new());
            let record = RSIndexResult::build_numeric(value as _).doc_id(u64::MAX).build();

            let _bytes_written =
                Numeric::encode(&mut buf, NumericDelta::from_u64(delta).unwrap(), &record).expect("to encode numeric record");

            buf.set_position(0);
            let prev_doc_id = u64::MAX - delta;
            let buf = buf.into_inner();
            let mut buf = Cursor::new(buf.as_ref());

            let record_decoded = Numeric::decode_new(&mut buf, prev_doc_id)
                .expect("to decode numeric record");

            assert_eq!(record_decoded, record, "failed for value: {}", value);
        }

        #[test]
        fn numeric_encode_decode_floats(
            delta in 0u64..72_057_594_037_927_935u64,
            value in f64::MIN..f64::MAX,
        ) {
            let mut buf = Cursor::new(Vec::new());
            let record = RSIndexResult::build_numeric(value).doc_id(u64::MAX).build();

            let _bytes_written =
                Numeric::encode(&mut buf, NumericDelta::from_u64(delta).unwrap(), &record).expect("to encode numeric record");

            buf.set_position(0);
            let prev_doc_id = u64::MAX - delta;
            let buf = buf.into_inner();
            let mut buf = Cursor::new(buf.as_ref());

            let record_decoded = Numeric::decode_new(&mut buf, prev_doc_id)
                .expect("to decode numeric record");

            assert_eq!(record_decoded, record, "failed for value: {}", value);
        }

        /// Every value with a short decimal form survives the round trip bit for bit,
        /// with or without bytes after it, and never takes more room than the float it
        /// replaces.
        #[test]
        fn numeric_encode_decode_decimals(
            delta in of_any_width(0..=7, 1 << 56),
            mantissa in of_any_width(1..=7, 1 << 53),
            scale in 1u32..=7,
            negative in proptest::bool::ANY,
        ) {
            let magnitude = mantissa as f64 / POWERS_OF_TEN[scale as usize];
            let value = if negative { -magnitude } else { magnitude };

            let mut buf = Cursor::new(Vec::new());
            let record = RSIndexResult::build_numeric(value).doc_id(u64::MAX).build();
            let written = Numeric::encode(&mut buf, NumericDelta::from_u64(delta).unwrap(), &record)
                .expect("to encode numeric record");
            let encoded = buf.into_inner();

            let delta_bytes = (encoded[0] & 0b111) as usize;
            let f32_exact = (magnitude as f32) as f64 == magnitude;
            let float_bytes = if value.fract() == 0.0 { 8 } else if f32_exact { 4 } else { 8 };
            assert!(written <= 1 + delta_bytes + float_bytes);

            // Below this the mantissa at the finest scale stays far enough under 2^53 that
            // rounding cannot hide the decimal form, so the encoder must find it.
            if value.fract() != 0.0 && magnitude < 1e8 {
                let (mut shortest, mut shortest_scale) = (mantissa, scale);
                while shortest_scale > 1 && shortest.is_multiple_of(10) {
                    shortest /= 10;
                    shortest_scale -= 1;
                }
                let decimal_bytes =
                    1 + (u64::BITS - shortest.leading_zeros()).div_ceil(8) as usize;
                let shorter = decimal_bytes < float_bytes;

                assert_eq!(is_decimal_header(encoded[0]), shorter, "for value: {value}");
                if shorter {
                    assert_eq!(written, 1 + delta_bytes + decimal_bytes, "for value: {value}");
                }
            }

            let prev_doc_id = u64::MAX - delta;
            let mut padded = encoded.clone();
            padded.resize(encoded.len() + 16, 0xff);
            for bytes in [&encoded, &padded] {
                let mut cursor = Cursor::new(bytes.as_slice());
                let decoded = Numeric::decode_new(&mut cursor, prev_doc_id)
                    .expect("to decode numeric record");

                assert_eq!(decoded.doc_id, u64::MAX);
                assert_eq!(decoded.as_numeric().unwrap().to_bits(), value.to_bits());
                assert_eq!(cursor.position(), written as u64);
            }
            assert_stored_value_matches_round_trip(value);
        }

        /// Keeps `Value::to_f64` (which backs `stored_value`) in agreement with
        /// `decode`, which reconstructs values from the bytes independently.
        #[test]
        fn numeric_stored_value_matches_encode_decode(value in f64::MIN..f64::MAX) {
            assert_stored_value_matches_round_trip(value);
        }

        /// The prepared path must be byte-identical to the record path.
        #[test]
        fn numeric_encode_prepared_matches_encode(
            value in f64::MIN..f64::MAX,
            delta in 0u64..72_057_594_037_927_935u64,
        ) {
            assert_encode_prepared_matches_encode(value, delta);
        }

        /// Re-preparing a stored value must be a no-op: the tree prepares on the way
        /// in, and split redistribution prepares values it read back out again.
        #[test]
        fn numeric_stored_value_is_idempotent(value in f64::MIN..f64::MAX) {
            let once = Numeric::stored_value(value);
            assert_eq!(Numeric::stored_value(once).to_bits(), once.to_bits());

            let once = NumericFloatCompression::stored_value(value);
            assert_eq!(
                NumericFloatCompression::stored_value(once).to_bits(),
                once.to_bits(),
            );
        }
    }
}
