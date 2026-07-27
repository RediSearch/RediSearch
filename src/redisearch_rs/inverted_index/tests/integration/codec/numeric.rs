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

/// `stored_value` must predict exactly what an encode/decode round trip produces —
/// that is the whole point of the trait, and the two implementations are separate
/// code (`decode` rebuilds the number straight from the bytes for speed).
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
        // Storing a stored value must be a no-op, or quantizing on the way in would
        // not be idempotent and repeated GC passes could keep shifting statistics.
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

/// The two write entry points must be interchangeable down to the bytes: whichever
/// one a caller reaches for, the block content has to be identical, or an index
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
    // Statistics computed over stored values must treat the two as one value.
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

#[test]
fn stored_value_of_nan_stays_nan() {
    // NaN survives as NaN (the payload may differ), so it can never move a bound:
    // every comparison against it is false.
    assert!(Numeric::stored_value(f64::NAN).is_nan());
    assert!(NumericFloatCompression::stored_value(f64::NAN).is_nan());
}

#[test]
fn compression_collapses_neighbouring_values_to_one_stored_value() {
    // The MOD-16877 shape: distinct f64s inside one f32 bucket. With compression on
    // they are a single stored value, so anything derived from them (cardinality,
    // bounds) must see one value, not many.
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
    let record = RSIndexResult::build_numeric(3.124).build();

    let _bytes_written =
        NumericFloatCompression::encode(&mut buf, NumericDelta::from_u64(0).unwrap(), &record)
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
    let record = RSIndexResult::build_numeric(3.124).doc_id(10).build();

    let result = Numeric::encode(&mut buffer, NumericDelta::from_u64(0).unwrap(), &record)
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
