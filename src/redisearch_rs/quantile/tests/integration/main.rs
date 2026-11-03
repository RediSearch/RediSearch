/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use quantile::QuantStream;
use std::num::NonZeroUsize;

#[rstest::rstest]
#[case::single(&[], 1, 128, [0., 63., 115., 127., 127.])]
#[case::partial(&[], 7, 503, [0., 251., 450., 493., 502.])]
#[case::full(&[], 500, 2000, [0., 992., 1814., 1970., 1999.])]
#[case::targeted_single(&[0.5, 0.9, 0.99], 1, 128, [4., 66., 120., 127., 127.])]
#[case::targeted_partial(&[0.5, 0.9, 0.99], 7, 503, [9., 251., 457., 502., 502.])]
#[case::targeted_full(&[0.5, 0.9, 0.99], 500, 2000, [49., 1046., 1845., 1999., 1999.])]
fn streaming_quantiles(
    #[case] targets: &[f64],
    #[case] capacity: usize,
    #[case] count: i32,
    #[case] expected: [f64; 5],
) {
    let mut stream = QuantStream::new(targets, NonZeroUsize::new(capacity).unwrap());
    assert!(stream.query(0.5).is_nan());
    for i in 0..count {
        stream.insert(f64::from((i * 997) % count));
    }
    for (q, expected) in [0.0, 0.5, 0.9, 0.99, 1.0].into_iter().zip(expected) {
        assert_eq!(stream.query(q), expected);
        assert_eq!(stream.query(q), expected);
    }
    assert_eq!(stream.count(), count as usize);
}

#[test]
fn insert_after_query() {
    let mut stream = QuantStream::new(&[0.5, 0.9], NonZeroUsize::new(4).unwrap());
    for value in [4., 1., 9.] {
        stream.insert(value);
    }
    assert_eq!(stream.count(), 0);
    assert_eq!(stream.query(0.5), 4.);
    assert_eq!(stream.count(), 3);
    for value in [8., 2., 7., 3.] {
        stream.insert(value);
    }
    assert_eq!(stream.count(), 7);
    for (q, expected) in [(0., 1.), (0.5, 7.), (1., 9.)] {
        assert_eq!(stream.query(q), expected);
    }
}

#[test]
fn nan_observations() {
    let mut stream = QuantStream::new(&[], NonZeroUsize::new(2).unwrap());
    stream.insert(f64::NAN);
    assert!(stream.query(0.5).is_nan());
    for value in [7.0, f64::NAN, 3.0, f64::NAN] {
        stream.insert(value);
    }
    assert_eq!(stream.count(), 2);
    assert_eq!(stream.query(0.5), 3.0);
    assert_eq!(stream.query(1.0), 7.0);
}

#[test]
fn signed_zero() {
    for value in [-0.0, 0.0] {
        let mut stream = QuantStream::new(&[], NonZeroUsize::new(1).unwrap());
        stream.insert(value);
        assert_eq!(stream.query(0.5).to_bits(), 0.0_f64.to_bits());
    }
}

#[test]
fn infinities_and_query_bounds() {
    let mut stream = QuantStream::new(&[], NonZeroUsize::new(8).unwrap());
    for value in [f64::INFINITY, 2.0, f64::NEG_INFINITY, -5.0] {
        stream.insert(value);
    }
    for (name, q, expected) in [
        ("below", -1.0, f64::NEG_INFINITY),
        ("median", 0.5, -5.0),
        ("above", 2.0, f64::INFINITY),
        ("nan", f64::NAN, f64::INFINITY),
    ] {
        assert_eq!(stream.query(q), expected, "{name}");
    }
}

// Proptest uses OS facilities unavailable under Miri.
#[cfg(not(miri))]
mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn exact_uncompressed_ranks(mut values in prop::collection::vec(-1000i32..1000, 1..80), percentile in 0u32..=100) {
            let mut stream = QuantStream::new(&[], NonZeroUsize::new(100).unwrap());
            for &value in &values {
                stream.insert(f64::from(value));
            }
            prop_assert_eq!(stream.count(), 0);
            values.sort_unstable();
            let rank = (f64::from(percentile) / 100.0 * values.len() as f64).ceil() as usize;
            let expected = values[rank.saturating_sub(1)];
            prop_assert_eq!(stream.query(f64::from(percentile) / 100.0), f64::from(expected));
            prop_assert_eq!(stream.count(), values.len());
        }
    }
}
