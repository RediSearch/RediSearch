/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::{BucketLevel, BucketsError, CompoundIndex, MAX_LEVELS, SchemaError};
use proptest::prelude::*;

use crate::util::{Index, tag_level};

#[test]
fn bucket_level_rejects_invalid_boundaries() {
    let cases: &[(&str, &[f64], BucketsError)] = &[
        (
            "no boundary",
            &[],
            BucketsError::TooFewBoundaries { got: 0 },
        ),
        (
            "one boundary",
            &[1.0],
            BucketsError::TooFewBoundaries { got: 1 },
        ),
        (
            "NaN",
            &[0.0, f64::NAN],
            BucketsError::NotFinite { index: 1 },
        ),
        (
            "infinity",
            &[f64::NEG_INFINITY, 0.0],
            BucketsError::NotFinite { index: 0 },
        ),
        (
            "repeated",
            &[0.0, 0.0],
            BucketsError::NotIncreasing { index: 1 },
        ),
        (
            "decreasing",
            &[0.0, 2.0, 1.0],
            BucketsError::NotIncreasing { index: 2 },
        ),
    ];
    for (name, boundaries, expected) in cases {
        assert_eq!(
            BucketLevel::new(boundaries.to_vec()),
            Err(expected.clone()),
            "{name}"
        );
    }
}

#[test]
fn values_outside_the_buckets_have_no_bucket() {
    let level = BucketLevel::new(vec![0.0, 10.0, 20.0]).unwrap();
    for (name, value) in [
        ("below the first boundary", -0.5),
        ("on the last boundary", 20.0),
        ("above the last boundary", 25.0),
        ("NaN", f64::NAN),
        ("+infinity", f64::INFINITY),
        ("-infinity", f64::NEG_INFINITY),
    ] {
        assert_eq!(level.bucket_of(value), None, "{name}");
    }
}

#[test]
fn negative_zero_falls_in_the_bucket_starting_at_zero() {
    let level = BucketLevel::new(vec![0.0, 10.0]).unwrap();
    assert_eq!(level.bucket_of(-0.0), Some(0));
}

/// Strictly increasing finite boundaries, between 2 and 6 of them.
fn boundaries() -> impl Strategy<Value = Vec<f64>> {
    prop::collection::btree_set(-1000i32..1000, 2..=6)
        .prop_map(|set| set.into_iter().map(f64::from).collect())
}

proptest! {
    // No failure file: Miri's isolation forbids the file system access it needs.
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 8 } else { 256 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn bucket_of_finds_the_half_open_bucket_holding_the_value(
        boundaries in boundaries(),
        value in -1100.0f64..1100.0,
    ) {
        let level = BucketLevel::new(boundaries.clone()).unwrap();
        let expected = boundaries
            .windows(2)
            .position(|w| w[0] <= value && value < w[1]);
        prop_assert_eq!(level.bucket_of(value), expected);
    }
}

#[test]
fn an_index_needs_between_one_and_max_levels() {
    assert_eq!(Index::new(vec![]).err(), Some(SchemaError::NoLevels));
    let too_many = (0..=MAX_LEVELS).map(|_| tag_level()).collect();
    assert_eq!(
        CompoundIndex::<Vec<u32>>::new(too_many).err(),
        Some(SchemaError::TooManyLevels {
            max: MAX_LEVELS,
            got: MAX_LEVELS + 1
        })
    );
    let most = (0..MAX_LEVELS).map(|_| tag_level()).collect();
    assert!(Index::new(most).is_ok());
}
