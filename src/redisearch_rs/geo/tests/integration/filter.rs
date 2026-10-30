/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{DistanceUnit, FilterError, RadiusFilter};
use proptest::prelude::*;

#[test]
fn unit_tokens() {
    for (name, token, expected) in [
        (
            "kilometers",
            b"kM".as_slice(),
            Some(DistanceUnit::Kilometers),
        ),
        ("meters", b"M", Some(DistanceUnit::Meters)),
        ("feet", b"Ft", Some(DistanceUnit::Feet)),
        ("miles", b"MI", Some(DistanceUnit::Miles)),
        ("empty", b"", None),
        ("suffix", b"kms", None),
        ("whitespace", b" m", None),
        ("nul", b"m\0", None),
        ("binary", b"\xff", None),
    ] {
        assert_eq!(DistanceUnit::parse(token), expected, "{name}");
    }
    for (unit, canonical) in [
        (DistanceUnit::Meters, c"m"),
        (DistanceUnit::Kilometers, c"km"),
        (DistanceUnit::Feet, c"ft"),
        (DistanceUnit::Miles, c"mi"),
    ] {
        assert_eq!(unit.as_c_str(), canonical);
    }
}

#[test]
fn validation_priority_and_bounds() {
    for (name, lon, lat, radius, unit, expected) in [
        (
            "unit first",
            181.0,
            91.0,
            -1.0,
            None,
            Some(FilterError::Unit),
        ),
        (
            "coordinates before radius",
            181.0,
            0.0,
            -1.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Coordinates),
        ),
        (
            "south outside",
            0.0,
            -90.01,
            1.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Coordinates),
        ),
        (
            "west outside",
            -180.01,
            0.0,
            1.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Coordinates),
        ),
        (
            "nan longitude",
            f64::NAN,
            0.0,
            1.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Coordinates),
        ),
        (
            "infinite latitude",
            0.0,
            f64::INFINITY,
            1.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Coordinates),
        ),
        (
            "nan radius",
            0.0,
            0.0,
            f64::NAN,
            Some(DistanceUnit::Meters),
            Some(FilterError::Radius),
        ),
        (
            "zero radius",
            0.0,
            0.0,
            -0.0,
            Some(DistanceUnit::Meters),
            Some(FilterError::Radius),
        ),
        (
            "negative infinity",
            0.0,
            0.0,
            f64::NEG_INFINITY,
            Some(DistanceUnit::Meters),
            Some(FilterError::Radius),
        ),
        (
            "northeast boundary",
            180.0,
            90.0,
            1.0,
            Some(DistanceUnit::Meters),
            None,
        ),
        (
            "southwest boundary",
            -180.0,
            -90.0,
            1.0,
            Some(DistanceUnit::Meters),
            None,
        ),
        (
            "infinite radius",
            13.0,
            52.0,
            f64::INFINITY,
            Some(DistanceUnit::Meters),
            None,
        ),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, radius, unit).err(),
            expected,
            "{name}"
        );
    }
}

#[test]
fn inclusive_radius_and_miss_distance() {
    // Spherical cosine formula at score 0's cell center (-179.9999973, -85.0511275).
    let filter = RadiusFilter::new(13.0, 52.0, 1.0, Some(DistanceUnit::Meters)).unwrap();
    let distance = filter.distance_to_score(0.0);
    assert!((distance - 16_328_703.86).abs() < 0.01, "{distance}");
    for (name, radius, expected) in [
        ("inside", distance + 0.001, true),
        ("outside", distance - 0.001, false),
    ] {
        let filter = RadiusFilter::new(13.0, 52.0, radius, Some(DistanceUnit::Meters)).unwrap();
        let (inside, measured) = filter.contains_score(0.0);
        assert_eq!(inside, expected, "{name}");
        assert!(
            (measured - distance).abs() < 0.000_001,
            "{name}: {measured}"
        );
    }

    // A subnormal radius in feet rounds to zero meters. At the cell center this
    // tests exact equality without Miri's transcendental rounding perturbations.
    let (lon, lat) = geo::hash::decode_to_lon_lat(geo::hash::GeoHashBits {
        bits: 0,
        step: geo::hash::GEO_STEP_MAX,
    });
    let boundary = RadiusFilter::new(
        lon.into_inner(),
        lat.into_inner(),
        f64::from_bits(1),
        Some(DistanceUnit::Feet),
    )
    .unwrap();
    assert_eq!(boundary.contains_score(0.0), (true, 0.0));
}

proptest! {
    #![proptest_config(ProptestConfig {
        #[cfg(miri)]
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn units_preserve_membership(
        lon in -180.0f64..=180.0,
        lat in -90.0f64..=90.0,
        score in 0u64..(1u64 << 52),
        radius in 0.001f64..20_000_000.0,
    ) {
        let meters = RadiusFilter::new(lon, lat, radius, Some(DistanceUnit::Meters)).unwrap();
        for (name, unit, factor) in [
            ("km", DistanceUnit::Kilometers, 1000.0),
            ("ft", DistanceUnit::Feet, 0.3048),
            ("mi", DistanceUnit::Miles, 1609.34),
        ] {
            let other = RadiusFilter::new(lon, lat, radius / factor, Some(unit)).unwrap();
            let (inside, distance) = other.contains_score(score as f64);
            let (expected, meters_distance) = meters.contains_score(score as f64);
            prop_assert_eq!(inside, expected, "{}", name);
            prop_assert!((distance - meters_distance).abs() < 0.000_001, "{}", name);
        }
    }
}
