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
fn units_and_construction_match_c() {
    for (code, text, canonical, factor) in [
        (0, c"kM", c"km", 1000.0),
        (1, c"M", c"m", 1.0),
        (2, c"Ft", c"ft", 0.3048),
        (3, c"mI", c"mi", 1609.34),
    ] {
        let unit = DistanceUnit::parse(text);
        assert_eq!(unit as i32, code);
        assert_eq!(unit.as_c_str(), canonical);
        assert_eq!(unit.meters_factor(), factor);
    }
    for bad in [c"", c" km", c"km ", c"meters", c"\xff", c"K"] {
        assert_eq!(DistanceUnit::parse(bad), DistanceUnit::Invalid);
    }
    assert_eq!(DistanceUnit::Invalid.as_c_str(), c"<badunit>");
    assert_eq!(DistanceUnit::Invalid as i32, -1);
    assert_eq!(
        DistanceUnit::parse_buffer(b"kM\0x"),
        DistanceUnit::Kilometers
    );
    assert_eq!(DistanceUnit::parse_buffer(b"k"), DistanceUnit::Invalid);
    let mut padded = [0; 16];
    padded[0] = b'm';
    assert_eq!(
        DistanceUnit::parse_buffer(&padded[..15]),
        DistanceUnit::Meters
    );
    assert_eq!(DistanceUnit::parse_buffer(&padded), DistanceUnit::Invalid);
    let filter = RadiusFilter::new(12.5, -42.25, -3.0, None);
    assert_eq!(
        (filter.lon, filter.lat, filter.radius),
        (12.5, -42.25, -3.0)
    );
    assert_eq!(filter.unit, DistanceUnit::Kilometers);
    assert_eq!(
        RadiusFilter::new(0.0, 0.0, 1.0, Some(b"")).unit,
        DistanceUnit::Invalid
    );
}

#[test]
fn validation_order_and_float_boundaries_match_c() {
    use FilterError::{Coordinates, Radius, Unit};
    for (lon, lat, radius, unit, expected) in [
        (181.0, 91.0, -1.0, &b"bad"[..], Err(Unit)),
        (181.0, 91.0, -1.0, &b"m"[..], Err(Coordinates)),
        (0.0, 0.0, 0.0, b"m", Err(Radius)),
        (0.0, 0.0, -0.0, b"m", Err(Radius)),
        (0.0, 0.0, f64::NEG_INFINITY, b"m", Err(Radius)),
        (-180.0, -90.0, 1.0, b"m", Ok(())),
        (180.0, 90.0, 1.0, b"m", Ok(())),
        ((-180.0_f64).next_down(), 0.0, 1.0, b"m", Err(Coordinates)),
        (0.0, 90.0_f64.next_up(), 1.0, b"m", Err(Coordinates)),
        (f64::INFINITY, 0.0, 1.0, b"m", Err(Coordinates)),
        (0.0, f64::NEG_INFINITY, 1.0, b"m", Err(Coordinates)),
        (f64::NAN, f64::NAN, f64::NAN, b"m", Ok(())),
        (f64::NAN, 91.0, 1.0, b"m", Err(Coordinates)),
        (181.0, f64::NAN, 1.0, b"m", Err(Coordinates)),
        (f64::NAN, 0.0, 0.0, b"m", Err(Radius)),
        (0.0, f64::NAN, 0.0, b"m", Err(Radius)),
        (f64::NAN, 0.0, 1.0, b"m", Ok(())),
        (0.0, f64::NAN, 1.0, b"m", Ok(())),
        (0.0, 0.0, f64::NAN, b"m", Ok(())),
        (0.0, 0.0, f64::INFINITY, b"m", Ok(())),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, radius, Some(unit)).validate(),
            expected
        );
    }
    assert_eq!(Unit.to_string(), "Invalid GeoFilter unit");
    assert_eq!(Coordinates.to_string(), "Invalid GeoFilter lat/lon");
    assert_eq!(Radius.to_string(), "Invalid GeoFilter radius");
}

#[test]
fn radius_units_and_inclusive_boundary_match_c() {
    // Independent 26-bit axis quantization of (12.6, -42.2) gives this score.
    // Its cell center is (12.600001394748688, -42.200001002212765); a separate
    // spherical-distance calculation gives the expected meters below.
    let score = 2_542_122_275_915_209.0;
    let expected_distance = 9938.194472756348;
    let mut filter = RadiusFilter::new(12.5, -42.25, 1.0, Some(b"m"));
    for (unit, factor) in [
        (DistanceUnit::Meters, 1.0),
        (DistanceUnit::Kilometers, 1000.0),
        (DistanceUnit::Feet, 0.3048),
        (DistanceUnit::Miles, 1609.34),
    ] {
        filter.unit = unit;
        filter.radius = f64::MAX;
        assert_eq!(filter.validate(), Ok(()));
        for (radius, expected) in [
            ((expected_distance + 1.0) / factor, true),
            ((expected_distance - 1.0) / factor, false),
            (f64::MAX, true),
            (f64::NAN, false),
            (f64::INFINITY, true),
        ] {
            filter.radius = radius;
            let (inside, distance) = filter.within_radius(score);
            assert_eq!(inside, expected);
            assert!((distance - expected_distance).abs() < 1e-7);
        }
    }
    // Coincident points give an exact boundary without relying on transcendental
    // results being bit-identical across calls (Miri deliberately perturbs them).
    let mut filter = RadiusFilter::new(12.600001394748688, -42.200001002212765, 0.0, Some(b"m"));
    assert_eq!(filter.within_radius(score), (true, 0.0));
    filter.radius = 0.0_f64.next_down();
    assert_eq!(filter.within_radius(score), (false, 0.0));
}

#[test]
fn score_conversion_truncates_and_saturates() {
    // Score zero is the center of the first 26-bit cell on each axis.
    let filter = RadiusFilter::new(
        -180.0 + 180.0 / 67_108_864.0,
        -85.05112878 + 85.05112878 / 67_108_864.0,
        0.01,
        Some(b"m"),
    );
    for score in [0.0, 0.75, -1.0, f64::NAN, f64::NEG_INFINITY] {
        assert_eq!(filter.within_radius(score), (true, 0.0));
    }
    let (inside, distance) = filter.within_radius(1.0);
    assert!(!inside);
    assert!(distance > 0.25);

    // Saturating to all-one bits decodes beyond both axes and clamps to their maxima.
    let filter = RadiusFilter::new(180.0, 85.05112878, 0.01, Some(b"m"));
    for score in [f64::INFINITY, f64::MAX, 18_446_744_073_709_551_616.0] {
        assert_eq!(filter.within_radius(score), (true, 0.0));
    }
}

#[test]
fn invalid_unit_retains_c_release_factor() {
    let score = 2_542_122_275_915_209.0;
    let mut filter = RadiusFilter::new(12.5, -42.25, 10000.0, Some(b"bad"));
    assert_eq!(filter.validate(), Err(FilterError::Unit));
    assert_eq!(DistanceUnit::Invalid.meters_factor(), -1.0);
    let (inside, distance) = filter.within_radius(score);
    assert!(!inside);
    assert!((distance - 9938.194472756348).abs() < 1e-7);
    filter.radius = -10000.0;
    let (inside, distance) = filter.within_radius(score);
    assert!(inside);
    assert!((distance - 9938.194472756348).abs() < 1e-7);
}

#[test]
#[should_panic]
fn nonfinite_center_cannot_be_evaluated() {
    RadiusFilter::new(f64::NAN, 0.0, 1.0, None).within_radius(0.0);
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Miri's isolation forbids filesystem-backed failure persistence.
        #[cfg(miri)]
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn unit_buffer_limit_precedes_nul_truncation(
        index in 0usize..4,
        uppercase in any::<bool>(),
        suffix in prop::collection::vec(any::<u8>(), 0..25),
    ) {
        let units = [
            (b"km".as_slice(), DistanceUnit::Kilometers),
            (b"m", DistanceUnit::Meters),
            (b"ft", DistanceUnit::Feet),
            (b"mi", DistanceUnit::Miles),
        ];
        let (name, unit) = units[index];
        let mut bytes = name.to_vec();
        if uppercase { bytes.make_ascii_uppercase(); }
        bytes.push(0);
        bytes.extend(suffix);
        let expected = if bytes.len() < 16 { unit }
                       else { DistanceUnit::Invalid };
        prop_assert_eq!(DistanceUnit::parse_buffer(&bytes), expected);
    }
}
