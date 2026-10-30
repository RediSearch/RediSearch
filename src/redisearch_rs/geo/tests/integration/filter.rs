/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{DistanceUnit, FilterError, RadiusFilter};

#[test]
fn units() {
    for (name, bytes, expected) in [
        (
            "kilometers",
            b"kM".as_slice(),
            Some(DistanceUnit::Kilometers),
        ),
        ("meters", b"M", Some(DistanceUnit::Meters)),
        ("feet", b"Ft", Some(DistanceUnit::Feet)),
        ("miles", b"mI", Some(DistanceUnit::Miles)),
        ("empty", b"", None),
        ("whitespace", b" km", None),
        ("suffix", b"kms", None),
        ("non_utf8", b"\xff", None),
        (
            "embedded_nul",
            b"km\0ignored",
            Some(DistanceUnit::Kilometers),
        ),
        ("nul_only", b"\0", None),
    ] {
        assert_eq!(DistanceUnit::parse(bytes), expected, "{name}");
        assert_eq!(DistanceUnit::parse_buffer(bytes), expected, "{name}");
    }
    assert_eq!(
        DistanceUnit::parse_buffer(b"km\0............"),
        Some(DistanceUnit::Kilometers)
    );
    assert_eq!(DistanceUnit::parse_buffer(b"km\0............."), None);
    assert_eq!(
        DistanceUnit::parse(b"km\0............."),
        Some(DistanceUnit::Kilometers)
    );
    for (unit, spelling) in [
        (DistanceUnit::Kilometers, c"km"),
        (DistanceUnit::Meters, c"m"),
        (DistanceUnit::Feet, c"ft"),
        (DistanceUnit::Miles, c"mi"),
    ] {
        assert_eq!(unit.as_c_str(), spelling);
    }
}

#[test]
fn validation() {
    for (name, lon, lat, radius, unit, expected) in [
        ("unit_first", 181.0, 91.0, -1.0, None, FilterError::Unit),
        (
            "lon_high",
            180.1,
            0.0,
            -1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lon_low",
            -180.1,
            0.0,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lat_high",
            0.0,
            90.1,
            -1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lat_low",
            0.0,
            -90.1,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lon_nan",
            f64::NAN,
            0.0,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lat_nan",
            0.0,
            f64::NAN,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lon_infinite",
            f64::INFINITY,
            0.0,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "lat_infinite",
            0.0,
            f64::NEG_INFINITY,
            1.0,
            Some(DistanceUnit::Meters),
            FilterError::Coordinates,
        ),
        (
            "radius_zero",
            12.0,
            -34.0,
            0.0,
            Some(DistanceUnit::Meters),
            FilterError::Radius,
        ),
        (
            "radius_negative",
            12.0,
            -34.0,
            -1.0,
            Some(DistanceUnit::Meters),
            FilterError::Radius,
        ),
        (
            "radius_nan",
            12.0,
            -34.0,
            f64::NAN,
            Some(DistanceUnit::Meters),
            FilterError::Radius,
        ),
        (
            "radius_negative_infinite",
            12.0,
            -34.0,
            f64::NEG_INFINITY,
            Some(DistanceUnit::Meters),
            FilterError::Radius,
        ),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, radius, unit).unwrap_err(),
            expected,
            "{name}"
        );
    }
    for (name, lon, lat, radius) in [
        ("upper_bounds", 180.0, 90.0, 1.0),
        ("lower_bounds", -180.0, -90.0, 1.0),
        ("above_geohash_latitude", 12.0, 86.0, 1.0),
        ("positive_infinity", 12.0, 34.0, f64::INFINITY),
        ("smallest_radius", 12.0, 34.0, f64::from_bits(1)),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, radius, Some(DistanceUnit::Meters))
                .unwrap()
                .radius_meters(),
            radius,
            "{name}"
        );
    }
}

#[test]
fn radius_boundary() {
    // Score zero decodes to the center of the southwesternmost geohash cell.
    let lon = -180.0 + 180.0 / (1_u64 << 26) as f64;
    let lat = -85.05112878 + 85.05112878 / (1_u64 << 26) as f64;
    for (name, unit, meters) in [
        ("kilometers", DistanceUnit::Kilometers, 2000.0),
        ("meters", DistanceUnit::Meters, 2.0),
        ("feet", DistanceUnit::Feet, 0.6096),
        ("miles", DistanceUnit::Miles, 3218.68),
    ] {
        let filter = RadiusFilter::new(lon, lat, 2.0, Some(unit)).unwrap();
        assert_eq!(filter.radius_meters(), meters, "{name}");
        assert!(filter.distance(0.0) < 1e-6, "{name}");
    }
    let filter = RadiusFilter::new(lon, lat + 1.0, 1.0, Some(DistanceUnit::Meters)).unwrap();
    // Independently use arc length along a meridian for a one-degree separation.
    let expected = 6_372_797.560_856 * std::f64::consts::PI / 180.0;
    assert!((filter.distance(0.0) - expected).abs() < 1e-6);
    assert!(filter.distance(0.0) > filter.radius_meters());
}

#[cfg(not(miri))] // proptest requires OS randomness and filesystem access.
#[test]
fn radius_properties() {
    use geo::hash::{GEO_STEP_MAX, WGS84Coordinates, align_52bits, encode_wgs84};
    use proptest::prelude::*;

    proptest!(|(lon in -179.0..179.0, lat in -84.0..84.0, radius in 1.0..1_000_000.0)| {
        let score = align_52bits(encode_wgs84(WGS84Coordinates::from_f64(lon, lat).unwrap(), GEO_STEP_MAX)) as f64;
        let filter = RadiusFilter::new(lon, lat, radius, Some(DistanceUnit::Meters)).unwrap();
        prop_assert!(filter.distance(score) < 1.0);
        let opposite = RadiusFilter::new(lon, -lat, radius, Some(DistanceUnit::Meters)).unwrap();
        let arc = 2.0 * lat.abs() * std::f64::consts::PI / 180.0 * 6_372_797.560_856;
        prop_assert!((opposite.distance(score) - arc).abs() < 1.0);
    });
}
