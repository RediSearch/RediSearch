/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use geo::hash::{
    GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, GEO_STEP_MAX, PrecisionStep,
    WGS84Coordinates, align_52bits, decode_to_lon_lat, encode_wgs84,
};

#[test]
fn encode_decode_roundtrip() {
    let cases = [
        (0.0, 0.0),
        (10.0, 20.0),
        (-122.4194, 37.7749),   // San Francisco
        (2.3522, 48.8566),      // Paris
        (139.6917, 35.6895),    // Tokyo
        (-0.1278, 51.5074),     // London
        (180.0, 85.05112878),   // max bounds
        (-180.0, -85.05112878), // min bounds
    ];

    for (lon, lat) in cases {
        let coords = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat))
            .unwrap_or_else(|e| panic!("failed to create coords ({lon}, {lat}): {e:?}"));
        let hash = encode_wgs84(coords, GEO_STEP_MAX);
        let (decoded_lon, decoded_lat) = decode_to_lon_lat(hash);
        let (decoded_lon, decoded_lat) = (decoded_lon.into_inner(), decoded_lat.into_inner());
        // 52-bit precision gives sub-meter accuracy, so 0.001 degree tolerance is generous
        assert!(
            (decoded_lon - lon).abs() < 0.001,
            "lon mismatch for ({lon}, {lat}): got {decoded_lon}"
        );
        assert!(
            (decoded_lat - lat).abs() < 0.001,
            "lat mismatch for ({lon}, {lat}): got {decoded_lat}"
        );
    }
}

#[test]
fn encode_out_of_range_longitude() {
    let err = WGS84Coordinates::new(R64::assert(181.0), R64::assert(0.0)).unwrap_err();
    assert_eq!(err.longitude, Some(181.0));
    assert_eq!(err.latitude, None);

    let err = WGS84Coordinates::new(R64::assert(-181.0), R64::assert(0.0)).unwrap_err();
    assert_eq!(err.longitude, Some(-181.0));
    assert_eq!(err.latitude, None);
}

#[test]
fn encode_out_of_range_latitude() {
    let err = WGS84Coordinates::new(R64::assert(0.0), R64::assert(90.0)).unwrap_err();
    assert_eq!(err.longitude, None);
    assert_eq!(err.latitude, Some(90.0));

    let err = WGS84Coordinates::new(R64::assert(0.0), R64::assert(-90.0)).unwrap_err();
    assert_eq!(err.longitude, None);
    assert_eq!(err.latitude, Some(-90.0));
}

#[test]
fn encode_out_of_range_both() {
    let err = WGS84Coordinates::new(R64::assert(200.0), R64::assert(90.0)).unwrap_err();
    assert_eq!(err.longitude, Some(200.0));
    assert_eq!(err.latitude, Some(90.0));
}

#[test]
fn from_f64_valid() {
    let c = WGS84Coordinates::from_f64(10.0, 20.0).unwrap();
    assert_eq!(c.longitude(), R64::assert(10.0));
    assert_eq!(c.latitude(), R64::assert(20.0));
}

#[test]
fn from_f64_boundary_values() {
    let c = WGS84Coordinates::from_f64(GEO_LONG_MIN, GEO_LAT_MIN).unwrap();
    assert_eq!(c.longitude(), R64::assert(GEO_LONG_MIN));
    assert_eq!(c.latitude(), R64::assert(GEO_LAT_MIN));

    let c = WGS84Coordinates::from_f64(GEO_LONG_MAX, GEO_LAT_MAX).unwrap();
    assert_eq!(c.longitude(), R64::assert(GEO_LONG_MAX));
    assert_eq!(c.latitude(), R64::assert(GEO_LAT_MAX));
}

#[test]
fn from_f64_out_of_range() {
    let err = WGS84Coordinates::from_f64(181.0, 0.0).unwrap_err();
    assert_eq!(err.longitude, Some(181.0));
    assert_eq!(err.latitude, None);

    let err = WGS84Coordinates::from_f64(0.0, 90.0).unwrap_err();
    assert_eq!(err.longitude, None);
    assert_eq!(err.latitude, Some(90.0));
}

#[test]
fn from_f64_nan_rejected() {
    assert!(WGS84Coordinates::from_f64(f64::NAN, 0.0).is_err());
    assert!(WGS84Coordinates::from_f64(0.0, f64::NAN).is_err());
    assert!(WGS84Coordinates::from_f64(f64::NAN, f64::NAN).is_err());
}

#[test]
fn from_f64_infinity_rejected() {
    assert!(WGS84Coordinates::from_f64(f64::INFINITY, 0.0).is_err());
    assert!(WGS84Coordinates::from_f64(0.0, f64::NEG_INFINITY).is_err());
}

#[test]
fn try_from_tuple() {
    let c = WGS84Coordinates::try_from((10.0, 20.0)).unwrap();
    assert_eq!(c, WGS84Coordinates::from_f64(10.0, 20.0).unwrap());
    assert!(WGS84Coordinates::try_from((181.0, 0.0)).is_err());
}

#[test]
fn precision_step_rejects_invalid_values() {
    assert!(PrecisionStep::new(0).is_err());
    assert!(PrecisionStep::new(27).is_err());
    assert!(PrecisionStep::new(1).is_ok());
    assert!(PrecisionStep::new(26).is_ok());
}

#[test]
fn nearby_points_have_similar_hashes() {
    let h1 = encode_wgs84(
        WGS84Coordinates::new(R64::assert(29.69465), R64::assert(34.95126)).unwrap(),
        GEO_STEP_MAX,
    );
    let h2 = encode_wgs84(
        WGS84Coordinates::new(R64::assert(29.69350), R64::assert(34.94737)).unwrap(),
        GEO_STEP_MAX,
    );
    // Nearby points should share high-order bits
    let aligned1 = geo::hash::align_52bits(h1);
    let aligned2 = geo::hash::align_52bits(h2);
    // Top 30 bits should match for points ~500m apart
    assert_eq!(aligned1 >> 22, aligned2 >> 22);
}

#[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
mod proptests {
    use decorum::R64;
    use geo::hash::{
        GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, PrecisionStep, WGS84Coordinates,
        decode_to_lon_lat, encode_wgs84,
    };
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn encode_decode_roundtrip(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            step in 1u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let coords = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat))
                .unwrap_or_else(|e| panic!("failed to create coords ({lon}, {lat}): {e:?}"));
            let hash = encode_wgs84(coords, step);
            let (decoded_lon, decoded_lat) = decode_to_lon_lat(hash);
            let (decoded_lon, decoded_lat) = (decoded_lon.into_inner(), decoded_lat.into_inner());
            // Lower steps have coarser precision — scale tolerance by cell size.
            let tolerance = (GEO_LONG_MAX - GEO_LONG_MIN) / (1u64 << step.as_u8()) as f64;
            prop_assert!(
                (decoded_lon - lon).abs() < tolerance,
                "lon mismatch for ({}, {}) step={}: got {}, tolerance={}",
                lon, lat, step.as_u8(), decoded_lon, tolerance
            );
            prop_assert!(
                (decoded_lat - lat).abs() < tolerance,
                "lat mismatch for ({}, {}) step={}: got {}, tolerance={}",
                lon, lat, step.as_u8(), decoded_lat, tolerance
            );
        }
    }
}

#[test]
fn align_52bits_fits_in_52_bits() {
    for step in 1..=26 {
        let step = PrecisionStep::new(step).unwrap();
        // Test all four corners + interior point.
        for (lon, lat) in [
            (GEO_LONG_MIN, GEO_LAT_MIN),
            (GEO_LONG_MAX, GEO_LAT_MAX),
            (GEO_LONG_MIN, GEO_LAT_MAX),
            (GEO_LONG_MAX, GEO_LAT_MIN),
            (0.0, 0.0),
        ] {
            let coords = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap();
            let h = encode_wgs84(coords, step);
            let aligned = align_52bits(h);
            assert!(
                aligned < (1u64 << 52),
                "step={}, ({lon}, {lat}): aligned 0x{aligned:013x} exceeds 52 bits",
                step.as_u8()
            );
        }
    }
}
