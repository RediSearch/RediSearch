/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat};
use geo::{GeoDistance, GeoFilter, GeoFilterError};
use rqe_core::RS_INVALID_FIELD_INDEX;

#[test]
fn unit_parsing_spelling_and_factors() {
    for (bytes, unit, spelling, factor) in [
        (b"kM".as_slice(), GeoDistance::Kilometers, "km", 1000.0),
        (b"M".as_slice(), GeoDistance::Meters, "m", 1.0),
        (b"Ft".as_slice(), GeoDistance::Feet, "ft", 0.3048),
        (b"mI".as_slice(), GeoDistance::Miles, "mi", 1609.34),
    ] {
        assert_eq!(GeoDistance::parse(bytes), unit);
        assert_eq!(GeoDistance::parse_buffer(bytes), unit);
        assert_eq!(unit.as_str(), spelling);
        assert_eq!(unit.factor(), factor);
    }
    for bytes in [b"".as_slice(), b"meters", b" km", b"km ", b"\xff"] {
        assert_eq!(GeoDistance::parse(bytes), GeoDistance::Invalid);
    }
    assert_eq!(GeoDistance::Invalid.as_str(), "<badunit>");
}

#[test]
fn unit_buffer_limit_is_checked_before_nul_truncation() {
    let mut bytes = [b'x'; 17];
    bytes[..3].copy_from_slice(b"km\0");
    assert_eq!(GeoDistance::parse(&bytes), GeoDistance::Kilometers);
    assert_eq!(
        GeoDistance::parse_buffer(&bytes[..15]),
        GeoDistance::Kilometers
    );
    assert_eq!(
        GeoDistance::parse_buffer(&bytes[..16]),
        GeoDistance::Invalid
    );
    assert_eq!(GeoDistance::parse_buffer(&bytes), GeoDistance::Invalid);
    assert_eq!(GeoDistance::parse_buffer(b"\0km"), GeoDistance::Invalid);
}

#[test]
#[should_panic(expected = "invalid GeoDistance unit")]
fn invalid_unit_factor_panics() {
    GeoDistance::Invalid.factor();
}

#[test]
fn construction_defers_validation_and_field_resolution() {
    let mut filter = GeoFilter::new(181.0, 91.0, -1.0, None);
    assert_eq!(filter.field_index, RS_INVALID_FIELD_INDEX);
    assert_eq!((filter.lon, filter.lat, filter.radius), (181.0, 91.0, -1.0));
    assert_eq!(filter.unit, GeoDistance::Kilometers);
    filter.set_field(Some(42));
    assert_eq!(filter.field_index, 42);
    filter.set_field(None);
    assert_eq!(filter.field_index, RS_INVALID_FIELD_INDEX);
    assert_eq!(
        GeoFilter::new(0.0, 0.0, 1.0, Some(b"")).unit,
        GeoDistance::Invalid
    );
}

#[test]
fn validation_precedence_and_error_text() {
    let mut filter = GeoFilter::new(181.0, 91.0, -1.0, Some(b"invalid"));
    assert_eq!(filter.validate(), Err(GeoFilterError::Unit));
    assert_eq!(
        filter.validate().unwrap_err().to_string(),
        "Invalid GeoFilter unit"
    );
    filter.unit = GeoDistance::Meters;
    assert_eq!(filter.validate(), Err(GeoFilterError::Coordinates));
    assert_eq!(
        filter.validate().unwrap_err().to_string(),
        "Invalid GeoFilter lat/lon"
    );
    filter.lon = 0.0;
    filter.lat = 0.0;
    assert_eq!(filter.validate(), Err(GeoFilterError::Radius));
    assert_eq!(
        filter.validate().unwrap_err().to_string(),
        "Invalid GeoFilter radius"
    );
}

#[test]
fn validation_preserves_comparison_boundaries_and_nonfinite_values() {
    for (lon, lat) in [(180.0, 90.0), (-180.0, -90.0), (f64::NAN, f64::NAN)] {
        assert_eq!(GeoFilter::new(lon, lat, 1.0, None).validate(), Ok(()));
    }
    for (lon, lat) in [
        (180.0_f64.next_up(), 0.0),
        ((-180.0_f64).next_down(), 0.0),
        (0.0, 90.0_f64.next_up()),
        (0.0, (-90.0_f64).next_down()),
        (f64::INFINITY, 0.0),
        (0.0, f64::NEG_INFINITY),
    ] {
        assert_eq!(
            GeoFilter::new(lon, lat, 1.0, None).validate(),
            Err(GeoFilterError::Coordinates)
        );
    }
    for radius in [0.0, -0.0, -1.0, f64::NEG_INFINITY] {
        assert_eq!(
            GeoFilter::new(0.0, 0.0, radius, None).validate(),
            Err(GeoFilterError::Radius)
        );
    }
    for radius in [0.0_f64.next_up(), f64::NAN, f64::INFINITY] {
        assert_eq!(GeoFilter::new(0.0, 0.0, radius, None).validate(), Ok(()));
    }
}

#[test]
fn containment_is_inclusive_and_always_reports_meters() {
    let score = 3_477_064_553_862_190.0;
    let mut filter = GeoFilter::new(13.0, 38.0, 1.0, Some(b"m"));
    let (within, distance) = filter.is_within_radius(score);
    assert!(!within);
    assert!(distance > 1.0);
    filter.radius = distance;
    assert_eq!(filter.is_within_radius(score), (true, distance));
    filter.radius = distance.next_down();
    assert_eq!(filter.is_within_radius(score), (false, distance));
    for unit in [
        GeoDistance::Kilometers,
        GeoDistance::Meters,
        GeoDistance::Feet,
        GeoDistance::Miles,
    ] {
        filter.unit = unit;
        filter.radius = distance * 2.0 / unit.factor();
        assert_eq!(filter.is_within_radius(score), (true, distance));
        filter.radius = distance / (2.0 * unit.factor());
        assert_eq!(filter.is_within_radius(score), (false, distance));
    }
    filter.radius = f64::NAN;
    assert_eq!(filter.is_within_radius(score), (false, distance));
    filter.radius = f64::INFINITY;
    assert_eq!(filter.is_within_radius(score), (true, distance));
}

#[test]
fn score_zero_and_saturating_decoder_are_preserved() {
    let (lon, lat) = decode_to_lon_lat(GeoHashBits {
        bits: 0,
        step: GEO_STEP_MAX,
    });
    let filter = GeoFilter::new(lon.into_inner(), lat.into_inner(), 1.0, None);
    for score in [0.0, -1.0, f64::NEG_INFINITY, f64::NAN, 0.9] {
        assert_eq!(filter.is_within_radius(score), (true, 0.0));
    }
}

#[test]
#[should_panic]
fn nan_center_preserves_evaluation_panic_despite_validation() {
    let filter = GeoFilter::new(f64::NAN, 0.0, 1.0, None);
    assert_eq!(filter.validate(), Ok(()));
    filter.is_within_radius(0.0);
}
