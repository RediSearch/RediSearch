/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{GEO_STEP_MAX, WGS84Coordinates, align_52bits, encode_wgs84};
use geo::{GeoDistance, GeoFilter, GeoFilterError};

#[test]
fn unit_spellings_discriminants_and_factors() {
    for (spelling, unit, discriminant, factor) in [
        (b"kM".as_slice(), GeoDistance::Kilometers, 0, 1000.0),
        (b"M", GeoDistance::Meters, 1, 1.0),
        (b"Ft", GeoDistance::Feet, 2, 0.3048),
        (b"MI", GeoDistance::Miles, 3, 1609.34),
    ] {
        assert_eq!(GeoDistance::parse(spelling), unit);
        assert_eq!(GeoDistance::parse(unit.as_str().as_bytes()), unit);
        assert_eq!(unit as i32, discriminant);
        assert_eq!(unit.meters_factor(), factor);
    }
    assert_eq!(GeoDistance::Invalid as i32, -1);
    assert_eq!(GeoDistance::Invalid.as_str(), "<badunit>");
    for spelling in [b"".as_slice(), b" km", b"km ", b"meters", b"\xff"] {
        assert_eq!(GeoDistance::parse(spelling), GeoDistance::Invalid);
    }
}

#[test]
fn unit_buffer_limit_precedes_c_string_truncation() {
    let mut buffer = [b'x'; 16];
    buffer[..3].copy_from_slice(b"km\0");
    assert_eq!(GeoDistance::parse(&buffer), GeoDistance::Kilometers);
    assert_eq!(
        GeoDistance::parse_buffer(&buffer[..15]),
        GeoDistance::Kilometers
    );
    assert_eq!(GeoDistance::parse_buffer(&buffer), GeoDistance::Invalid);
    assert_eq!(GeoDistance::parse_buffer(b"\0km"), GeoDistance::Invalid);
}

#[test]
#[should_panic(expected = "invalid geo distance unit")]
fn invalid_unit_has_no_conversion_factor() {
    GeoDistance::Invalid.meters_factor();
}

#[test]
fn construction_does_not_validate_and_defaults_only_absent_units() {
    let filter = GeoFilter::new(181.0, 91.0, -1.0, None);
    assert_eq!(filter.lon, 181.0);
    assert_eq!(filter.lat, 91.0);
    assert_eq!(filter.radius, -1.0);
    assert_eq!(filter.unit, GeoDistance::Kilometers);
    assert_eq!(
        GeoFilter::new(0.0, 0.0, 1.0, Some(b"")).unit,
        GeoDistance::Invalid
    );
}

#[test]
fn validation_precedence_and_coordinate_bounds() {
    let mut filter = GeoFilter::new(181.0, 91.0, 0.0, Some(b"bad"));
    assert_eq!(filter.validate(), Err(GeoFilterError::Unit));
    filter.unit = GeoDistance::Meters;
    assert_eq!(filter.validate(), Err(GeoFilterError::Coordinates));
    for (lon, lat) in [
        (180.1, 0.0),
        (-180.1, 0.0),
        (0.0, 90.1),
        (0.0, -90.1),
        (f64::INFINITY, 0.0),
        (0.0, f64::NEG_INFINITY),
    ] {
        assert_eq!(
            GeoFilter::new(lon, lat, 1.0, None).validate(),
            Err(GeoFilterError::Coordinates)
        );
    }
    for (lon, lat) in [(180.0, 90.0), (-180.0, -90.0)] {
        assert_eq!(GeoFilter::new(lon, lat, 1.0, None).validate(), Ok(()));
    }
    for radius in [0.0, -0.0, -1.0, f64::NEG_INFINITY] {
        assert_eq!(
            GeoFilter::new(0.0, 0.0, radius, None).validate(),
            Err(GeoFilterError::Radius)
        );
    }
    for (error, text) in [
        (GeoFilterError::Unit, "Invalid GeoFilter unit"),
        (GeoFilterError::Coordinates, "Invalid GeoFilter lat/lon"),
        (GeoFilterError::Radius, "Invalid GeoFilter radius"),
    ] {
        assert_eq!(error.to_string(), text);
    }
}

#[test]
fn validation_preserves_nan_acceptance_and_unbounded_positive_radius() {
    for (lon, lat, radius) in [
        (f64::NAN, 0.0, 1.0),
        (0.0, f64::NAN, 1.0),
        (0.0, 0.0, f64::NAN),
        (0.0, 0.0, f64::INFINITY),
    ] {
        assert_eq!(GeoFilter::new(lon, lat, radius, None).validate(), Ok(()));
    }
}

#[test]
fn radius_check_returns_meters_even_outside_and_includes_boundary() {
    let score = align_52bits(encode_wgs84(
        WGS84Coordinates::from_f64(1.0, 1.0).unwrap(),
        GEO_STEP_MAX,
    )) as f64;
    let mut filter = GeoFilter::new(0.0, 0.0, 1.0, Some(b"m"));
    let (included, distance) = filter.is_within_radius(score);
    assert!(!included);
    assert!((distance - 157293.7).abs() < 1.0);
    filter.radius = distance;
    assert_eq!(filter.is_within_radius(score), (true, distance));
    filter.radius = f64::NAN;
    assert_eq!(filter.is_within_radius(score), (false, distance));
    filter.radius = f64::INFINITY;
    assert_eq!(filter.is_within_radius(score), (true, distance));
    for unit in [
        GeoDistance::Kilometers,
        GeoDistance::Feet,
        GeoDistance::Miles,
    ] {
        filter.unit = unit;
        filter.radius = distance / unit.meters_factor() + 1.0;
        assert_eq!(filter.is_within_radius(score), (true, distance));
        filter.radius = distance / unit.meters_factor() - 1.0;
        assert_eq!(filter.is_within_radius(score), (false, distance));
    }
}

#[test]
fn finite_radius_unit_conversion_overflow_retains_infinite_search_radius() {
    for unit in [GeoDistance::Kilometers, GeoDistance::Miles] {
        let filter = GeoFilter {
            lon: 0.0,
            lat: 0.0,
            radius: f64::MAX,
            unit,
        };
        assert!(filter.radius.is_finite());
        assert_eq!(filter.validate(), Ok(()));
        assert_eq!(filter.radius * unit.meters_factor(), f64::INFINITY);
        let (included, distance) = filter.is_within_radius(0.0);
        assert!(included);
        assert!(distance.is_finite());
        assert!(distance > 0.0);
    }
}

#[test]
fn radius_score_cast_matches_existing_decoder() {
    let filter = GeoFilter::new(0.0, 0.0, 1.0, None);
    let zero = filter.is_within_radius(0.0);
    for score in [-1.0, f64::NEG_INFINITY, f64::NAN, 0.9] {
        assert_eq!(filter.is_within_radius(score), zero);
    }
    assert_eq!(
        filter.is_within_radius(f64::INFINITY),
        filter.is_within_radius(u64::MAX as f64)
    );
}

#[test]
#[should_panic]
fn nan_center_still_panics_downstream_of_successful_validation() {
    let filter = GeoFilter::new(f64::NAN, 0.0, 1.0, None);
    assert_eq!(filter.validate(), Ok(()));
    filter.is_within_radius(0.0);
}
