/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{
    DistanceUnit::{self, Feet, Kilometers, Meters, Miles},
    InvalidGeoFilter, RadiusFilter,
    hash::{self, GEO_STEP_MAX, WGS84Coordinates},
};

const fn filter(lon: f64, lat: f64, radius: f64, unit: DistanceUnit) -> RadiusFilter {
    RadiusFilter {
        lon,
        lat,
        radius,
        unit,
    }
}

/// Encodes a point the way the geo index stores it.
fn geohash(lon: f64, lat: f64) -> f64 {
    let coords = WGS84Coordinates::from_f64(lon, lat).expect("coordinates are in bounds");
    hash::align_52bits(hash::encode_wgs84(coords, GEO_STEP_MAX)) as f64
}

#[test]
fn units_parse_from_their_names_ignoring_ascii_case() {
    for (name, unit) in [
        ("km", Kilometers),
        ("KM", Kilometers),
        ("m", Meters),
        ("M", Meters),
        ("ft", Feet),
        ("Ft", Feet),
        ("mi", Miles),
        ("mI", Miles),
    ] {
        assert_eq!(DistanceUnit::parse(name.as_bytes()), Some(unit), "{name}");
        assert_eq!(unit.name().to_bytes(), name.to_ascii_lowercase().as_bytes());
    }
}

#[test]
fn unknown_unit_names_do_not_parse() {
    for name in [&b""[..], b"k", b"kms", b" km", b"meters", b"km\0x", b"\xff"] {
        assert_eq!(DistanceUnit::parse(name), None, "{name:?}");
    }
}

#[test]
fn radius_converts_to_meters_by_unit() {
    for (unit, meters) in [
        (Kilometers, 2000.0),
        (Meters, 2.0),
        (Feet, 0.6096),
        (Miles, 3218.68),
    ] {
        let actual = filter(0.0, 0.0, 2.0, unit).radius_meters();
        assert!((actual - meters).abs() < 1e-9, "{unit:?}: {actual}");
    }
}

#[test]
fn invalid_unit_is_reported_before_coordinates_and_radius() {
    assert_eq!(
        RadiusFilter::new(181.0, 91.0, -1.0, None),
        Err(InvalidGeoFilter::Unit)
    );
}

#[test]
fn out_of_range_coordinates_are_reported_before_radius() {
    for (name, lon, lat) in [
        ("lon above", 180.000_001, 0.0),
        ("lon below", -180.000_001, 0.0),
        ("lat above", 0.0, 90.000_001),
        ("lat below", 0.0, -90.000_001),
        ("infinite lon", f64::INFINITY, 0.0),
        ("infinite lat", 0.0, f64::NEG_INFINITY),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, -1.0, Some(Meters)),
            Err(InvalidGeoFilter::Coordinates),
            "{name}"
        );
    }
}

#[test]
fn coordinates_at_their_limits_are_accepted() {
    for (name, lon, lat) in [("upper", 180.0, 90.0), ("lower", -180.0, -90.0)] {
        assert_eq!(
            RadiusFilter::new(lon, lat, 1.0, Some(Miles)),
            Ok(filter(lon, lat, 1.0, Miles)),
            "{name}"
        );
    }
}

#[test]
fn non_positive_radii_are_rejected() {
    for radius in [0.0, -0.0, -1.0, f64::NEG_INFINITY] {
        assert_eq!(
            RadiusFilter::new(0.0, 0.0, radius, Some(Kilometers)),
            Err(InvalidGeoFilter::Radius),
            "{radius}"
        );
    }
}

#[test]
fn nan_parameters_pass_validation() {
    for (name, lon, lat, radius) in [
        ("lon", f64::NAN, 0.0, 1.0),
        ("lat", 0.0, f64::NAN, 1.0),
        ("radius", 0.0, 0.0, f64::NAN),
    ] {
        assert!(
            RadiusFilter::new(lon, lat, radius, Some(Feet)).is_ok(),
            "{name}"
        );
    }
}

#[test]
fn distance_is_measured_to_the_decoded_geohash() {
    // Expected haversine distances on the 6372797.560856 m sphere; the geohash
    // moves a point by well under a meter.
    for (name, center, point, meters) in [
        ("along a meridian", (0.0, 0.0), (0.0, 1.0), 111_226.3),
        ("along a parallel", (0.0, 60.0), (1.0, 60.0), 55_612.62),
    ] {
        let actual = filter(center.0, center.1, 1.0, Meters).distance_to(geohash(point.0, point.1));
        assert!((actual - meters).abs() < 1.0, "{name}: {actual}");
    }
}

#[test]
fn points_on_the_radius_are_within_it() {
    let within = filter(0.0, 0.0, 1.5, Kilometers);
    assert!(within.reaches(0.0));
    assert!(within.reaches(1500.0));
    assert!(!within.reaches(1500.0_f64.next_up()));
    assert!(!filter(0.0, 0.0, f64::NAN, Kilometers).reaches(0.0));
}

#[test]
fn nan_centers_reach_no_point() {
    for (name, lon, lat) in [("lon", f64::NAN, 0.0), ("lat", 0.0, f64::NAN)] {
        let filter = filter(lon, lat, 1.0, Kilometers);
        assert!(
            !filter.reaches(filter.distance_to(geohash(0.0, 0.0))),
            "{name}"
        );
    }
}
