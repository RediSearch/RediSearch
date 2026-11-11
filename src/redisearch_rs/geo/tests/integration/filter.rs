/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{DistanceUnit, RadiusFilter};

#[test]
fn distance_units() {
    for (name, unit, meters) in [
        ("km", DistanceUnit::Kilometers, 1000.0),
        ("m", DistanceUnit::Meters, 1.0),
        ("ft", DistanceUnit::Feet, 0.3048),
        ("mi", DistanceUnit::Miles, 1609.34),
    ] {
        assert_eq!(DistanceUnit::parse(name.as_bytes()), Some(unit), "{name}");
        assert_eq!(
            DistanceUnit::parse(name.to_uppercase().as_bytes()),
            Some(unit)
        );
        assert_eq!(DistanceUnit::from_raw(unit as i32), Some(unit));
        assert_eq!(unit.as_c_str().to_bytes(), name.as_bytes());
        assert_eq!(unit.meters(), meters);
    }
    for invalid in [
        b"".as_slice(),
        b" km",
        b"km ",
        b"k",
        b"miles",
        b"m\0",
        b"\xff",
    ] {
        assert_eq!(DistanceUnit::parse(invalid), None, "{invalid:?}");
    }
    for invalid in [-1, 4, i32::MIN, i32::MAX] {
        assert_eq!(DistanceUnit::from_raw(invalid), None);
    }
}

#[test]
fn filter_validation() {
    let unit = Some(DistanceUnit::Meters);
    assert_eq!(
        RadiusFilter::new(181.0, 91.0, -1.0, None).unwrap_err(),
        "Invalid GeoFilter unit"
    );
    for (name, lon, lat) in [
        ("east", 180.01, 0.0),
        ("west", -180.01, 0.0),
        ("north", 0.0, 90.01),
        ("south", 0.0, -90.01),
        ("NaN longitude", f64::NAN, 0.0),
        ("NaN latitude", 0.0, f64::NAN),
        ("infinite longitude", f64::INFINITY, 0.0),
        ("infinite latitude", 0.0, f64::NEG_INFINITY),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, -1.0, unit).unwrap_err(),
            "Invalid GeoFilter lat/lon",
            "{name}"
        );
    }
    for (name, radius) in [("NaN", f64::NAN), ("zero", 0.0), ("negative", -1.0)] {
        assert_eq!(
            RadiusFilter::new(0.0, 0.0, radius, unit).unwrap_err(),
            "Invalid GeoFilter radius",
            "{name}"
        );
    }
    for (lon, lat) in [(180.0, 90.0), (-180.0, -90.0)] {
        assert!(RadiusFilter::new(lon, lat, f64::INFINITY, unit).is_ok());
    }
}

#[test]
fn radius_boundary() {
    // Score zero decodes to the center of the southwest-most indexed cell.
    let lon = -180.0 + 180.0 / (1u64 << 26) as f64;
    let lat = -85.05112878 + 85.05112878 / (1u64 << 26) as f64;
    let center = geo::Coordinates::parse_geo(&format!("{lon},{lat}")).unwrap();
    // A point ten degrees north along the same meridian is R * angle away.
    let expected = 6_372_797.560856 * 10.0f64.to_radians();
    for unit in [
        DistanceUnit::Meters,
        DistanceUnit::Kilometers,
        DistanceUnit::Feet,
        DistanceUnit::Miles,
    ] {
        for (name, scale, included) in [("inside", 1.01, true), ("outside", 0.99, false)] {
            let filter = RadiusFilter::new(
                lon,
                lat + 10.0,
                expected * scale / unit.meters(),
                Some(unit),
            )
            .unwrap();
            let (actual, distance) = filter.contains(0.0);
            assert_eq!(actual, included, "{unit:?} {name}");
            assert!((distance - expected).abs() < 1e-8);
        }
    }
    let distance = geo::hash::haversine_distance(
        center.lon,
        decorum::R64::assert(lat + 10.0),
        center.lon,
        center.lat,
    );
    let (inside, actual) = RadiusFilter::new(lon, lat + 10.0, distance, Some(DistanceUnit::Meters))
        .unwrap()
        .contains(0.0);
    // Miri may round successive transcendental calls differently.
    assert!((actual - distance).abs() < 1e-8);
    assert_eq!(inside, actual <= distance);
}

// Proptest needs filesystem access unavailable under Miri.
#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn radius_is_monotone(lon in -180.0..180.0, lat in -90.0..90.0, score in 0u64..(1u64 << 52), radius in 1.0..2.0e7) {
        let small = RadiusFilter::new(lon, lat, radius, Some(DistanceUnit::Meters)).unwrap();
        let large = RadiusFilter::new(lon, lat, radius * 2.0, Some(DistanceUnit::Meters)).unwrap();
        let (inside, distance) = small.contains(score as f64);
        proptest::prop_assert_eq!(inside, distance <= radius);
        proptest::prop_assert!(!inside || large.contains(score as f64).0);
    }
}
