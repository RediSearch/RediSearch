/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{RadiusFilter, Unit};

#[test]
fn units() {
    for (name, spelling, unit, meters) in [
        ("kilometers", c"km", Unit::Kilometers, 1000.0),
        ("meters", c"m", Unit::Meters, 1.0),
        ("feet", c"ft", Unit::Feet, 0.3048),
        ("miles", c"mi", Unit::Miles, 1609.34),
    ] {
        assert_eq!(Unit::parse(spelling.to_bytes()), Some(unit), "{name}");
        assert_eq!(
            Unit::parse(&spelling.to_bytes().to_ascii_uppercase()),
            Some(unit),
            "{name}"
        );
        assert_eq!(unit.as_c_str(), spelling, "{name}");
        assert_eq!(Unit::from_raw(unit as i32), Some(unit), "{name}");
        assert_eq!(unit.meters(), meters, "{name}");
        let filter = RadiusFilter::new(0.0, 0.0, 2.0, Some(unit)).unwrap();
        assert!(filter.contains(2.0 * meters), "{name}");
        assert!(!filter.contains(2.0 * meters + 0.001), "{name}");
    }
    for (name, input, expected) in [
        ("empty", b"".as_slice(), None),
        ("unknown", b"yd", None),
        ("whitespace", b"m ", None),
        ("binary", b"\xff", None),
        ("nul", b"m\0junk", Some(Unit::Meters)),
        ("maximum buffer", b"m\0xxxxxxxxxxxxx", Some(Unit::Meters)),
        ("oversized buffer", b"m\0xxxxxxxxxxxxxx", None),
    ] {
        assert_eq!(Unit::parse(input), expected, "{name}");
    }
    for invalid in [-1, i32::MIN, 4, i32::MAX] {
        assert_eq!(Unit::from_raw(invalid), None);
    }
}

#[test]
fn validation() {
    assert_eq!(
        RadiusFilter::new(181.0, 91.0, -1.0, None).unwrap_err(),
        "Invalid GeoFilter unit"
    );
    for (name, lon, lat) in [
        ("longitude high", 180.001, 0.0),
        ("longitude low", -180.001, 0.0),
        ("latitude high", 0.0, 90.001),
        ("latitude low", 0.0, -90.001),
        ("nan longitude", f64::NAN, 0.0),
        ("nan latitude", 0.0, f64::NAN),
        ("infinite longitude", f64::INFINITY, 0.0),
        ("infinite latitude", 0.0, f64::NEG_INFINITY),
    ] {
        assert_eq!(
            RadiusFilter::new(lon, lat, -1.0, Some(Unit::Meters)).unwrap_err(),
            "Invalid GeoFilter lat/lon",
            "{name}"
        );
    }
    for (name, radius) in [("zero", 0.0), ("negative", -1.0), ("nan", f64::NAN)] {
        assert_eq!(
            RadiusFilter::new(0.0, 0.0, radius, Some(Unit::Meters)).unwrap_err(),
            "Invalid GeoFilter radius",
            "{name}"
        );
    }
    for (lon, lat) in [(180.0, 90.0), (-180.0, -90.0)] {
        assert!(RadiusFilter::new(lon, lat, f64::INFINITY, Some(Unit::Meters)).is_ok());
    }
}

#[test]
fn radius_boundary() {
    // Redis' Palermo score; the center is Catania. The expected great-circle
    // distance is independent of the radius filtering implementation.
    let filter = RadiusFilter::new(15.087269, 37.502669, 166_275.0, Some(Unit::Meters)).unwrap();
    let distance = filter.distance(3479099956230698.0);
    assert!((distance - 166_274.0).abs() < 1.0, "{distance}");
    assert!(filter.contains(distance));
    assert!(!filter.contains(166_275.001));
    assert!(filter.contains(166_275.0));
    assert!(!filter.contains(f64::NAN));
}

// Proptest accesses the filesystem, unavailable under Miri isolation.
#[cfg(not(miri))]
proptest::proptest! {
    #[test]
    fn radius_membership(radius in 0.001f64..1.0e7, distance in 0.0f64..2.0e7) {
        let filter = RadiusFilter::new(12.0, -34.0, radius, Some(Unit::Meters)).unwrap();
        proptest::prop_assert_eq!(filter.contains(distance), distance <= radius);
    }
}
