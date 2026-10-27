/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{FilterParameters, GeoDistance, aggregate_distance};

#[test]
fn units_preserve_spelling_factors_and_buffer_semantics() {
    for (bytes, unit, factor) in [
        (b"KM".as_slice(), GeoDistance::Kilometers, 1000.0),
        (b"m", GeoDistance::Meters, 1.0),
        (b"Ft", GeoDistance::Feet, 0.3048),
        (b"mi", GeoDistance::Miles, 1609.34),
    ] {
        assert_eq!(GeoDistance::parse(bytes), unit);
        assert_eq!(GeoDistance::from_raw(unit as i32), unit);
        assert_eq!(unit.factor(), factor);
        assert_eq!(GeoDistance::parse(unit.as_c_str().to_bytes()), unit);
    }
    assert_eq!(
        GeoDistance::parse_buffer(b"km\0garbage"),
        GeoDistance::Kilometers
    );
    assert_eq!(
        GeoDistance::parse_buffer(b"km\0abcdefghijkl"),
        GeoDistance::Kilometers
    );
    assert_eq!(
        GeoDistance::parse_buffer(b"km\0abcdefghijklm"),
        GeoDistance::Invalid
    );
    for bytes in [b"".as_slice(), b"meters", b" km", b"m ", b"\xff"] {
        assert_eq!(GeoDistance::parse_buffer(bytes), GeoDistance::Invalid);
    }
    assert_eq!(GeoDistance::from_raw(42).as_c_str(), c"<badunit>");
}

#[test]
fn validation_preserves_order_and_nan_acceptance() {
    let valid = FilterParameters {
        lon: 180.0,
        lat: -90.0,
        radius: f64::INFINITY,
    };
    assert_eq!(valid.validate(), Ok(()));
    for parameters in [
        FilterParameters {
            lon: f64::NAN,
            ..valid
        },
        FilterParameters {
            lat: f64::NAN,
            ..valid
        },
        FilterParameters {
            radius: f64::NAN,
            ..valid
        },
    ] {
        assert_eq!(parameters.validate(), Ok(()));
    }
    for parameters in [
        FilterParameters {
            lon: 180.1,
            radius: 0.0,
            ..valid
        },
        FilterParameters { lat: 90.1, ..valid },
        FilterParameters {
            lon: f64::INFINITY,
            ..valid
        },
        FilterParameters {
            lat: f64::NEG_INFINITY,
            ..valid
        },
    ] {
        assert_eq!(parameters.validate(), Err("Invalid GeoFilter lat/lon"));
    }
    for radius in [0.0, -0.0, -1.0, f64::NEG_INFINITY] {
        assert_eq!(
            FilterParameters { radius, ..valid }.validate(),
            Err("Invalid GeoFilter radius")
        );
    }
}

#[test]
fn aggregate_rounds_to_centimeters_and_propagates_nonfinite_values() {
    assert_eq!(aggregate_distance(1.2, 4.5, 1.25, 4.5), 5544.17);
    assert_eq!(aggregate_distance(180.0, 90.0, 180.0, 90.0), 0.0);
    assert!(aggregate_distance(f64::NAN, 0.0, 0.0, 0.0).is_nan());
    assert!(aggregate_distance(0.0, f64::INFINITY, 0.0, 0.0).is_nan());
}
