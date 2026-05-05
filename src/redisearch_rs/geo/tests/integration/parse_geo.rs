/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{Coordinates, ParseGeoError};

/// Mirror of the private `MAX_GEO_STRING_LEN` constant in the `geo` crate.
const MAX_GEO_STRING_LEN: usize = 128;

#[test]
fn valid_comma_separated() {
    let coords = Coordinates::parse_geo("1.5,2.5").unwrap();
    assert_eq!(coords.lon, 1.5);
    assert_eq!(coords.lat, 2.5);
}

#[test]
fn valid_space_separated() {
    let coords = Coordinates::parse_geo("1.5 2.5").unwrap();
    assert_eq!(coords.lon, 1.5);
    assert_eq!(coords.lat, 2.5);
}

#[test]
fn comma_space_separated() {
    let coords = Coordinates::parse_geo("1.23, 4.56").unwrap();
    assert_eq!(coords.lon, 1.23);
    assert_eq!(coords.lat, 4.56);
}

#[test]
fn comma_space_separated_geo_coords() {
    let coords = Coordinates::parse_geo("29.69465, 34.95126").unwrap();
    assert_eq!(coords.lon, 29.69465);
    assert_eq!(coords.lat, 34.95126);
}

#[test]
fn negative_coordinates() {
    let coords = Coordinates::parse_geo("-122.4194,37.7749").unwrap();
    assert_eq!(coords.lon, -122.4194);
    assert_eq!(coords.lat, 37.7749);
}

#[test]
fn integer_coordinates() {
    let coords = Coordinates::parse_geo("10,20").unwrap();
    assert_eq!(coords.lon, 10.0);
    assert_eq!(coords.lat, 20.0);
}

#[test]
fn too_long() {
    let s = "1".repeat(MAX_GEO_STRING_LEN + 1);
    assert_eq!(Coordinates::parse_geo(&s), Err(ParseGeoError::TooLong));
}

#[test]
fn exactly_max_length_valid() {
    // "1.0," is 4 chars, pad lon part to fill up to MAX_GEO_STRING_LEN
    let lon_part = "1".repeat(MAX_GEO_STRING_LEN - 4);
    let s = format!("{lon_part},1.0");
    assert!(s.len() == MAX_GEO_STRING_LEN);
    let result = Coordinates::parse_geo(&s);
    // The 124-digit number is finite (~1.11e+123), so parsing succeeds.
    assert!(result.is_ok(), "expected Ok, got {result:?}");
}

#[test]
fn no_separator() {
    assert_eq!(
        Coordinates::parse_geo("aaaa"),
        Err(ParseGeoError::MissingSeparator)
    );
}

#[test]
fn non_numeric() {
    assert!(matches!(
        Coordinates::parse_geo("abc,def"),
        Err(ParseGeoError::Invalid { .. })
    ));
}

#[test]
fn partial_non_numeric() {
    assert!(matches!(
        Coordinates::parse_geo("1.0,abc"),
        Err(ParseGeoError::Invalid { .. })
    ));
    assert!(matches!(
        Coordinates::parse_geo("abc,1.0"),
        Err(ParseGeoError::Invalid { .. })
    ));
}

#[test]
fn empty_string() {
    assert_eq!(
        Coordinates::parse_geo(""),
        Err(ParseGeoError::MissingSeparator)
    );
}

#[test]
fn trailing_text() {
    assert!(matches!(
        Coordinates::parse_geo("1.0,2.0abc"),
        Err(ParseGeoError::Invalid { .. })
    ));
}

#[test]
fn leading_text() {
    assert!(matches!(
        Coordinates::parse_geo("abc1.0,2.0"),
        Err(ParseGeoError::Invalid { .. })
    ));
}

#[test]
fn scientific_notation() {
    let coords = Coordinates::parse_geo("1e2,2e3").unwrap();
    assert_eq!(coords.lon, 100.0);
    assert_eq!(coords.lat, 2000.0);
}

#[test]
fn nan_rejected() {
    assert_eq!(
        Coordinates::parse_geo("NaN,1.0"),
        Err(ParseGeoError::NotFinite)
    );
    assert_eq!(
        Coordinates::parse_geo("1.0,NaN"),
        Err(ParseGeoError::NotFinite)
    );
    assert_eq!(
        Coordinates::parse_geo("NaN,NaN"),
        Err(ParseGeoError::NotFinite)
    );
}

#[test]
fn infinity_rejected() {
    assert_eq!(
        Coordinates::parse_geo("inf,1.0"),
        Err(ParseGeoError::NotFinite)
    );
    assert_eq!(
        Coordinates::parse_geo("1.0,inf"),
        Err(ParseGeoError::NotFinite)
    );
    assert_eq!(
        Coordinates::parse_geo("-inf,1.0"),
        Err(ParseGeoError::NotFinite)
    );
    assert_eq!(
        Coordinates::parse_geo("infinity,1.0"),
        Err(ParseGeoError::NotFinite)
    );
}

#[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
mod proptests {
    use geo::Coordinates;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_comma(lon in -180.0f64..=180.0, lat in -90.0f64..=90.0) {
            let s = format!("{lon},{lat}");
            let result = Coordinates::parse_geo(&s);
            prop_assert!(result.is_ok(), "failed to parse: {s}");
            let coords = result.unwrap();
            prop_assert_eq!(coords.lon, lon);
            prop_assert_eq!(coords.lat, lat);
        }

        #[test]
        fn roundtrip_space(lon in -180.0f64..=180.0, lat in -90.0f64..=90.0) {
            let s = format!("{lon} {lat}");
            let result = Coordinates::parse_geo(&s);
            prop_assert!(result.is_ok(), "failed to parse: {s}");
            let coords = result.unwrap();
            prop_assert_eq!(coords.lon, lon);
            prop_assert_eq!(coords.lat, lat);
        }
    }
}
