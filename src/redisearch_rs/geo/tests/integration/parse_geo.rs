/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{ParseGeoError, parse_geo};

#[test]
fn valid_comma_separated() {
    let (lon, lat) = parse_geo("1.5,2.5").unwrap();
    assert_eq!(lon, 1.5);
    assert_eq!(lat, 2.5);
}

#[test]
fn valid_space_separated() {
    let (lon, lat) = parse_geo("1.5 2.5").unwrap();
    assert_eq!(lon, 1.5);
    assert_eq!(lat, 2.5);
}

#[test]
fn comma_space_separated() {
    let (lon, lat) = parse_geo("1.23, 4.56").unwrap();
    assert_eq!(lon, 1.23);
    assert_eq!(lat, 4.56);
}

#[test]
fn comma_space_separated_geo_coords() {
    let (lon, lat) = parse_geo("29.69465, 34.95126").unwrap();
    assert_eq!(lon, 29.69465);
    assert_eq!(lat, 34.95126);
}

#[test]
fn negative_coordinates() {
    let (lon, lat) = parse_geo("-122.4194,37.7749").unwrap();
    assert_eq!(lon, -122.4194);
    assert_eq!(lat, 37.7749);
}

#[test]
fn integer_coordinates() {
    let (lon, lat) = parse_geo("10,20").unwrap();
    assert_eq!(lon, 10.0);
    assert_eq!(lat, 20.0);
}

#[test]
fn no_separator() {
    assert_eq!(parse_geo("aaaa"), Err(ParseGeoError::Invalid));
}

#[test]
fn non_numeric() {
    assert_eq!(parse_geo("abc,def"), Err(ParseGeoError::Invalid));
}

#[test]
fn partial_non_numeric() {
    assert_eq!(parse_geo("1.0,abc"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("abc,1.0"), Err(ParseGeoError::Invalid));
}

#[test]
fn empty_string() {
    assert_eq!(parse_geo(""), Err(ParseGeoError::Invalid));
}

#[test]
fn trailing_text() {
    assert_eq!(parse_geo("1.0,2.0abc"), Err(ParseGeoError::Invalid));
}

#[test]
fn leading_text() {
    assert_eq!(parse_geo("abc1.0,2.0"), Err(ParseGeoError::Invalid));
}

#[test]
fn scientific_notation() {
    let (lon, lat) = parse_geo("1e2,2e3").unwrap();
    assert_eq!(lon, 100.0);
    assert_eq!(lat, 2000.0);
}

#[test]
fn nan_rejected() {
    assert_eq!(parse_geo("NaN,1.0"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("1.0,NaN"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("NaN,NaN"), Err(ParseGeoError::Invalid));
}

#[test]
fn infinity_rejected() {
    assert_eq!(parse_geo("inf,1.0"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("1.0,inf"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("-inf,1.0"), Err(ParseGeoError::Invalid));
    assert_eq!(parse_geo("infinity,1.0"), Err(ParseGeoError::Invalid));
}

#[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
mod proptests {
    use geo::parse_geo;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip_comma(lon in -180.0f64..=180.0, lat in -90.0f64..=90.0) {
            let s = format!("{lon},{lat}");
            let result = parse_geo(&s);
            prop_assert!(result.is_ok(), "failed to parse: {s}");
            let (parsed_lon, parsed_lat) = result.unwrap();
            prop_assert_eq!(parsed_lon, lon);
            prop_assert_eq!(parsed_lat, lat);
        }

        #[test]
        fn roundtrip_space(lon in -180.0f64..=180.0, lat in -90.0f64..=90.0) {
            let s = format!("{lon} {lat}");
            let result = parse_geo(&s);
            prop_assert!(result.is_ok(), "failed to parse: {s}");
            let (parsed_lon, parsed_lat) = result.unwrap();
            prop_assert_eq!(parsed_lon, lon);
            prop_assert_eq!(parsed_lat, lat);
        }
    }
}
