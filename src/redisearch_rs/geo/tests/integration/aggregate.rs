/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{GEO_STEP_MAX, WGS84Coordinates, align_52bits, encode_wgs84};
use geo::{GeoDistanceError, GeoDistanceValue, ParseGeoError, geodistance};

enum Value<'a> {
    Number(f64),
    String(&'a [u8]),
    Other,
    Ref(&'a Value<'a>),
    Trio(&'a Value<'a>),
}

impl Value<'_> {
    fn dereference(&self) -> &Self {
        match self {
            Self::Ref(value) => value.dereference(),
            _ => self,
        }
    }
}

impl GeoDistanceValue for Value<'_> {
    fn geo_string(&self) -> Option<&[u8]> {
        match self.dereference() {
            Self::String(bytes) => Some(bytes),
            _ => None,
        }
    }
    fn geo_number(&self) -> Option<f64> {
        match self.dereference() {
            Self::Number(number) => Some(*number),
            _ => None,
        }
    }
    fn to_number(&self) -> Option<f64> {
        match self.dereference() {
            Self::Number(number) => Some(*number),
            Self::String(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
            Self::Trio(value) => value.to_number(),
            _ => None,
        }
    }
}

use Value::{Number, Other, Ref, String, Trio};

#[test]
fn coordinate_fields_and_raw_pairs_use_all_overloads() {
    let expected = geodistance(&[
        String(b"13.361389,38.115556"),
        String(b"15.087269 37.502669"),
    ])
    .unwrap();
    assert_eq!(expected, 166274.26);
    for values in [
        vec![
            Number(13.361389),
            Number(38.115556),
            Number(15.087269),
            Number(37.502669),
        ],
        vec![
            String(b"13.361389"),
            String(b"38.115556"),
            String(b"15.087269,37.502669"),
        ],
        vec![
            String(b"13.361389,38.115556"),
            String(b"15.087269"),
            String(b"37.502669"),
        ],
    ] {
        assert_eq!(geodistance(&values).unwrap(), expected);
    }
}

#[test]
fn numeric_fields_are_decoded_scores_but_numeric_strings_remain_fields() {
    let score = align_52bits(encode_wgs84(
        WGS84Coordinates::from_f64(13.361389, 38.115556).unwrap(),
        GEO_STEP_MAX,
    )) as f64;
    let point = Number(score);
    assert_eq!(geodistance(&[Ref(&point), Number(score)]), Ok(0.0));
    assert_eq!(
        geodistance(&[Number(0.0), Number(0.0), Number(score)]),
        geodistance(&[String(b"0,0"), Number(score)])
    );
    assert!(matches!(
        geodistance(&[String(b"123"), Number(score)]),
        Err(GeoDistanceError::Parse(ParseGeoError::MissingSeparator))
    ));
    assert_eq!(geodistance(&[Number(-1.0), Number(f64::NAN)]), Ok(0.0));
}

#[test]
fn trios_coerce_only_in_raw_coordinate_positions() {
    let lon = Number(1.0);
    assert_eq!(
        geodistance(&[Trio(&lon), Number(2.0), String(b"1,2")]),
        Ok(0.0)
    );
    assert_eq!(
        geodistance(&[Trio(&lon), String(b"1,2")]),
        Err(GeoDistanceError::Argument)
    );
    let field = String(b"1,2");
    assert_eq!(
        geodistance(&[Ref(&field), Number(1.0), Number(2.0)]),
        Ok(0.0)
    );
}

#[test]
fn parse_failures_are_distinct_from_numeric_conversion_failures() {
    for bytes in [
        b"\xff".as_slice(),
        b"missing",
        b"bad,1",
        b"1,NaN",
        b"1,2\0junk",
    ] {
        assert!(matches!(
            geodistance(&[String(bytes), String(b"0,0")]),
            Err(GeoDistanceError::Parse(_))
        ));
    }
    assert_eq!(
        geodistance(&[Other, String(b"0,0")]),
        Err(GeoDistanceError::Argument)
    );
    assert_eq!(
        geodistance(&[Number(0.0), Other, Number(1.0), Number(1.0)]),
        Err(GeoDistanceError::Argument)
    );
    assert_eq!(
        geodistance(&[String(b"0,0"), Number(1.0), Other]),
        Err(GeoDistanceError::Argument)
    );
    assert_eq!(
        geodistance(&[Number(0.0), Number(0.0), Other]),
        Err(GeoDistanceError::Argument)
    );
    assert!(matches!(
        geodistance(&[Number(0.0), Number(0.0), String(b"bad")]),
        Err(GeoDistanceError::Parse(_))
    ));
}

#[test]
fn evaluation_stops_at_first_error_in_c_order() {
    assert_eq!(
        geodistance(&[Other, String(b"bad")]),
        Err(GeoDistanceError::Argument)
    );
    assert_eq!(
        geodistance(&[Number(0.0), Other, String(b"bad")]),
        Err(GeoDistanceError::Argument)
    );
    assert!(matches!(
        geodistance(&[String(b"bad"), Other, Other]),
        Err(GeoDistanceError::Parse(_))
    ));
    assert_eq!(
        geodistance(&[Other, Other, String(b"bad"), Other]),
        Err(GeoDistanceError::Argument)
    );
}

#[test]
fn raw_nonfinite_coordinates_produce_nan_without_parse_error_or_range_validation() {
    for coordinate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        for position in 0..4 {
            let mut values = [Number(0.0), Number(0.0), Number(1.0), Number(1.0)];
            values[position] = Number(coordinate);
            assert!(geodistance(&values).unwrap().is_nan());
        }
        for values in [
            [Number(coordinate), Number(0.0), String(b"1,1")],
            [Number(0.0), Number(coordinate), String(b"1,1")],
            [String(b"0,0"), Number(coordinate), Number(1.0)],
            [String(b"0,0"), Number(1.0), Number(coordinate)],
        ] {
            assert!(geodistance(&values).unwrap().is_nan());
        }
    }
    for coordinate in [b"NaN".as_slice(), b"inf", b"-inf"] {
        for values in [
            [String(coordinate), String(b"0"), String(b"1,1")],
            [String(b"0"), String(coordinate), String(b"1,1")],
            [String(b"0,0"), String(coordinate), String(b"1")],
            [String(b"0,0"), String(b"1"), String(coordinate)],
        ] {
            assert!(geodistance(&values).unwrap().is_nan());
        }
    }
    for coordinate in [b"NaN,1".as_slice(), b"1,inf", b"1,-inf"] {
        for values in [
            vec![String(b"0,0"), String(coordinate)],
            vec![Number(0.0), Number(0.0), String(coordinate)],
        ] {
            assert_eq!(
                geodistance(&values),
                Err(GeoDistanceError::Parse(ParseGeoError::NotFinite))
            );
        }
    }
    assert_eq!(
        geodistance(&[Number(181.0), Number(91.0), Number(181.0), Number(91.0)]),
        Ok(0.0)
    );
    assert_eq!(
        geodistance(&[String(b"181,91"), String(b"181,91")]),
        Ok(0.0)
    );
}

#[test]
fn centimeter_rounding_uses_nearest_not_truncation() {
    use decorum::R64;
    let meters_per_degree = geo::hash::haversine_distance(
        R64::assert(0.0),
        R64::assert(0.0),
        R64::assert(1.0),
        R64::assert(0.0),
    );
    for (meters, expected) in [
        (0.0049, 0.0),
        (0.0051, 0.01),
        (0.0149, 0.01),
        (0.0151, 0.02),
    ] {
        assert_eq!(
            geodistance(&[
                Number(0.0),
                Number(0.0),
                Number(meters / meters_per_degree),
                Number(0.0)
            ]),
            Ok(expected)
        );
    }
}

#[test]
#[should_panic(expected = "geodistance requires two to four arguments")]
fn argument_count_is_enforced_by_caller() {
    geodistance(&[Other]).unwrap();
}
