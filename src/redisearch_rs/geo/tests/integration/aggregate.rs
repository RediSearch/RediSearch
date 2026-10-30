/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::GeoValue::{Number, Other, String as Text};
use geo::hash::{GEO_STEP_MAX, WGS84Coordinates, align_52bits, encode_wgs84};
use geo::{DistanceArgument, GeoValue, ParseGeoError, geodistance};

#[test]
fn all_arities_and_three_argument_orientations_round_to_centimeters() {
    let expected = 166_274.26;
    for args in [
        vec![Text(b"13.361389,38.115556"), Text(b"15.087269,37.502669")],
        vec![
            Number(13.361389),
            Number(38.115556),
            Number(15.087269),
            Number(37.502669),
        ],
        vec![
            Text(b"13.361389"),
            Text(b"38.115556"),
            Text(b"15.087269,37.502669"),
        ],
        vec![
            Text(b"13.361389,38.115556"),
            Text(b"15.087269"),
            Text(b"37.502669"),
        ],
    ] {
        let result = geodistance(&args);
        assert_eq!(result.distance, expected);
        assert!(result.error.is_none());
    }
}

#[test]
fn numeric_fields_decode_scores_instead_of_converting_to_coordinates() {
    let score = |lon, lat| {
        align_52bits(encode_wgs84(
            WGS84Coordinates::from_f64(lon, lat).unwrap(),
            GEO_STEP_MAX,
        )) as f64
    };
    let result = geodistance(&[
        Number(score(13.361389, 38.115556)),
        Number(score(15.087269, 37.502669)),
    ]);
    assert_eq!(result.distance, 166_274.15);
    assert!(result.error.is_none());
    assert_eq!(geodistance(&[Number(0.0), Number(f64::NAN)]).distance, 0.0);
}

#[test]
fn field_parse_errors_return_nan_and_preserve_first_error() {
    for (args, expected) in [
        (
            vec![Text(b"\xff"), Text(b"missing")],
            ParseGeoError::InvalidUtf8,
        ),
        (
            vec![Text(b"0,0"), Text(b"missing")],
            ParseGeoError::MissingSeparator,
        ),
        (
            vec![Number(0.0), Number(0.0), Text(b"nan,0")],
            ParseGeoError::NotFinite,
        ),
        (
            vec![Text(b"inf,0"), Number(0.0), Number(0.0)],
            ParseGeoError::NotFinite,
        ),
    ] {
        let result = geodistance(&args);
        assert!(result.distance.is_nan());
        assert_eq!(result.error, Some(expected));
    }
    let result = geodistance(&[Text(b"x,0"), Number(0.0)]);
    assert!(result.distance.is_nan());
    assert!(matches!(result.error, Some(ParseGeoError::Invalid { .. })));
}

#[test]
fn conversion_failures_are_nan_without_query_errors_and_short_circuit() {
    for args in [
        vec![Other, Text(b"bad")],
        vec![Text(b"0,0"), Other],
        vec![Number(0.0), Other, Text(b"bad")],
        vec![Text(b"0,0"), Other, Number(0.0)],
        vec![Text(b"0,0"), Number(0.0), Other],
        vec![Other, Number(0.0), Number(0.0), Number(0.0)],
        vec![Number(0.0), Other, Number(0.0), Number(0.0)],
        vec![Number(0.0), Number(0.0), Other, Number(0.0)],
        vec![Number(0.0), Number(0.0), Number(0.0), Other],
    ] {
        let result = geodistance(&args);
        assert!(result.distance.is_nan());
        assert!(result.error.is_none());
    }
}

#[test]
fn nonfinite_numeric_coordinates_return_nan_but_out_of_range_finite_values_work() {
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let result = geodistance(&[Number(value), Number(0.0), Number(0.0), Number(0.0)]);
        assert!(result.distance.is_nan());
        assert!(result.error.is_none());
    }
    let result = geodistance(&[Number(181.0), Number(91.0), Number(181.0), Number(91.0)]);
    assert_eq!(result.distance, 0.0);
    assert!(result.error.is_none());
}

#[test]
fn three_argument_dispatch_uses_convertibility_not_number_variant_or_finiteness() {
    let result = geodistance(&[Text(b"NaN"), Number(0.0), Text(b"0,0")]);
    assert!(result.distance.is_nan());
    assert!(result.error.is_none());
    let result = geodistance(&[Text(b" 0"), Number(0.0), Text(b"0,0")]);
    assert!(result.distance.is_nan());
    assert!(result.error.is_some());
}

#[test]
fn numeric_conversion_and_field_views_remain_independent() {
    struct View {
        field: GeoValue<'static>,
        number: Option<f64>,
    }
    impl DistanceArgument for View {
        fn field(&self) -> GeoValue<'_> {
            self.field
        }
        fn to_number(&self) -> Option<f64> {
            self.number
        }
    }
    let args = [
        View {
            field: Other,
            number: Some(0.0),
        },
        View {
            field: Other,
            number: Some(0.0),
        },
        View {
            field: Text(b"0,0"),
            number: None,
        },
    ];
    assert_eq!(geodistance(&args).distance, 0.0);
    let result = geodistance(&args[..2]);
    assert!(result.distance.is_nan());
    assert!(result.error.is_none());
}

#[test]
#[should_panic(expected = "geodistance requires 2 to 4 arguments")]
fn arity_is_a_registry_precondition() {
    geodistance(&[Other]);
}
