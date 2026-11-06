/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{GeoDistance, GeoFilterError, LegacyGeoArgs, LegacyGeoFilter, LegacyGeoParseError};
use rqe_core::RS_INVALID_FIELD_INDEX;

const CONVERSION_ERROR: &str = "Could not convert argument to expected type";

struct Args<'a> {
    bytes: &'a [&'a [u8]],
    conversions: &'a [Result<f64, &'static str>],
    unreadable: Option<usize>,
}

impl LegacyGeoArgs for Args<'_> {
    fn len(&self) -> usize {
        self.bytes.len()
    }

    fn string(&self, index: usize) -> Result<&[u8], &'static str> {
        if self.unreadable == Some(index) {
            Err("Expected an argument, but none provided")
        } else {
            Ok(self.bytes[index])
        }
    }

    fn number(&self, index: usize) -> Result<f64, &'static str> {
        self.conversions[index]
    }
}

#[test]
fn success_consumes_only_filter_and_owns_truncated_field() {
    let bytes: &[&[u8]] = &[
        b"prefix",
        b"field\0ignored",
        b"181",
        b"90",
        b"-1",
        b"Mi\0ignored",
        b"suffix",
    ];
    let args = Args {
        bytes,
        conversions: &[Ok(0.0), Ok(0.0), Ok(181.0), Ok(90.0), Ok(-1.0)],
        unreadable: None,
    };
    let mut offset = 1;
    let mut empty = true;
    let filter = LegacyGeoFilter::parse(&args, &mut offset, &mut empty).unwrap();
    assert_eq!(offset, 6);
    assert!(empty);
    assert_eq!(filter.field, b"field");
    assert_ne!(filter.field.as_ptr(), bytes[1].as_ptr());
    assert_eq!(filter.base.field_index, RS_INVALID_FIELD_INDEX);
    assert_eq!(
        (filter.base.lon, filter.base.lat, filter.base.radius),
        (181.0, 90.0, -1.0)
    );
    assert_eq!(filter.base.unit, GeoDistance::Miles);
    assert_eq!(filter.base.validate(), Err(GeoFilterError::Coordinates));
}

#[test]
fn insufficient_arguments_leave_cursor_and_flag_untouched() {
    let args = Args {
        bytes: &[b"field", b"0", b"0", b"1"],
        conversions: &[],
        unreadable: None,
    };
    assert!(!args.is_empty());
    let mut offset = 0;
    let mut empty = false;
    let error = LegacyGeoFilter::parse(&args, &mut offset, &mut empty).unwrap_err();
    assert_eq!(error, LegacyGeoParseError::MissingArguments);
    assert_eq!(error.to_string(), "GEOFILTER requires 5 arguments");
    assert_eq!(offset, 0);
    assert!(!empty);
}

#[test]
fn unreadable_field_does_not_advance() {
    let args = Args {
        bytes: &[b"", b"", b"", b"", b""],
        conversions: &[],
        unreadable: Some(0),
    };
    let mut offset = 0;
    let error = LegacyGeoFilter::parse(&args, &mut offset, &mut false).unwrap_err();
    assert_eq!(
        error.to_string(),
        "Bad arguments for <geo property>: Expected an argument, but none provided"
    );
    assert_eq!(offset, 0);
}

#[test]
fn numeric_failures_preserve_position_label_and_prior_empty_flag() {
    for (index, label) in [(1, "lon"), (2, "lat"), (3, "radius")] {
        let mut bytes: [&[u8]; 5] = [b"field", b"", b"", b"", b"km"];
        bytes[index] = b"bad";
        let args = Args {
            bytes: &bytes,
            conversions: &[Err(CONVERSION_ERROR); 5],
            unreadable: None,
        };
        let mut offset = 0;
        let mut empty = false;
        let error = LegacyGeoFilter::parse(&args, &mut offset, &mut empty).unwrap_err();
        assert_eq!(
            error,
            LegacyGeoParseError::BadArgument {
                argument: label,
                reason: CONVERSION_ERROR
            }
        );
        assert_eq!(
            error.to_string(),
            format!("Bad arguments for <{label}>: {CONVERSION_ERROR}")
        );
        assert_eq!(offset, index);
        assert_eq!(empty, index > 1);
    }
}

#[test]
fn empty_numeric_tokens_coalesce_only_after_conversion_failure() {
    let bytes: &[&[u8]] = &[b"field", b"", b"", b"", b"km"];
    let mut args = Args {
        bytes,
        conversions: &[Err(CONVERSION_ERROR); 5],
        unreadable: None,
    };
    let mut offset = 0;
    let mut empty = false;
    let filter = LegacyGeoFilter::parse(&args, &mut offset, &mut empty).unwrap();
    assert_eq!(
        (filter.base.lon, filter.base.lat, filter.base.radius),
        (0.0, 0.0, 0.0)
    );
    assert!(empty);
    assert_eq!(offset, 5);
    assert_eq!(filter.base.validate(), Err(GeoFilterError::Radius));

    // Internal char cursors can successfully convert empty input via strtod;
    // that path must not set the flag used for dialect rejection.
    args.conversions = &[Ok(0.0); 5];
    offset = 0;
    empty = false;
    LegacyGeoFilter::parse(&args, &mut offset, &mut empty).unwrap();
    assert!(!empty);
}

#[test]
fn logical_length_not_c_string_length_controls_empty_coalescing() {
    let args = Args {
        bytes: &[b"field", b"\0", b"1", b"1", b"km"],
        conversions: &[Err(CONVERSION_ERROR); 5],
        unreadable: None,
    };
    let mut offset = 0;
    let mut empty = false;
    assert!(matches!(
        LegacyGeoFilter::parse(&args, &mut offset, &mut empty),
        Err(LegacyGeoParseError::BadArgument {
            argument: "lon",
            ..
        })
    ));
    assert_eq!(offset, 1);
    assert!(!empty);
}

#[test]
fn unit_failure_advances_and_preserves_raw_error_bytes() {
    let args = Args {
        bytes: &[b"field", b"0", b"0", b"1", b"bogus\0ignored"],
        conversions: &[Ok(1.0); 5],
        unreadable: None,
    };
    let mut offset = 0;
    let error = LegacyGeoFilter::parse(&args, &mut offset, &mut false).unwrap_err();
    assert_eq!(error, LegacyGeoParseError::UnknownUnit(b"bogus".to_vec()));
    assert_eq!(error.to_string(), "Unknown distance unit bogus");
    assert_eq!(offset, 5);
}

#[test]
fn legacy_units_do_not_have_the_bounded_parser_length_limit() {
    let args = Args {
        bytes: &[
            b"field",
            b"0",
            b"0",
            b"1",
            b"km\0a suffix longer than sixteen bytes",
        ],
        conversions: &[Ok(1.0); 5],
        unreadable: None,
    };
    let filter = LegacyGeoFilter::parse(&args, &mut 0, &mut false).unwrap();
    assert_eq!(filter.base.unit, GeoDistance::Kilometers);
}

#[test]
fn empty_coalescing_does_not_mask_string_access_errors() {
    let args = Args {
        bytes: &[b"field", b"", b"0", b"1", b"km"],
        conversions: &[Err(CONVERSION_ERROR); 5],
        unreadable: Some(1),
    };
    let mut offset = 0;
    assert!(matches!(
        LegacyGeoFilter::parse(&args, &mut offset, &mut false),
        Err(LegacyGeoParseError::BadArgument {
            argument: "lon",
            reason: CONVERSION_ERROR
        })
    ));
    assert_eq!(offset, 1);
}
