/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{GeoDistance, GeoFilter, LegacyGeoArgs, LegacyGeoFilter, LegacyGeoParseError};

struct Args<'a> {
    values: &'a [&'a [u8]],
    offset: usize,
    convert: fn(&[u8]) -> Option<f64>,
}

fn decimal(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

impl<'a> Args<'a> {
    fn new(values: &'a [&'a [u8]]) -> Self {
        Self {
            values,
            offset: 0,
            convert: decimal,
        }
    }
}

impl<'a> LegacyGeoArgs<'a> for Args<'a> {
    fn remaining(&self) -> usize {
        self.values.len() - self.offset
    }
    fn current(&self) -> &'a [u8] {
        self.values[self.offset]
    }
    fn current_double(&self) -> Option<f64> {
        (self.convert)(self.current())
    }
    fn advance(&mut self) {
        self.offset += 1;
    }
}

#[test]
fn missing_arguments_do_not_consume_or_clear_empty_flag() {
    let mut args = Args::new(&[b"field", b"1", b"2", b"3"]);
    let mut has_empty = true;
    let error = LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap_err();
    assert_eq!(error, LegacyGeoParseError::MissingArguments);
    assert_eq!(error.to_string(), "GEOFILTER requires 5 arguments");
    assert_eq!(args.offset, 0);
    assert!(has_empty);
}

#[test]
fn legacy_parse_owns_field_truncates_nul_and_leaves_extra_arguments() {
    let mut args = Args::new(&[
        b"field\0tail",
        b"181",
        b"91",
        b"-1",
        b"mI\0tail that exceeds 16 bytes",
        b"next",
    ]);
    let mut has_empty = false;
    let filter = LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap();
    assert_eq!(filter.field.as_ref(), b"field");
    assert_eq!(
        filter.base,
        GeoFilter {
            lon: 181.0,
            lat: 91.0,
            radius: -1.0,
            unit: GeoDistance::Miles
        }
    );
    assert_eq!(args.offset, 5);
    assert_eq!(args.current(), b"next");
    assert!(!has_empty);
}

#[test]
fn empty_numeric_failures_coalesce_and_preserve_progress_on_later_failure() {
    let mut args = Args::new(&[b"field", b"", b"", b"", b"km"]);
    let mut has_empty = false;
    let filter = LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap();
    assert_eq!(filter.base, GeoFilter::new(0.0, 0.0, 0.0, None));
    assert!(has_empty);
    let mut args = Args::new(&[b"field", b"", b"bad", b"1", b"km"]);
    has_empty = false;
    assert_eq!(
        LegacyGeoFilter::parse(&mut args, &mut has_empty),
        Err(LegacyGeoParseError::Latitude)
    );
    assert_eq!(args.offset, 2);
    assert!(has_empty);
}

#[test]
fn successful_empty_conversion_does_not_mark_empty_filter_value() {
    let mut args = Args::new(&[b"field", b"", b"", b"", b"m"]);
    args.convert = |_| Some(0.0);
    let mut has_empty = false;
    LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap();
    assert!(!has_empty);
}

#[test]
fn conversion_failures_leave_offending_number_current() {
    for (position, error, label) in [
        (1, LegacyGeoParseError::Longitude, "lon"),
        (2, LegacyGeoParseError::Latitude, "lat"),
        (3, LegacyGeoParseError::Radius, "radius"),
    ] {
        let mut values: [&[u8]; 5] = [b"field", b"1", b"2", b"3", b"m"];
        values[position] = b"bad";
        let mut args = Args::new(&values);
        let mut has_empty = false;
        let actual = LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap_err();
        assert_eq!(actual, error);
        assert_eq!(
            actual.to_string(),
            format!("Bad arguments for <{label}>: Could not convert argument to expected type")
        );
        assert_eq!(args.offset, position);
        assert!(!has_empty);
    }
}

#[test]
fn unknown_unit_is_consumed_and_retains_c_string_error() {
    let mut args = Args::new(&[b"field", b"1", b"2", b"3", b"yards\0ignored"]);
    let mut has_empty = false;
    let error = LegacyGeoFilter::parse(&mut args, &mut has_empty).unwrap_err();
    assert_eq!(
        error,
        LegacyGeoParseError::UnknownUnit(b"yards".as_slice().into())
    );
    assert_eq!(error.to_string(), "Unknown distance unit yards");
    assert_eq!(args.offset, 5);
}

#[test]
fn legacy_parser_retains_cursor_specific_conversion_results() {
    let mut args = Args::new(&[
        b"field",
        b"adapter-specific",
        b"adapter-specific",
        b"adapter-specific",
        b"m",
    ]);
    args.convert = |_| Some(f64::NAN);
    let filter = LegacyGeoFilter::parse(&mut args, &mut false).unwrap();
    assert!(filter.base.lon.is_nan());
    assert!(filter.base.lat.is_nan());
    assert!(filter.base.radius.is_nan());
    assert_eq!(filter.base.validate(), Ok(()));
}
