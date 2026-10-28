/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CStr, ptr, sync::Once};

use ffi::{ArgsCursor, GeoFilter, LegacyGeoFilter};
use geo::hash;
use geo_index_ffi::*;
use query_error::{QueryError, QueryErrorCode};
use redisearch_rs as _;
use rqe_core::RS_INVALID_FIELD_INDEX;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Install the string APIs once, before any test can read them.
fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(redis_mock::init_redis_module_mock);
}

#[test]
fn unit_buffers_and_c_discriminants() {
    for (text, expected, canonical) in [
        (c"kM", 0, c"km"),
        (c"M", 1, c"m"),
        (c"fT", 2, c"ft"),
        (c"Mi", 3, c"mi"),
        (c"", -1, c"<badunit>"),
        (c" km", -1, c"<badunit>"),
        (c"km ", -1, c"<badunit>"),
        (c"meters", -1, c"<badunit>"),
        (c"\xff", -1, c"<badunit>"),
    ] {
        // SAFETY: text is NUL-terminated and readable for this call.
        assert_eq!(unsafe { RustGeoDistance_Parse(text.as_ptr()) }, expected);
        // SAFETY: text covers the stated byte length.
        let parsed = unsafe { RustGeoDistance_Parse_Buffer(text.as_ptr(), text.count_bytes()) };
        assert_eq!(parsed, expected);
        // SAFETY: ToString returns a static NUL-terminated string.
        let name = unsafe { CStr::from_ptr(RustGeoDistance_ToString(expected)) };
        assert_eq!(name, canonical);
    }
    for (bytes, expected) in [
        (b"km\0garbage".as_slice(), 0),
        (b"km\0xxxxxxxxxxxx".as_slice(), 0),
        (b"km\0xxxxxxxxxxxxx".as_slice(), -1),
    ] {
        // SAFETY: bytes covers the stated length.
        let parsed = unsafe { RustGeoDistance_Parse_Buffer(bytes.as_ptr().cast(), bytes.len()) };
        assert_eq!(parsed, expected);
    }
    for len in [0, 16] {
        assert_eq!(
            // SAFETY: these lengths do not access the pointer, per the contract.
            unsafe { RustGeoDistance_Parse_Buffer(ptr::null(), len) },
            -1
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Calls C numeric-filter construction and destruction")]
fn construction_field_index_and_mixed_allocator_cleanup() {
    setup();
    for (unit, len, expected) in [
        (ptr::null(), 99, 0),
        (c"".as_ptr(), 0, -1),
        (c"FT".as_ptr(), 2, 2),
    ] {
        // SAFETY: the Redis allocator mock is installed; unit buffers are live.
        let filter = unsafe { RustNewGeoFilter(31.5, -24.75, 12.25, unit, len) };
        // SAFETY: the constructor returned a fresh, exclusively owned filter.
        let filter = unsafe { &mut *filter };
        assert_eq!(filter.unitType, expected);
        assert_eq!(filter.fieldIndex, RS_INVALID_FIELD_INDEX);
        assert!(filter.numericFilters.is_null());
        assert_eq!(
            (filter.lon, filter.lat, filter.radius),
            (31.5, -24.75, 12.25)
        );
        // SAFETY: FieldSpec is a C layout of scalars, raw pointers and a union;
        // all zero bits are valid. No other field is accessed by SetField.
        let mut field: ffi::FieldSpec = unsafe { std::mem::zeroed() };
        field.index = 7;
        // SAFETY: filter and field are live and disjoint.
        unsafe { RustGeoFilter_SetField(filter, &field) };
        field.index = 11;
        assert_eq!(filter.fieldIndex, 7);
        // SAFETY: filter is exclusively borrowed; a null field is supported.
        unsafe { RustGeoFilter_SetField(filter, ptr::null()) };
        assert_eq!(filter.fieldIndex, RS_INVALID_FIELD_INDEX);
        if expected != -1 {
            // SAFETY: numericFilters is null; the resulting array is handed to
            // RustGeoFilter_Free below after the range borrows have ended.
            let ranges = unsafe { rqe_iterators::build_geo_numeric_filters(filter, 7) }.unwrap();
            assert!(!ranges.is_empty());
            assert!(!filter.numericFilters.is_null());
        }
        // SAFETY: this is the original Redis allocation with no remaining
        // borrowers, and any numericFilters came from the real range builder.
        unsafe { RustGeoFilter_Free(filter) };
    }
}

#[test]
fn validation_messages_and_radius_output() {
    let mut gf = GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lon: 181.0,
        lat: 90.0,
        radius: 0.0,
        unitType: -1,
        numericFilters: ptr::null_mut(),
    };
    let mut error = QueryError::default();
    for (unit, lon, radius, expected) in [
        (-1, 181.0, 0.0, "Invalid GeoFilter unit"),
        (0, 181.0, 0.0, "Invalid GeoFilter lat/lon"),
        (0, 180.0, 0.0, "Invalid GeoFilter radius"),
    ] {
        gf.unitType = unit;
        gf.lon = lon;
        gf.radius = radius;
        assert_eq!(
            // SAFETY: the filter and error are initialized, live, and disjoint.
            unsafe { RustGeoFilter_Validate(&gf, error.as_opaque_mut_ptr()) },
            0
        );
        assert_eq!(error.code(), QueryErrorCode::Syntax);
        assert_eq!(
            error.public_message().unwrap().to_bytes(),
            expected.as_bytes()
        );
        assert_eq!(
            error.private_message().unwrap().to_bytes(),
            format!("SEARCH_SYNTAX {expected}").as_bytes()
        );
        error.clear();
    }
    error.set_error(QueryErrorCode::ParseArgs, "first error");
    assert_eq!(
        // SAFETY: the filter and error are initialized, live, and disjoint.
        unsafe { RustGeoFilter_Validate(&gf, error.as_opaque_mut_ptr()) },
        0
    );
    assert_eq!(error.public_message(), Some(c"first error"));
    error.clear();
    gf.radius = f64::INFINITY;
    for (lon, lat) in [(180.0, 90.0), (-180.0, -90.0), (f64::NAN, f64::NAN)] {
        gf.lon = lon;
        gf.lat = lat;
        assert_eq!(
            // SAFETY: the filter and error are live and disjoint; validation
            // permits non-finite numbers without executing them.
            unsafe { RustGeoFilter_Validate(&gf, error.as_opaque_mut_ptr()) },
            1
        );
        assert!(error.is_ok());
    }
    let hash = hash::encode_wgs84(
        hash::WGS84Coordinates::from_f64(12.25, -33.5).unwrap(),
        hash::GEO_STEP_MAX,
    );
    let score = hash::align_52bits(hash) as f64;
    let (lon, lat) = hash::decode_to_lon_lat(hash);
    gf.lon = lon.into_inner();
    gf.lat = lat.into_inner();
    gf.radius = 0.0;
    let mut distance = -1.0;
    // SAFETY: the filter and distance output are live and disjoint.
    assert_eq!(unsafe { RustIsWithinRadius(&gf, score, &mut distance) }, 1);
    assert_eq!(distance, 0.0);
    gf.radius = -f64::MIN_POSITIVE;
    assert_eq!(
        // SAFETY: the filter is live and the output pointer may be null.
        unsafe { RustIsWithinRadius(&gf, score, ptr::null_mut()) },
        0
    );
    gf.lon = 12.0;
    gf.lat = -33.25;
    gf.radius = 1.0;
    // SAFETY: the filter and distance output are live and disjoint.
    assert_eq!(unsafe { RustIsWithinRadius(&gf, score, &mut distance) }, 0);
    // Independent geographic scale: about 36 km, not 36 m or 36 million m.
    assert!((35_000.0..37_000.0).contains(&distance), "{distance}");
    for (radius, expected) in [(1.0, 0), (100.0, 1)] {
        gf.radius = radius;
        // Miri's -Zmiri-recursive-validation also catches creating a reference
        // to this output before initialization, even without reading it.
        let mut output = std::mem::MaybeUninit::<f64>::uninit();
        // SAFETY: the filter is initialized and disjoint from the aligned,
        // exclusively writable output, which need not be initialized.
        let found = unsafe { RustIsWithinRadius(&gf, score, output.as_mut_ptr()) };
        assert_eq!(found, expected);
        // SAFETY: a non-null output is initialized on both a hit and a miss.
        let meters = unsafe { output.assume_init() };
        assert!((35_000.0..37_000.0).contains(&meters), "{meters}");
    }
}

/// Backends whose distinct numeric-conversion rules must survive the port.
#[derive(Clone, Copy, Debug)]
enum ArgumentKind {
    CString,
    RedisString,
}

/// Exercise the real C cursor with either of its command-relevant backends.
/// The callback runs while the borrowed field-name buffer is still alive.
fn parse_with(
    args: &[&CStr],
    kind: ArgumentKind,
    error: &mut QueryError,
    check: impl FnOnce(i32, &LegacyGeoFilter, usize, bool, &QueryError),
) {
    setup();
    // SAFETY: setup installs the mock once before this read and never changes it.
    let create_string = unsafe { redis_module::raw::RedisModule_CreateString.unwrap() };
    // SAFETY: setup installs the mock once before this read and never changes it.
    let free_string = unsafe { redis_module::raw::RedisModule_FreeString.unwrap() };
    let mut objects: Vec<_> = args
        .iter()
        .map(|arg| match kind {
            ArgumentKind::CString => arg.as_ptr().cast_mut().cast(),
            ArgumentKind::RedisString => {
                // SAFETY: arg covers its byte length, and null disables context ownership.
                unsafe { create_string(ptr::null_mut(), arg.as_ptr(), arg.count_bytes()) }.cast()
            }
        })
        .collect();
    let mut cursor = ArgsCursor {
        objs: objects.as_mut_ptr(),
        type_: match kind {
            ArgumentKind::CString => ffi::ACType_AC_TYPE_CHAR as i32,
            ArgumentKind::RedisString => ffi::ACType_AC_TYPE_RSTRING as i32,
        },
        argc: objects.len(),
        offset: 0,
    };
    let gf =
        redis_mock::allocator::alloc_shim(size_of::<LegacyGeoFilter>()).cast::<LegacyGeoFilter>();
    let mut empty = false;
    // SAFETY: gf is fresh aligned Redis storage, cursor arguments are live and
    // match its backend, and all outputs are initialized and disjoint. Mock APIs
    // are installed; the field-name buffer outlives the filter below.
    let result = unsafe {
        RustGeoFilter_LegacyParse(gf, &mut cursor, &mut empty, error.as_opaque_mut_ptr())
    };
    // SAFETY: parsing initializes gf on every return path.
    let parsed = unsafe { &*gf };
    assert_eq!(parsed.base.fieldIndex, RS_INVALID_FIELD_INDEX);
    assert!(parsed.base.numericFilters.is_null());
    check(result, parsed, cursor.offset, empty, error);
    // SAFETY: gf is exclusively owned, Redis-allocated, initialized by parsing,
    // and no longer borrowed. Its name buffer is still live.
    unsafe { RustLegacyGeoFilter_Free(gf) };
    if let ArgumentKind::RedisString = kind {
        for object in objects {
            // SAFETY: each object was created above and has no remaining borrower.
            unsafe { free_string(ptr::null_mut(), object.cast()) };
        }
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Calls the C ArgsCursor and HiddenString implementations"
)]
fn legacy_parser_success_failures_and_redacted_errors() {
    for kind in [ArgumentKind::CString, ArgumentKind::RedisString] {
        let args = [c"place", c"31.5", c"-24.75", c"12.25", c"KM", c"tail"];
        let mut error = QueryError::default();
        parse_with(
            &args,
            kind,
            &mut error,
            |result, gf, offset, empty, error| {
                assert_eq!(result, 0);
                assert_eq!(offset, 5);
                assert!(!empty);
                assert!(error.is_ok());
                assert_eq!(
                    (gf.base.lon, gf.base.lat, gf.base.radius, gf.base.unitType),
                    (31.5, -24.75, 12.25, 0)
                );
                let mut len = 0;
                // SAFETY: successful parsing produced a live HiddenString borrowing
                // the still-live input buffer. len is a disjoint writable output.
                let name = unsafe { ffi::HiddenString_GetUnsafe(gf.field, &mut len) };
                assert_eq!(len, 5);
                // SAFETY: HiddenString's borrowed name is NUL-terminated.
                assert_eq!(unsafe { CStr::from_ptr(name) }, c"place");
            },
        );
        for (bad, private) in [
            (1, c"SEARCH_PARSE_ARGS Bad arguments for <lon>: Could not convert argument to expected type"),
            (2, c"SEARCH_PARSE_ARGS Bad arguments for <lat>: Could not convert argument to expected type"),
            (3, c"SEARCH_PARSE_ARGS Bad arguments for <radius>: Could not convert argument to expected type"),
            (4, c"SEARCH_PARSE_ARGS Unknown distance unit \xff"),
        ] {
            let mut invalid = args;
            invalid[bad] = c"\xff";
            parse_with(&invalid, kind, &mut error, |result, gf, offset, empty, error| {
                assert_eq!(result, 1);
                assert_eq!(offset, if bad == 4 { 5 } else { bad });
                assert!(!empty);
                assert!(gf.field.is_null());
                assert_eq!(error.code(), QueryErrorCode::ParseArgs);
                assert_eq!(error.public_message(), Some(if bad == 4 { c"Unknown distance unit" } else { c"Bad arguments" }));
                assert_eq!(error.private_message(), Some(private));
            });
            error.clear();
        }
        parse_with(
            &args[..4],
            kind,
            &mut error,
            |result, gf, offset, _, error| {
                assert_eq!((result, offset), (1, 0));
                assert!(gf.field.is_null());
                assert_eq!(
                    (gf.base.lon, gf.base.lat, gf.base.radius, gf.base.unitType),
                    (0.0, 0.0, 0.0, 0)
                );
                assert_eq!(
                    error.private_message(),
                    Some(c"SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments")
                );
            },
        );
        error.clear();
        for empty_at in 1..=3 {
            let mut empty_args = args;
            empty_args[empty_at] = c"";
            parse_with(
                &empty_args,
                kind,
                &mut error,
                |result, gf, offset, empty, error| {
                    assert_eq!((result, offset), (0, 5));
                    assert_eq!(empty, matches!(kind, ArgumentKind::RedisString));
                    assert!(error.is_ok());
                    let mut expected = [31.5, -24.75, 12.25];
                    expected[empty_at - 1] = 0.0;
                    assert_eq!([gf.base.lon, gf.base.lat, gf.base.radius], expected);
                },
            );
        }
        for (text, c_result, redis_result, value) in [
            (c" 31.5", 0, 1, 31.5),
            (c"nan", 0, 1, f64::NAN),
            (c"inf", 1, 0, f64::INFINITY),
            (c"1e999", 1, 1, 0.0),
            (c"1e-999", 0, 1, 0.0),
        ] {
            let mut numeric_args = args;
            numeric_args[1] = text;
            let expected = match kind {
                ArgumentKind::CString => c_result,
                ArgumentKind::RedisString => redis_result,
            };
            parse_with(
                &numeric_args,
                kind,
                &mut error,
                |result, gf, offset, empty, error| {
                    assert_eq!(result, expected, "{kind:?}: {text:?}");
                    assert_eq!(offset, if expected == 0 { 5 } else { 1 });
                    assert!(!empty);
                    assert_eq!(error.is_ok(), expected == 0);
                    if expected == 0 {
                        assert!(gf.base.lon == value || (gf.base.lon.is_nan() && value.is_nan()));
                    }
                },
            );
            error.clear();
        }
        let mut invalid = args;
        invalid[4] = c"unknown";
        error.set_error(QueryErrorCode::Syntax, "first error");
        parse_with(&invalid, kind, &mut error, |result, _, _, _, error| {
            assert_eq!(result, 1);
            assert_eq!(error.code(), QueryErrorCode::Syntax);
            assert_eq!(error.public_message(), Some(c"first error"));
            assert_eq!(error.private_message(), Some(c"SEARCH_SYNTAX first error"));
        });
    }
}
