/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CStr, ptr};

use geo_ffi::*;
use query_error::{QueryError, QueryErrorCode};
use rqe_iterators_test_utils::GlobalGuard;

#[test]
fn unit_buffer_boundary() {
    for (token, expected) in [
        (c"kM", ffi::GeoDistance_GEO_DISTANCE_KM),
        (c"M", ffi::GeoDistance_GEO_DISTANCE_M),
        (c"Ft", ffi::GeoDistance_GEO_DISTANCE_FT),
        (c"mI", ffi::GeoDistance_GEO_DISTANCE_MI),
        (c"yards", ffi::GeoDistance_GEO_DISTANCE_INVALID),
        (c"", ffi::GeoDistance_GEO_DISTANCE_INVALID),
    ] {
        // SAFETY: every token is a static NUL-terminated string.
        assert_eq!(
            unsafe { GeoDistance_Parse(token.as_ptr()) },
            expected,
            "{token:?}"
        );
    }
    for (name, bytes, expected) in [
        (
            "nonterminated",
            b"MI".as_slice(),
            ffi::GeoDistance_GEO_DISTANCE_MI,
        ),
        (
            "short nul prefix",
            b"m\0xxxxxxxxxxxxx",
            ffi::GeoDistance_GEO_DISTANCE_M,
        ),
        (
            "length limit",
            b"m\0xxxxxxxxxxxxxx",
            ffi::GeoDistance_GEO_DISTANCE_INVALID,
        ),
        ("empty", b"", ffi::GeoDistance_GEO_DISTANCE_INVALID),
    ] {
        // SAFETY: bytes contains exactly the readable buffer length passed to the ABI.
        unsafe {
            assert_eq!(
                GeoDistance_Parse_Buffer(bytes.as_ptr().cast(), bytes.len()),
                expected,
                "{name}"
            );
        }
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires live C functions and Redis allocator symbols")]
fn filter_lifecycle() {
    let _guard = GlobalGuard::default();
    // SAFETY: FieldSpec contains C scalars, unions and nullable pointers, all valid when zeroed.
    let mut field: ffi::FieldSpec = unsafe { std::mem::zeroed() };
    field.index = 7;
    // SAFETY: null unit selects the default; the linked allocator mocks are initialized.
    let gf = unsafe { NewGeoFilter(13.0, 52.0, 10.0, ptr::null(), 0) };
    let mut error = QueryError::default();
    // SAFETY: gf uniquely owns initialized Redis storage; outputs are disjoint locals.
    unsafe {
        assert_eq!((*gf).fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert_eq!((*gf).unitType, ffi::GeoDistance_GEO_DISTANCE_KM);
        assert!((*gf).numericFilters.is_null());
        GeoFilter_SetField(gf, &field);
        assert_eq!((*gf).fieldIndex, 7);
        GeoFilter_SetField(gf, ptr::null());
        assert_eq!((*gf).fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert_eq!(GeoFilter_Validate(gf, error.as_opaque_mut_ptr()), 1);
        let mut distance = 0.0;
        assert_eq!(
            isWithinRadius(gf, encodeGeo(13.001, 52.0), &mut distance),
            1
        );
        assert!((distance - 68.5).abs() < 0.5);
        assert_eq!(
            isWithinRadius(gf, encodeGeo(13.001, 52.0), ptr::null_mut()),
            1
        );
        (*gf).unitType = 42;
        assert_eq!(GeoFilter_Validate(gf, error.as_opaque_mut_ptr()), 0);
        assert_eq!(error.public_message(), Some(c"Invalid GeoFilter unit"));
        assert_eq!(CStr::from_ptr(GeoDistance_ToString(42)), c"<badunit>");
        assert_eq!(isWithinRadius(gf, 0.0, &mut distance), 0);
        assert!(distance.is_nan());
        (*gf).unitType = ffi::GeoDistance_GEO_DISTANCE_KM;
        assert!(
            !rqe_iterators::build_geo_numeric_filters(&mut *gf, 0)
                .unwrap()
                .is_empty()
        );
        GeoFilter_Free(gf);
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "requires the C ArgsCursor and hidden-string implementation"
)]
fn legacy_parser_ownership_and_errors() {
    let _guard = GlobalGuard::default();
    redis_mock::init_redis_module_mock();
    for (name, args, consumed, expected) in [
        ("success", [c"g", c"13", c"52", c"10", c"km"], 5, None),
        ("empty longitude", [c"g", c"", c"52", c"10", c"km"], 5, None),
        (
            "leading whitespace",
            [c"g", c" 13", c"52", c"10", c"km"],
            1,
            Some(c"Bad arguments"),
        ),
        (
            "bad longitude",
            [c"g", c"bad", c"52", c"10", c"km"],
            1,
            Some(c"Bad arguments"),
        ),
        (
            "bad latitude",
            [c"g", c"13", c"bad", c"10", c"km"],
            2,
            Some(c"Bad arguments"),
        ),
        (
            "bad radius",
            [c"g", c"13", c"52", c"bad", c"km"],
            3,
            Some(c"Bad arguments"),
        ),
        (
            "binary unit",
            [c"g", c"13", c"52", c"10", c"\xff"],
            5,
            Some(c"Unknown distance unit"),
        ),
    ] {
        let empty_longitude = args[1].is_empty();
        let mut args = args.map(|s| {
            // SAFETY: mocks are initialized; each static string covers its given length.
            unsafe {
                redis_module::raw::RedisModule_CreateString.unwrap()(
                    ptr::null_mut(),
                    s.as_ptr(),
                    s.to_bytes().len(),
                )
                .cast()
            }
        });
        let mut cursor = ffi::ArgsCursor {
            objs: args.as_mut_ptr(),
            type_: ffi::ACType_AC_TYPE_RSTRING as i32,
            argc: args.len(),
            offset: 0,
        };
        let mut error = QueryError::default();
        let mut empty = false;
        let gf = redis_mock::allocator::alloc_shim(size_of::<ffi::LegacyGeoFilter>())
            .cast::<ffi::LegacyGeoFilter>();
        // SAFETY: cursor references live mock Redis strings until after filter destruction;
        // gf is a fresh allocation and all other outputs are initialized disjoint locals.
        unsafe {
            let result =
                GeoFilter_LegacyParse(gf, &mut cursor, &mut empty, error.as_opaque_mut_ptr());
            assert_eq!(
                result == redis_module::REDISMODULE_OK as i32,
                expected.is_none(),
                "{name}"
            );
            assert_eq!(cursor.offset, consumed, "{name}");
            assert_eq!(error.public_message(), expected, "{name}");
            assert_eq!(empty, empty_longitude, "{name}");
            if name == "binary unit" {
                assert_eq!(
                    error.private_message(),
                    Some(c"SEARCH_PARSE_ARGS Unknown distance unit \xff")
                );
            } else if expected == Some(c"Bad arguments") {
                let label = ["lon", "lat", "radius"][consumed - 1];
                assert_eq!(error.private_message().unwrap().to_bytes(), format!("SEARCH_PARSE_ARGS Bad arguments for <{label}>: Could not convert argument to expected type").as_bytes());
            }
            if expected.is_none() {
                assert_eq!(
                    ((*gf).base.lon, (*gf).base.lat, (*gf).base.radius),
                    (if empty_longitude { 0.0 } else { 13.0 }, 52.0, 10.0)
                );
                assert_eq!(
                    CStr::from_ptr(ffi::HiddenString_GetUnsafe((*gf).field, ptr::null_mut())),
                    c"g"
                );
            }
            LegacyGeoFilter_Free(gf);
            for arg in args {
                redis_module::raw::RedisModule_FreeString.unwrap()(ptr::null_mut(), arg.cast());
            }
        }
    }

    let mut error = QueryError::default();
    error.set_error(QueryErrorCode::Generic, "first error");
    let mut cursor = ffi::ArgsCursor {
        objs: ptr::null_mut(),
        type_: ffi::ACType_AC_TYPE_CHAR as i32,
        argc: 0,
        offset: 0,
    };
    let mut empty = true;
    let gf = redis_mock::allocator::alloc_shim(size_of::<ffi::LegacyGeoFilter>()).cast();
    // SAFETY: an empty cursor reads no arguments; output storage is fresh and disjoint.
    unsafe {
        assert_eq!(
            GeoFilter_LegacyParse(gf, &mut cursor, &mut empty, error.as_opaque_mut_ptr()),
            redis_module::REDISMODULE_ERR as i32
        );
        assert_eq!(error.public_message(), Some(c"first error"));
        assert!(empty);
        LegacyGeoFilter_Free(gf);
    }
}
