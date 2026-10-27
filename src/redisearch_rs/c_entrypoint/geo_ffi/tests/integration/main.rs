/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo_ffi::*;
use query_error::QueryError;
use std::{ffi::CStr, mem::MaybeUninit, ptr};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn filter() -> ffi::GeoFilter {
    ffi::GeoFilter {
        fieldIndex: u16::MAX,
        lon: 0.0,
        lat: 0.0,
        radius: 1.0,
        unitType: 0,
        numericFilters: ptr::null_mut(),
    }
}

#[test]
fn ffi_units_validation_and_bool_boundary() {
    // SAFETY: all pointers refer to local initialized storage and static strings.
    unsafe {
        assert_eq!(GeoDistance_Parse(c"Mi".as_ptr()), 3);
        assert_eq!(GeoDistance_Parse_Buffer(c"ft".as_ptr(), 2), 2);
        assert_eq!(GeoDistance_Parse_Buffer(ptr::null(), 16), -1);
        assert_eq!(GeoDistance_Parse_Buffer(ptr::null(), 0), -1);
        assert_eq!(CStr::from_ptr(GeoDistance_ToString(-2)), c"<badunit>");
        let mut gf = filter();
        let mut error = QueryError::default();
        for (lon, lat, radius) in [
            (f64::NAN, 0.0, 1.0),
            (0.0, f64::NAN, 1.0),
            (0.0, 0.0, f64::NAN),
        ] {
            gf.lon = lon;
            gf.lat = lat;
            gf.radius = radius;
            assert_eq!(GeoFilter_Validate(&gf, error.as_opaque_mut_ptr()), 1);
            assert!(error.is_ok());
        }
        gf = filter();
        gf.unitType = -1;
        gf.lat = 91.0;
        gf.radius = 0.0;
        assert_eq!(GeoFilter_Validate(&gf, error.as_opaque_mut_ptr()), 0);
        assert_eq!(error.public_message(), Some(c"Invalid GeoFilter unit"));
        error.clear();
        gf.unitType = 0;
        assert_eq!(GeoFilter_Validate(&gf, error.as_opaque_mut_ptr()), 0);
        assert_eq!(error.public_message(), Some(c"Invalid GeoFilter lat/lon"));
        error.clear();
        gf.lat = 0.0;
        assert_eq!(GeoFilter_Validate(&gf, error.as_opaque_mut_ptr()), 0);
        assert_eq!(error.public_message(), Some(c"Invalid GeoFilter radius"));
        gf = filter();
        let rust_boundary: unsafe extern "C" fn(*const ffi::GeoFilter, f64, *mut f64) -> bool =
            isWithinRadius;
        let mut distance = -1.0;
        assert!(rust_boundary(&gf, encodeGeo(0.0, 0.0), &mut distance));
        assert!(distance >= 0.0 && distance < 1.0);
        assert!(!rust_boundary(&gf, encodeGeo(10.0, 10.0), ptr::null_mut()));
        gf.radius = f64::NAN;
        assert!(!rust_boundary(&gf, encodeGeo(0.0, 0.0), ptr::null_mut()));
        // The C declaration remains int; pin it without invoking an incompatible ABI in Miri.
        assert!(include_str!("../../../../../geo_index.h").contains("int isWithinRadius("));
        // SAFETY: bindgen's FieldSpec consists of C scalars, pointers and unions;
        // zero initializes those fields, and only index is accessed by SetField.
        let mut field: ffi::FieldSpec = MaybeUninit::zeroed().assume_init();
        field.index = 7;
        GeoFilter_SetField(&mut gf, &field);
        assert_eq!(gf.fieldIndex, 7);
        GeoFilter_SetField(&mut gf, ptr::null());
        assert_eq!(gf.fieldIndex, u16::MAX);
    }
}

#[test]
fn allocate_and_free_filter() {
    // SAFETY: the allocator is initialized once and the returned filter is uniquely owned.
    unsafe {
        init_allocator();
        let gf = NewGeoFilter(12.0, 34.0, 56.0, ptr::null(), 0);
        assert_eq!(
            ((*gf).lon, (*gf).lat, (*gf).radius, (*gf).unitType),
            (12.0, 34.0, 56.0, 0)
        );
        GeoFilter_Free(gf);
        let gf = NewGeoFilter(0.0, 0.0, 1.0, c"MI".as_ptr(), 2);
        assert_eq!((*gf).unitType, 3);
        GeoFilter_Free(gf);
    }
}

fn init_allocator() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        // SAFETY: initialized once before any allocator access in these tests.
        unsafe {
            redis_module::RedisModule_Alloc = Some(redis_mock::allocator::alloc_shim);
            redis_module::RedisModule_Free = Some(redis_mock::allocator::free_shim);
        }
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "legacy parsing calls C ArgsCursor and HiddenString functions"
)]
fn legacy_cursor_errors_and_success_cleanup() {
    init_allocator();
    let cases: &[(&[&[u8]], usize, Option<&CStr>)] = &[
        (
            &[b"g", b"1", b"2", b"3"],
            0,
            Some(c"GEOFILTER requires 5 arguments"),
        ),
        (
            &[b"g", b"bad", b"2", b"3", b"km"],
            1,
            Some(c"Bad arguments"),
        ),
        (
            &[b"g", b"1", b"bad", b"3", b"km"],
            2,
            Some(c"Bad arguments"),
        ),
        (
            &[b"g", b"1", b"2", b"bad", b"km"],
            3,
            Some(c"Bad arguments"),
        ),
        (
            &[b"g", b"1", b"2", b"3", b"\xff"],
            5,
            Some(c"Unknown distance unit"),
        ),
        (&[b"g\0ignored", b"1", b"2", b"3", b"KM", b"extra"], 5, None),
    ];
    for &(args, offset, message) in cases {
        let strings: Vec<_> = args
            .iter()
            .map(|s| {
                let mut s = s.to_vec();
                s.push(0);
                s
            })
            .collect();
        let mut pointers: Vec<_> = strings
            .iter()
            .map(|s| s.as_ptr().cast_mut().cast())
            .collect();
        let mut cursor = ffi::ArgsCursor {
            objs: pointers.as_mut_ptr(),
            type_: 2,
            argc: args.len(),
            offset: 0,
        };
        let mut error = QueryError::default();
        let mut empty = false;
        // SAFETY: all cursor buffers are live and NUL-terminated; the filter allocation is owned here.
        unsafe {
            let alloc = redis_module::RedisModule_Alloc.unwrap();
            let gf = alloc(size_of::<ffi::LegacyGeoFilter>()).cast();
            assert_eq!(
                GeoFilter_LegacyParse(gf, &mut cursor, &mut empty, error.as_opaque_mut_ptr()),
                i32::from(message.is_some())
            );
            assert_eq!(cursor.offset, offset);
            assert_eq!(error.public_message(), message);
            assert!(!empty);
            if args.last() == Some(&b"\xff".as_slice()) {
                assert_eq!(
                    error.private_message().unwrap().to_bytes(),
                    b"SEARCH_PARSE_ARGS Unknown distance unit \xff"
                );
            }
            if message.is_none() {
                let name = ffi::HiddenString_GetUnsafe((*gf).field, ptr::null_mut());
                assert_eq!(CStr::from_ptr(name), c"g");
                assert_eq!((*gf).base.unitType, 0);
            }
            LegacyGeoFilter_Free(gf);
        }
    }
}

#[test]
fn aggregate_argument_forms_and_errors() {
    use value_ffi::{
        constructors::{
            RSValue_NewBorrowedString, RSValue_NewNull, RSValue_NewNumber, RSValue_NewUndefined,
        },
        getters::RSValue_Number_Get,
        shared::RSValue_DecrRef,
    };
    // SAFETY: a zeroed evaluator has valid null pointers and its enum zero variant; only err is read.
    let mut ctx: ffi::ExprEval = unsafe { MaybeUninit::zeroed().assume_init() };
    let mut error = QueryError::default();
    ctx.err = error.as_opaque_mut_ptr().cast();
    let string = |s: &'static CStr| {
        // SAFETY: borrowed strings have static lifetime.
        unsafe { RSValue_NewBorrowedString(s.as_ptr(), s.count_bytes() as u32) }
    };
    let cases = [
        (vec![string(c"1.2,4.5"), string(c"1.25,4.5")], false, false),
        (
            vec![
                RSValue_NewNumber(1.2),
                RSValue_NewNumber(4.5),
                string(c"1.25,4.5"),
            ],
            false,
            false,
        ),
        (
            vec![
                string(c"1.2,4.5"),
                RSValue_NewNumber(1.25),
                RSValue_NewNumber(4.5),
            ],
            false,
            false,
        ),
        (
            vec![
                RSValue_NewNumber(1.2),
                RSValue_NewNumber(4.5),
                RSValue_NewNumber(1.25),
                RSValue_NewNumber(4.5),
            ],
            false,
            false,
        ),
        (
            vec![
                RSValue_NewNumber(encodeGeo(1.2, 4.5)),
                RSValue_NewNumber(encodeGeo(1.25, 4.5)),
            ],
            false,
            false,
        ),
        (vec![RSValue_NewNull(), string(c"1,2")], true, false),
        (vec![string(c"bad"), string(c"1,2")], true, true),
        (
            vec![RSValue_NewNumber(1.0), string(c"bad"), string(c"1,2")],
            true,
            false,
        ),
        (
            vec![string(c"1,2"), RSValue_NewNumber(1.0), RSValue_NewNull()],
            true,
            false,
        ),
    ];
    for (mut args, nan, has_error) in cases {
        error.clear();
        ctx.err = error.as_opaque_mut_ptr().cast();
        let result = RSValue_NewUndefined();
        // SAFETY: arguments and result are separately owned live values, evaluator error is live.
        unsafe {
            assert_eq!(
                geofunc_distance(&mut ctx, args.as_mut_ptr(), args.len(), result),
                1
            );
            let distance = RSValue_Number_Get(result);
            if nan {
                assert!(distance.is_nan());
            } else {
                assert!((distance - 5544.17).abs() < 1.0);
            }
            assert_eq!(!error.is_ok(), has_error);
            RSValue_DecrRef(result);
            for arg in args {
                RSValue_DecrRef(arg);
            }
        }
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "range construction and cleanup call C NumericFilter functions"
)]
fn free_filter_with_numeric_ranges() {
    init_allocator();
    // SAFETY: the filter and every populated range have unique ownership; the
    // array layout matches the geo iterator's owned range allocation.
    unsafe {
        let gf = NewGeoFilter(0.0, 0.0, 1.0, ptr::null(), 0);
        let mut ranges = Box::new([ptr::null_mut(); geo::GEO_RANGE_COUNT]);
        ranges[0] = ffi::NewNumericFilter(0.0, 10.0, true, true, true, 0, gf.cast());
        ranges[8] = ffi::NewNumericFilter(10.0, 20.0, true, true, true, 0, gf.cast());
        (*gf).numericFilters = Box::into_raw(ranges).cast();
        GeoFilter_Free(gf);
    }
}
