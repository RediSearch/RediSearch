/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![cfg(not(miri))] // These boundary tests link the real C argument parser.

use geo_filter_ffi::*;
use query_error::{QueryError, QueryErrorCode};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    ffi::CStr,
    ptr,
};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

thread_local! { static LIVE: Cell<isize> = const { Cell::new(0) }; }
struct CountAllocations;
// SAFETY: allocation and deallocation use System with the caller's unchanged layout.
unsafe impl GlobalAlloc for CountAllocations {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid nonzero allocation layout.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let _ = LIVE.try_with(|n| n.set(n.get() + 1));
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        let _ = LIVE.try_with(|n| n.set(n.get() - 1));
        // SAFETY: p came from System with this layout and is owned by the caller.
        unsafe { System.dealloc(p, layout) };
    }
}
#[global_allocator]
static ALLOCATOR: CountAllocations = CountAllocations;

#[test]
fn boundary() {
    // SAFETY: every input below is a live terminated string or a bounded byte slice.
    unsafe {
        for (name, tag) in [(c"kM", 0), (c"m", 1), (c"FT", 2), (c"mi", 3), (c"bad", -1)] {
            assert_eq!(Rust_GeoDistance_Parse(name.as_ptr()), tag);
            assert_eq!(
                Rust_GeoDistance_Parse_Buffer(name.as_ptr(), name.count_bytes()),
                tag
            );
        }
        for (bytes, tag) in [
            (&b"mi\0"[..1], 1),
            (&b"ft"[..], 2),
            (&b"km\0xxxxxxxxxxxx"[..], 0),
            (&b"km\0xxxxxxxxxxxxx"[..], -1),
        ] {
            assert_eq!(
                Rust_GeoDistance_Parse_Buffer(bytes.as_ptr().cast(), bytes.len()),
                tag
            );
        }
        assert_eq!(CStr::from_ptr(Rust_GeoDistance_ToString(-1)), c"<badunit>");
        assert_eq!(CStr::from_ptr(Rust_GeoDistance_ToString(2)), c"ft");
    }
    let allocations = LIVE.get();
    // SAFETY: null unit selects the default; the mock Redis allocator is linked above.
    let filter = unsafe { Rust_NewGeoFilter(12.0, 34.0, 2.0, ptr::null(), 0) };
    let mut error = QueryError::default();
    // SAFETY: filter is a fresh, exclusive allocation and error is a disjoint local.
    unsafe {
        assert_eq!((*filter).lon, 12.0);
        assert_eq!((*filter).lat, 34.0);
        assert_eq!((*filter).radius, 2.0);
        assert_eq!((*filter).unitType, 0);
        assert!((*filter).numericFilters.is_null());
        assert_eq!((*filter).fieldIndex, u16::MAX);
        let mut field: ffi::FieldSpec = std::mem::zeroed();
        field.index = 9;
        Rust_GeoFilter_SetField(filter, &field);
        assert_eq!((*filter).fieldIndex, 9);
        Rust_GeoFilter_SetField(filter, ptr::null());
        assert_eq!((*filter).fieldIndex, u16::MAX);
        assert_eq!(
            Rust_GeoFilter_Validate(filter, error.as_opaque_mut_ptr()),
            1
        );
        let score = geo::hash::align_52bits(geo::hash::encode_wgs84(
            geo::hash::WGS84Coordinates::from_f64(12.01, 34.0).unwrap(),
            geo::hash::GEO_STEP_MAX,
        )) as f64;
        let mut distance = -1.0;
        assert_eq!(Rust_isWithinRadius(filter, score, &mut distance), 1);
        assert!((900.0..940.0).contains(&distance));
        (*filter).unitType = 1;
        (*filter).radius = distance;
        assert_eq!(Rust_isWithinRadius(filter, score, ptr::null_mut()), 1);
        (*filter).radius = 1.0;
        let mut outside = -1.0;
        assert_eq!(Rust_isWithinRadius(filter, score, &mut outside), 0);
        assert_eq!(outside, distance);
        let mut ranges = Box::new([ptr::null_mut(); geo::GEO_RANGE_COUNT]);
        ranges[0] = ffi::NewNumericFilter(0.0, 1.0, true, false, true, 0, filter.cast());
        (*filter).numericFilters = Box::into_raw(ranges).cast();
        Rust_GeoFilter_Free(filter);
    }
    assert_eq!(LIVE.get(), allocations);
}

#[test]
fn invalid_abi_unit() {
    // SAFETY: the supplied token is readable; allocations use the linked Redis mock.
    let filter = unsafe { Rust_NewGeoFilter(12.0, 34.0, 2.0, c"mi".as_ptr(), 2) };
    // SAFETY: filter is exclusively owned until the final free.
    unsafe {
        assert_eq!((*filter).unitType, 3);
        for (lon, radius, tag, message) in [
            (12.0, 2.0, 42, c"Invalid GeoFilter unit"),
            (f64::NAN, 2.0, 3, c"Invalid GeoFilter lat/lon"),
            (12.0, f64::NAN, 3, c"Invalid GeoFilter radius"),
        ] {
            (*filter).lon = lon;
            (*filter).radius = radius;
            (*filter).unitType = tag;
            let mut error = QueryError::default();
            assert_eq!(
                Rust_GeoFilter_Validate(filter, error.as_opaque_mut_ptr()),
                0
            );
            assert_eq!(error.public_message(), Some(message));
        }
        Rust_GeoFilter_Free(filter);
    }
}

#[test]
fn legacy_failure() {
    redis_mock::init_redis_module_mock();
    for (name, args, offset, message, detail) in [
        (
            "short",
            vec![c"g", c"12"],
            0,
            "GEOFILTER requires 5 arguments",
            "",
        ),
        (
            "lon",
            vec![c"g", c"bad", c"34", c"1", c"km"],
            1,
            "Bad arguments",
            " for <lon>: Could not convert argument to expected type",
        ),
        (
            "lat",
            vec![c"g", c"12", c"bad", c"1", c"km"],
            2,
            "Bad arguments",
            " for <lat>: Could not convert argument to expected type",
        ),
        (
            "radius",
            vec![c"g", c"12", c"34", c"bad", c"km"],
            3,
            "Bad arguments",
            " for <radius>: Could not convert argument to expected type",
        ),
        (
            "unit",
            vec![c"g", c"12", c"34", c"1", c"bad"],
            5,
            "Unknown distance unit",
            " bad",
        ),
        ("success", vec![c"g", c"12", c"34", c"7", c"FT"], 5, "", ""),
        ("empty_lon", vec![c"g", c"", c"34", c"7", c"FT"], 5, "", ""),
    ] {
        let strings: Vec<_> = args
            .iter()
            .map(|s| redis_module::RedisString::create(None, s.to_bytes()))
            .collect();
        let mut pointers: Vec<_> = strings.iter().map(|s| s.inner.cast()).collect();
        let mut cursor = ffi::ArgsCursor {
            objs: pointers.as_mut_ptr(),
            type_: 1,
            argc: pointers.len(),
            offset: 0,
        };
        let allocations = LIVE.get();
        let output = redis_mock::allocator::alloc_shim(size_of::<geo::abi::LegacyGeoFilter>())
            .cast::<geo::abi::LegacyGeoFilter>();
        let mut error = QueryError::default();
        let mut empty = false;
        // SAFETY: the cursor references live terminated strings and every output is a disjoint local.
        let result = unsafe {
            Rust_GeoFilter_LegacyParse(output, &mut cursor, &mut empty, error.as_opaque_mut_ptr())
        };
        assert_eq!(result == 0, message.is_empty(), "{name}");
        assert_eq!(cursor.offset, offset, "{name}");
        assert_eq!(empty, name == "empty_lon");
        // SAFETY: the parser initializes output on success and failure alike.
        let parsed = unsafe { &*output };
        if result == 0 {
            assert_eq!(parsed.base.lon, if empty { 0.0 } else { 12.0 });
            assert_eq!(
                (parsed.base.lat, parsed.base.radius, parsed.base.unitType),
                (34.0, 7.0, 2)
            );
            // SAFETY: successful parsing creates a live field wrapper borrowing the first argument.
            let field =
                unsafe { ffi::HiddenString_GetUnsafe(parsed.field.cast(), ptr::null_mut()) };
            // SAFETY: the returned borrowed argument remains NUL-terminated and alive.
            assert_eq!(unsafe { CStr::from_ptr(field) }, c"g");
        } else {
            assert!(parsed.field.is_null());
            assert!(parsed.base.numericFilters.is_null());
            assert_eq!(
                (parsed.base.lon, parsed.base.lat, parsed.base.radius),
                (0.0, 0.0, 0.0)
            );
            assert_eq!(error.public_message().unwrap().to_str().unwrap(), message);
            assert_eq!(error.code(), QueryErrorCode::ParseArgs);
            assert_eq!(
                error.private_message().unwrap().to_str().unwrap(),
                format!("SEARCH_PARSE_ARGS {message}{detail}")
            );
        }
        // SAFETY: output is a live Redis allocation initialized by the parser, with no remaining borrows.
        unsafe { Rust_LegacyGeoFilter_Free(output) };
        drop(error);
        assert_eq!(LIVE.get(), allocations, "{name}");
    }
}
