/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{GEO_DISTANCE_KM, GEO_DISTANCE_M, LegacyGeoFilter};
use geo_ffi::*;
use query_error::QueryError;
use redis_module::raw;
use std::{
    ffi::{CStr, c_void},
    ptr,
    sync::atomic::{AtomicIsize, Ordering::Relaxed},
};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

static ALLOCATIONS: AtomicIsize = AtomicIsize::new(0);
unsafe extern "C" fn allocate(size: usize) -> *mut c_void {
    ALLOCATIONS.fetch_add(1, Relaxed);
    redis_mock::allocator::alloc_shim(size)
}
unsafe extern "C" fn release(p: *mut c_void) {
    ALLOCATIONS.fetch_sub(1, Relaxed);
    redis_mock::allocator::free_shim(p);
}

#[unsafe(export_name = "RedisModule_StringPtrLen")]
static mut STRING_BYTES: Option<
    unsafe extern "C" fn(*const raw::RedisModuleString, *mut usize) -> *const std::ffi::c_char,
> = None;
#[unsafe(export_name = "RedisModule_StringToDouble")]
static mut STRING_DOUBLE: Option<
    unsafe extern "C" fn(*const raw::RedisModuleString, *mut f64) -> i32,
> = None;

#[test]
#[cfg_attr(miri, ignore = "requires native C FFI")]
fn filter_lifecycle() {
    redis_mock::init_redis_module_mock();
    // SAFETY: this binary has one test; allocator/API tables remain stable during calls.
    // Buffers and exclusive outputs outlive every call, and every allocation is freed once.
    unsafe {
        RedisModule_Alloc = allocate;
        RedisModule_Free = release;
        STRING_BYTES = raw::RedisModule_StringPtrLen;
        STRING_DOUBLE = raw::RedisModule_StringToDouble;
        for (unit, code) in [(c"KM", 0), (c"m", 1), (c"FT", 2), (c"mi", 3), (c"bad", -1)] {
            assert_eq!(GeoDistance_Parse(unit.as_ptr()), code);
            assert_eq!(
                GeoDistance_Parse_Buffer(unit.as_ptr(), unit.to_bytes().len()),
                code
            );
            let spelling = if code == -1 {
                "<badunit>".into()
            } else {
                unit.to_str().unwrap().to_lowercase()
            };
            assert_eq!(
                CStr::from_ptr(GeoDistance_ToString(code)).to_str().unwrap(),
                spelling
            );
        }
        assert_eq!(
            GeoDistance_Parse_Buffer(b"m\0xxxxxxxxxxxxx".as_ptr().cast(), 15),
            GEO_DISTANCE_M
        );
        assert_eq!(GeoDistance_Parse_Buffer(ptr::null(), 16), -1);
        let gf = NewGeoFilter(2.0, 3.0, 4.0, ptr::null(), 0);
        assert_eq!(
            ((*gf).lon, (*gf).lat, (*gf).radius, (*gf).unitType),
            (2.0, 3.0, 4.0, GEO_DISTANCE_KM)
        );
        assert_eq!((*gf).fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        assert!((*gf).numericFilters.is_null());
        // FieldSpec consists of C scalar, pointer, bitfield and union members; zero is valid.
        let mut field: ffi::FieldSpec = std::mem::zeroed();
        field.index = 42;
        GeoFilter_SetField(gf, &field);
        assert_eq!((*gf).fieldIndex, 42);
        GeoFilter_SetField(gf, ptr::null());
        assert_eq!((*gf).fieldIndex, rqe_core::RS_INVALID_FIELD_INDEX);
        let mut error = QueryError::default();
        assert_eq!(GeoFilter_Validate(gf, error.as_opaque_mut_ptr()), 1);
        (*gf).radius = -1.0;
        assert_eq!(GeoFilter_Validate(gf, error.as_opaque_mut_ptr()), 0);
        assert_eq!(
            error.private_message().unwrap(),
            c"SEARCH_SYNTAX Invalid GeoFilter radius"
        );
        GeoFilter_Free(gf);
        let gf = NewGeoFilter(0.0, 0.0, 200000.0, c"m".as_ptr(), 1);
        assert_eq!((*gf).unitType, GEO_DISTANCE_M);
        let mut distance = std::mem::MaybeUninit::uninit();
        let score = encodeGeo(1.0, 0.0);
        assert_eq!(isWithinRadius(gf, score, distance.as_mut_ptr()), 1);
        let mut distance = distance.assume_init();
        assert!((distance - 111226.3).abs() < 1.0);
        (*gf).radius = 1000.0;
        assert_eq!(isWithinRadius(gf, score, ptr::null_mut()), 0);
        distance = -1.0;
        assert_eq!(isWithinRadius(gf, score, &mut distance), 0);
        assert!((distance - 111226.3).abs() < 1.0);
        // The C and Rust layouts match; this also exercises allocator handoff before the swap.
        drop(rqe_iterators::build_geo_numeric_filters(&mut *gf.cast(), 7).unwrap());
        assert!(!(*gf).numericFilters.is_null());
        GeoFilter_Free(gf);
        for (lon, lat, radius, unit, argc, offset, message) in [
            (c"12", c"34", c"56", c"ft", 5, 5, ""),
            (c"", c"34", c"56", c"ft", 5, 5, ""),
            (c"x", c"34", c"56", c"ft", 5, 1, "Bad arguments"),
            (c"12", c"x", c"56", c"ft", 5, 2, "Bad arguments"),
            (c"12", c"34", c"x", c"ft", 5, 3, "Bad arguments"),
            (c"12", c"34", c"56", c"yd", 5, 5, "Unknown distance unit"),
            (c"12", c"34", c"56", c"\xff", 5, 5, "Unknown distance unit"),
            (
                c"12",
                c"34",
                c"56",
                c"ft",
                4,
                0,
                "GEOFILTER requires 5 arguments",
            ),
        ] {
            let mut args = [c"ignored", c"location", lon, lat, radius, unit].map(|s| {
                raw::RedisModule_CreateString.unwrap()(
                    ptr::null_mut(),
                    s.as_ptr(),
                    s.to_bytes().len(),
                )
                .cast()
            });
            let mut cursor = ffi::ArgsCursor {
                objs: args.as_mut_ptr(),
                type_: 1,
                argc: argc + 1,
                offset: 1,
            };
            let legacy = allocate(size_of::<LegacyGeoFilter>()).cast();
            let mut empty = false;
            error.clear();
            assert_eq!(
                GeoFilter_LegacyParse(legacy, &mut cursor, &mut empty, error.as_opaque_mut_ptr()),
                i32::from(!message.is_empty())
            );
            assert_eq!(cursor.offset, offset + 1);
            if message.is_empty() {
                assert_eq!(
                    (
                        (*legacy).base.lon,
                        (*legacy).base.lat,
                        (*legacy).base.radius,
                        (*legacy).base.unitType
                    ),
                    (if lon.is_empty() { 0.0 } else { 12.0 }, 34.0, 56.0, 2)
                );
                assert_eq!(empty, lon.is_empty());
                assert!(!(*legacy).field.is_null());
            } else {
                assert_eq!(error.public_message().unwrap().to_str().unwrap(), message);
                let private = match offset {
                    0 => b"SEARCH_PARSE_ARGS GEOFILTER requires 5 arguments".to_vec(),
                    5 => [b"SEARCH_PARSE_ARGS Unknown distance unit ".as_slice(), unit.to_bytes()].concat(),
                    _ => format!("SEARCH_PARSE_ARGS Bad arguments for <{}>: Could not convert argument to expected type", ["lon", "lat", "radius"][offset - 1]).into_bytes(),
                };
                assert_eq!(error.private_message().unwrap().to_bytes(), private);
                assert!((*legacy).field.is_null());
            }
            LegacyGeoFilter_Free(legacy);
            for arg in args {
                raw::RedisModule_FreeString.unwrap()(ptr::null_mut(), arg.cast());
            }
            assert_eq!(ALLOCATIONS.load(Relaxed), 0);
        }
    }
}
