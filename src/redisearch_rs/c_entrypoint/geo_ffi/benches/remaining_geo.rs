/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Compare the original local master C bodies with their Rust FFI replacements.

use criterion::{Criterion, criterion_group, criterion_main};
use geo_ffi::*;
use query_error::{QueryError, opaque::OpaqueQueryError};
use std::{
    ffi::{c_char, c_int},
    hint::black_box,
    ptr,
    time::Duration,
};
use value_ffi::{
    RSValue,
    constructors::{RSValue_NewBorrowedString, RSValue_NewNumber, RSValue_NewUndefined},
    getters::RSValue_Number_Get,
    shared::RSValue_DecrRef,
};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

unsafe extern "C" {
    fn C_GeoDistance_Parse(unit: *const c_char) -> c_int;
    fn C_GeoFilter_Validate(gf: *const ffi::GeoFilter, status: *mut OpaqueQueryError) -> c_int;
    fn C_NewGeoFilter(
        lon: f64,
        lat: f64,
        radius: f64,
        unit: *const c_char,
        len: usize,
    ) -> *mut ffi::GeoFilter;
    fn C_GeoFilter_Free(gf: *mut ffi::GeoFilter);
    fn C_GeoFilter_LegacyParse(
        gf: *mut ffi::LegacyGeoFilter,
        ac: *mut ffi::ArgsCursor,
        empty: *mut bool,
        status: *mut OpaqueQueryError,
    ) -> c_int;
    fn C_LegacyGeoFilter_Free(gf: *mut ffi::LegacyGeoFilter);
    fn C_isWithinRadius(gf: *const ffi::GeoFilter, bits: f64, distance: *mut f64) -> c_int;
    fn C_geofunc_distance(
        ctx: *mut std::ffi::c_void,
        args: *mut std::ffi::c_void,
        argc: usize,
        result: *mut std::ffi::c_void,
    ) -> c_int;
}

/// Call the C baseline using opaque pointers at its foreign boundary.
///
/// # Safety
/// The evaluator, argument array and result must satisfy the expression callback contract.
unsafe extern "C" fn c_distance(
    ctx: *mut ffi::ExprEval,
    args: *mut *mut RSValue,
    argc: usize,
    result: *mut RSValue,
) -> c_int {
    // SAFETY: the caller supplies live expression values; casts preserve their addresses.
    unsafe { C_geofunc_distance(ctx.cast(), args.cast(), argc, result.cast()) }
}

fn benchmarks(c: &mut Criterion) {
    // SAFETY: initialization is single-threaded, before any benchmark uses the allocator.
    unsafe {
        redis_module::RedisModule_Alloc = Some(redis_mock::allocator::alloc_shim);
        redis_module::RedisModule_Free = Some(redis_mock::allocator::free_shim);
    }
    eprintln!("C baseline: {}", env!("GEO_C_BASELINE"));
    let mut group = c.benchmark_group("remaining_geo");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(30);
    let mut error = QueryError::default();
    // SAFETY: each pointer refers to a static string or live owned value, and the
    // matching allocator/free pair owns every filter. Values outlive all callbacks.
    unsafe {
        let gf = NewGeoFilter(1.2, 4.5, 10.0, ptr::null(), 0);
        let bits = encodeGeo(1.25, 4.5);
        assert_eq!(
            C_GeoDistance_Parse(c"KM".as_ptr()),
            GeoDistance_Parse(c"KM".as_ptr())
        );
        assert_eq!(
            C_GeoFilter_Validate(gf, error.as_opaque_mut_ptr()),
            GeoFilter_Validate(gf, error.as_opaque_mut_ptr())
        );
        let mut c_meters = 0.0;
        let mut rust_distance = 0.0;
        assert_eq!(
            C_isWithinRadius(gf, bits, &mut c_meters) != 0,
            isWithinRadius(gf, bits, &mut rust_distance)
        );
        assert_eq!(c_meters, rust_distance);
        for (name, parse) in [
            (
                "C/unit",
                C_GeoDistance_Parse as unsafe extern "C" fn(_) -> _,
            ),
            ("Rust/unit", GeoDistance_Parse),
        ] {
            group.bench_function(name, |b| {
                b.iter(|| black_box(parse(black_box(c"KM".as_ptr()))))
            });
        }
        for (name, validate) in [
            (
                "C/validate",
                C_GeoFilter_Validate as unsafe extern "C" fn(_, _) -> _,
            ),
            ("Rust/validate", GeoFilter_Validate),
        ] {
            group.bench_function(name, |b| {
                b.iter(|| black_box(validate(black_box(gf), error.as_opaque_mut_ptr())))
            });
        }
        group.bench_function("C/radius", |b| {
            b.iter(|| {
                black_box(C_isWithinRadius(
                    black_box(gf),
                    black_box(bits),
                    ptr::null_mut(),
                ))
            })
        });
        group.bench_function("Rust/radius", |b| {
            b.iter(|| {
                black_box(isWithinRadius(
                    black_box(gf),
                    black_box(bits),
                    ptr::null_mut(),
                ))
            })
        });
        for (name, new, free) in [
            (
                "C/new_free",
                C_NewGeoFilter as unsafe extern "C" fn(_, _, _, _, _) -> _,
                C_GeoFilter_Free as unsafe extern "C" fn(_),
            ),
            ("Rust/new_free", NewGeoFilter, GeoFilter_Free),
        ] {
            group.bench_function(name, |b| {
                b.iter(|| {
                    let gf = new(
                        black_box(1.2),
                        black_box(4.5),
                        black_box(10.0),
                        black_box(c"km".as_ptr()),
                        2,
                    );
                    black_box(gf);
                    free(gf);
                })
            });
        }
        let mut argv: Vec<_> = [c"location", c"1.2", c"4.5", c"10", c"km"]
            .iter()
            .map(|s| s.as_ptr().cast_mut().cast())
            .collect();
        for (name, parse, free) in [
            (
                "C/legacy",
                C_GeoFilter_LegacyParse as unsafe extern "C" fn(_, _, _, _) -> _,
                C_LegacyGeoFilter_Free as unsafe extern "C" fn(_),
            ),
            ("Rust/legacy", GeoFilter_LegacyParse, LegacyGeoFilter_Free),
        ] {
            group.bench_function(name, |b| {
                b.iter(|| {
                    let gf =
                        redis_module::RedisModule_Alloc.unwrap()(size_of::<ffi::LegacyGeoFilter>())
                            .cast();
                    let mut cursor = ffi::ArgsCursor {
                        objs: argv.as_mut_ptr(),
                        type_: 2,
                        argc: 5,
                        offset: 0,
                    };
                    let mut empty = false;
                    black_box(parse(
                        gf,
                        black_box(&mut cursor),
                        &mut empty,
                        error.as_opaque_mut_ptr(),
                    ));
                    free(gf);
                })
            });
        }
        let mut ctx: ffi::ExprEval = std::mem::zeroed();
        ctx.err = error.as_opaque_mut_ptr().cast();
        let string = |s: &'static std::ffi::CStr| {
            RSValue_NewBorrowedString(s.as_ptr(), s.count_bytes() as u32)
        };
        for (form, mut args) in [
            ("aggregate2", vec![string(c"1.2,4.5"), string(c"1.25,4.5")]),
            (
                "aggregate3",
                vec![
                    RSValue_NewNumber(1.2),
                    RSValue_NewNumber(4.5),
                    string(c"1.25,4.5"),
                ],
            ),
            (
                "aggregate4",
                vec![
                    RSValue_NewNumber(1.2),
                    RSValue_NewNumber(4.5),
                    RSValue_NewNumber(1.25),
                    RSValue_NewNumber(4.5),
                ],
            ),
        ] {
            let result = RSValue_NewUndefined();
            c_distance(&mut ctx, args.as_mut_ptr(), args.len(), result);
            let expected = RSValue_Number_Get(result);
            geofunc_distance(&mut ctx, args.as_mut_ptr(), args.len(), result);
            assert_eq!(expected, RSValue_Number_Get(result));
            for (language, evaluate) in [
                ("C", c_distance as unsafe extern "C" fn(_, _, _, _) -> _),
                ("Rust", geofunc_distance),
            ] {
                group.bench_function(format!("{language}/{form}"), |b| {
                    b.iter(|| {
                        black_box(evaluate(
                            black_box(&mut ctx),
                            black_box(args.as_mut_ptr()),
                            args.len(),
                            result,
                        ))
                    })
                });
            }
            RSValue_DecrRef(result);
            for arg in args {
                RSValue_DecrRef(arg);
            }
        }
        GeoFilter_Free(gf);
    }
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
