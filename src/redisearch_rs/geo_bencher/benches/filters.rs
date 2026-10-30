/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{hint::black_box, ptr, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use ffi::{GeoDistance_GEO_DISTANCE_KM, GeoFilter};
use geo_bencher as c;
use geo_ffi as rust;
use query_error::QueryError;

fn filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("geo_unit_parse");
    for (name, parse) in [
        ("c", c::GeoDistance_Parse as unsafe extern "C" fn(_) -> _),
        ("rust", rust::Rust_GeoDistance_Parse),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                for unit in [c"m", c"KM", c"Ft", c"mi", c"bad"] {
                    // SAFETY: all inputs are static NUL-terminated strings.
                    black_box(unsafe { parse(black_box(unit.as_ptr())) });
                }
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("geo_unit_buffer");
    for (name, parse) in [
        (
            "c",
            c::GeoDistance_Parse_Buffer as unsafe extern "C" fn(_, _) -> _,
        ),
        ("rust", rust::Rust_GeoDistance_Parse_Buffer),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                for unit in [b"m".as_slice(), b"KM", b"Ft", b"mi", b"bad"] {
                    // SAFETY: each slice contains the supplied number of readable bytes.
                    black_box(unsafe {
                        parse(black_box(unit.as_ptr().cast()), black_box(unit.len()))
                    });
                }
            })
        });
    }
    group.finish();

    let filter = GeoFilter {
        fieldIndex: 0,
        lon: 13.0,
        lat: 52.0,
        radius: 10.0,
        unitType: GeoDistance_GEO_DISTANCE_KM,
        numericFilters: ptr::null_mut(),
    };
    for (label, score) in [
        ("near", rust::encodeGeo(13.001, 52.0)),
        ("far", rust::encodeGeo(-73.98, 40.75)),
    ] {
        let mut group = c.benchmark_group(format!("geo_radius_{label}"));
        for (name, contains) in [
            ("c", c::isWithinRadius as unsafe extern "C" fn(_, _, _) -> _),
            ("rust", rust::Rust_isWithinRadius),
        ] {
            group.bench_function(name, |b| {
                b.iter(|| {
                    let mut distance = 0.0;
                    // SAFETY: filter and output are initialized, live and disjoint.
                    black_box(unsafe {
                        contains(black_box(&filter), black_box(score), &mut distance)
                    });
                    black_box(distance);
                })
            });
        }
        group.finish();
    }

    let mut group = c.benchmark_group("geo_construct_free");
    for (name, new, free) in [
        (
            "c",
            ffi::NewGeoFilter as unsafe extern "C" fn(_, _, _, _, _) -> _,
            ffi::GeoFilter_Free as unsafe extern "C" fn(_),
        ),
        ("rust", rust::Rust_NewGeoFilter, rust::Rust_GeoFilter_Free),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                // SAFETY: a static unit string and initialized Redis allocator mocks.
                let gf = unsafe {
                    new(
                        black_box(13.0),
                        black_box(52.0),
                        black_box(10.0),
                        c"km".as_ptr(),
                        2,
                    )
                };
                black_box(gf);
                // SAFETY: gf is a uniquely owned allocation from the paired constructor.
                unsafe { free(gf) };
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("geo_validate");
    for (name, validate) in [
        (
            "c",
            c::GeoFilter_Validate as unsafe extern "C" fn(_, _) -> _,
        ),
        ("rust", rust::Rust_GeoFilter_Validate),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut error = QueryError::default();
                // SAFETY: the filter and initialized error are live and disjoint.
                black_box(unsafe { validate(black_box(&filter), error.as_opaque_mut_ptr()) });
            })
        });
    }
    group.finish();

    let mut args = [c"g", c"13", c"52", c"10", c"km"].map(|s| s.as_ptr().cast_mut().cast());
    let mut group = c.benchmark_group("geo_legacy_parse_free");
    for (name, parse, free) in [
        (
            "c",
            c::GeoFilter_LegacyParse as unsafe extern "C" fn(_, _, _, _) -> _,
            c::LegacyGeoFilter_Free as unsafe extern "C" fn(_),
        ),
        (
            "rust",
            rust::Rust_GeoFilter_LegacyParse,
            rust::Rust_LegacyGeoFilter_Free,
        ),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut cursor = ffi::ArgsCursor {
                    objs: args.as_mut_ptr(),
                    type_: ffi::ACType_AC_TYPE_CHAR as i32,
                    argc: args.len(),
                    offset: 0,
                };
                let mut empty = false;
                let mut error = QueryError::default();
                let gf =
                    redis_mock::allocator::alloc_shim(size_of::<ffi::LegacyGeoFilter>()).cast();
                // SAFETY: gf is fresh Redis-allocated storage; cursor points at live terminated
                // arguments, and the initialized writable outputs are disjoint.
                black_box(unsafe {
                    parse(
                        gf,
                        black_box(&mut cursor),
                        &mut empty,
                        error.as_opaque_mut_ptr(),
                    )
                });
                // SAFETY: either parser initializes gf for destruction, including on failure.
                unsafe { free(gf) };
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = filters
}
criterion_main!(benches);
