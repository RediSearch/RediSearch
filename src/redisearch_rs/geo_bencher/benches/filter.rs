/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use geo::{DistanceUnit, RadiusFilter};

redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

fn filter(c: &mut Criterion) {
    for bytes in [b"m".as_slice(), b"KM", b"ft", b"mi", b"invalid"] {
        let mut group = c.benchmark_group(format!("unit/{}", String::from_utf8_lossy(bytes)));
        group.bench_function("c", |b| {
            b.iter(|| {
                // SAFETY: the borrowed slice has the specified length.
                unsafe {
                    ffi::GeoDistance_Parse_Buffer(black_box(bytes.as_ptr().cast()), bytes.len())
                }
            })
        });
        group.bench_function("rust", |b| b.iter(|| DistanceUnit::parse(black_box(bytes))));
        group.finish();
    }
    let gf = ffi::GeoFilter {
        fieldIndex: 0,
        lat: 48.85,
        lon: 2.29,
        radius: 10.0,
        unitType: DistanceUnit::Kilometers as i32,
        numericFilters: std::ptr::null_mut(),
    };
    let mut status = query_error::QueryError::default();
    let mut group = c.benchmark_group("validate");
    group.bench_function("c", |b| {
        b.iter(|| {
            // SAFETY: both objects are initialized and exclusively borrowed for the call.
            unsafe { ffi::GeoFilter_Validate(black_box(&gf), status.as_opaque_mut_ptr().cast()) }
        })
    });
    group.bench_function("rust", |b| {
        b.iter(|| {
            let gf = black_box(&gf);
            RadiusFilter::new(
                gf.lon,
                gf.lat,
                gf.radius,
                DistanceUnit::from_raw(gf.unitType),
            )
        })
    });
    group.finish();
    for score in [0.0, 3663833102791133.0] {
        let rust = RadiusFilter::new(
            gf.lon,
            gf.lat,
            gf.radius,
            DistanceUnit::from_raw(gf.unitType),
        )
        .unwrap();
        let mut group = c.benchmark_group(format!("radius/{score}"));
        group.bench_function("c", |b| {
            b.iter(|| {
                let mut distance = 0.0;
                // SAFETY: the filter and output are live, non-aliasing values.
                let inside =
                    unsafe { ffi::isWithinRadius(black_box(&gf), black_box(score), &mut distance) };
                (inside, distance)
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| black_box(rust).contains(black_box(score)))
        });
        group.finish();
    }
}

criterion_group!(benches, filter);
criterion_main!(benches);
