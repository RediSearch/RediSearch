/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{hint::black_box, ptr};

use criterion::{Criterion, criterion_group, criterion_main};
use geo::{RadiusFilter, Unit};
use redisearch_rs as _;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn filters(c: &mut Criterion) {
    for input in ["km", "M", "ft", "mi", "unknown"] {
        let mut group = c.benchmark_group(format!("unit/{input}"));
        group.bench_function("c", |b| {
            b.iter(|| {
                let input = black_box(input.as_bytes());
                // SAFETY: the borrowed buffer is readable for its length.
                unsafe { ffi::GeoDistance_Parse_Buffer(input.as_ptr().cast(), input.len()) }
            })
        });
        group.bench_function("rust", |b| {
            b.iter(|| Unit::parse(black_box(input.as_bytes())))
        });
        group.finish();
    }
    let filter = ffi::GeoFilter {
        fieldIndex: 0,
        lat: 37.502669,
        lon: 15.087269,
        radius: 200.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: ptr::null_mut(),
    };
    let mut group = c.benchmark_group("validate");
    group.bench_function("c", |b| {
        b.iter(|| {
            // SAFETY: the valid filter cannot write an error to the unused status.
            unsafe { ffi::GeoFilter_Validate(black_box(&filter), ptr::null_mut()) }
        })
    });
    group.bench_function("rust", |b| {
        b.iter(|| {
            let f = black_box(&filter);
            RadiusFilter::new(f.lon, f.lat, f.radius, Unit::from_raw(f.unitType))
        })
    });
    group.finish();
    let rust = RadiusFilter::new(
        filter.lon,
        filter.lat,
        filter.radius,
        Some(Unit::Kilometers),
    )
    .unwrap();
    let mut group = c.benchmark_group("radius");
    group.bench_function("c", |b| {
        b.iter(|| {
            let mut distance = 0.0;
            // SAFETY: the filter and output are live, aligned, non-overlapping values.
            let found = unsafe {
                ffi::isWithinRadius(
                    black_box(&filter),
                    black_box(3479099956230698.0),
                    &mut distance,
                )
            };
            black_box((found, distance))
        })
    });
    group.bench_function("rust", |b| {
        b.iter(|| {
            let filter = black_box(rust);
            let distance = filter.distance(black_box(3479099956230698.0));
            black_box((filter.contains(distance), distance))
        })
    });
}

criterion_group!(benches, filters);
criterion_main!(benches);
