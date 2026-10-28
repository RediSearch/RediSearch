/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, hint::black_box};

use criterion::{Criterion, criterion_group, criterion_main};
use geo::{DistanceUnit, FilterParams};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

unsafe extern "C" {
    fn CGeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> i32;
    fn CGeoFilter_Validate(gf: *const ffi::GeoFilter, error: *mut ffi::QueryError) -> i32;
    fn CIsWithinRadius(gf: *const ffi::GeoFilter, score: f64, distance: *mut f64) -> i32;
}

fn benchmarks(c: &mut Criterion) {
    let units: &[&[u8]] = &[b"km", b"M", b"fT", b"Mi", b"yards", b"km\0junk", b""];
    c.bench_function("geo_filter/units/c", |b| {
        b.iter(|| {
            for unit in black_box(units) {
                // SAFETY: each slice contains `len` readable bytes, including empty slices.
                black_box(unsafe { CGeoDistance_Parse_Buffer(unit.as_ptr().cast(), unit.len()) });
            }
        });
    });
    c.bench_function("geo_filter/units/rust", |b| {
        b.iter(|| {
            for unit in black_box(units) {
                black_box(DistanceUnit::parse_buffer(unit));
            }
        });
    });

    let filter = ffi::GeoFilter {
        fieldIndex: 7,
        lon: 12.0,
        lat: -33.25,
        radius: 40.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: std::ptr::null_mut(),
    };
    c.bench_function("geo_filter/validate/c", |b| {
        b.iter(|| {
            // SAFETY: the filter is valid, so validation never writes an error.
            black_box(unsafe { CGeoFilter_Validate(black_box(&filter), std::ptr::null_mut()) });
        });
    });
    let params = FilterParams {
        lon: filter.lon,
        lat: filter.lat,
        radius: filter.radius,
        unit: Some(DistanceUnit::Kilometers),
    };
    c.bench_function("geo_filter/validate/rust", |b| {
        b.iter(|| black_box(black_box(&params).validate()));
    });
    let score = redisearch_rs::geo::encodeGeo(12.25, -33.5);
    c.bench_function("geo_filter/radius/c", |b| {
        b.iter(|| {
            let mut distance = 0.0;
            // SAFETY: both pointers are valid and exclusively borrowed where written.
            let found =
                unsafe { CIsWithinRadius(black_box(&filter), black_box(score), &mut distance) };
            black_box((found, distance));
        });
    });
    c.bench_function("geo_filter/radius/rust", |b| {
        b.iter(|| black_box(black_box(&params).within_radius(black_box(score))));
    });
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
