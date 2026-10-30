/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{Criterion, criterion_group, criterion_main};
use geo_filter_ffi::*;
use std::{hint::black_box, ptr};

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn filters(c: &mut Criterion) {
    let filter = ffi::GeoFilter {
        fieldIndex: 0,
        lat: 34.0,
        lon: 12.0,
        radius: 2.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: ptr::null_mut(),
    };
    for (name, longitude) in [("near", 12.01), ("far", -120.0)] {
        let score = geo::hash::align_52bits(geo::hash::encode_wgs84(
            geo::hash::WGS84Coordinates::from_f64(longitude, 34.0).unwrap(),
            geo::hash::GEO_STEP_MAX,
        )) as f64;
        let mut group = c.benchmark_group(format!("geo_filter_radius_{name}"));
        for (implementation, function) in [
            (
                "c",
                ffi::isWithinRadius as unsafe extern "C" fn(_, _, _) -> _,
            ),
            ("rust", Rust_isWithinRadius),
        ] {
            group.bench_function(implementation, |b| {
                b.iter(|| {
                    let mut distance = 0.0;
                    // SAFETY: filter is validated, the output is an exclusive local and the score is indexed data.
                    black_box(unsafe {
                        function(black_box(&filter), black_box(score), &mut distance)
                    });
                    black_box(distance);
                })
            });
        }
        group.finish();
    }
    let mut group = c.benchmark_group("geo_filter_unit");
    for (implementation, function) in [
        (
            "c",
            ffi::GeoDistance_Parse_Buffer as unsafe extern "C" fn(_, _) -> _,
        ),
        ("rust", Rust_GeoDistance_Parse_Buffer),
    ] {
        group.bench_function(implementation, |b| {
            b.iter(|| {
                // SAFETY: the token contains two readable bytes.
                black_box(unsafe { function(black_box(c"KM".as_ptr()), black_box(2)) })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, filters);
criterion_main!(benches);
