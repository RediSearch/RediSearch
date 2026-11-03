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
use geo::{DistanceUnit, GeoFilter, RadiusFilter};

fn filter(c: &mut Criterion) {
    for bytes in [b"m".as_slice(), b"KM", b"ft", b"mi", b"invalid"] {
        let mut group = c.benchmark_group(format!("unit/{}", String::from_utf8_lossy(bytes)));
        group.bench_function("rust", |b| b.iter(|| DistanceUnit::parse(black_box(bytes))));
        group.finish();
    }
    let gf = GeoFilter {
        fieldIndex: 0,
        lat: 48.85,
        lon: 2.29,
        radius: 10.0,
        unitType: DistanceUnit::Kilometers as i32,
        numericFilters: std::ptr::null_mut(),
    };
    let mut group = c.benchmark_group("validate");
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
        group.bench_function("rust", |b| {
            b.iter(|| black_box(rust).contains(black_box(score)))
        });
        group.finish();
    }
}

criterion_group!(benches, filter);
criterion_main!(benches);
