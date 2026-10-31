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
use geo::{GeoFilter, RadiusFilter, Unit};

fn filters(c: &mut Criterion) {
    for input in ["km", "M", "ft", "mi", "unknown"] {
        let mut group = c.benchmark_group(format!("unit/{input}"));
        group.bench_function("rust", |b| {
            b.iter(|| Unit::parse(black_box(input.as_bytes())))
        });
        group.finish();
    }
    let filter = GeoFilter::new(15.087269, 37.502669, 200.0, geo::GEO_DISTANCE_KM);
    let mut group = c.benchmark_group("validate");
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
