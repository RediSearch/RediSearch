/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The radius check every record of a geo query goes through.

use std::{hint::black_box, ptr};

use criterion::{Criterion, criterion_group, criterion_main};
use geo::{
    DistanceUnit, RadiusFilter,
    hash::{self, GEO_STEP_MAX, WGS84Coordinates},
};
use geo_bencher as _;

const LON: f64 = 2.1;
const LAT: f64 = 48.1;
const RADIUS_KM: f64 = 10.0;

fn is_within_radius(c: &mut Criterion) {
    // A 22 km by 15 km grid centered on the filter, about half of it within the radius.
    let points: Vec<f64> = (0..100)
        .flat_map(|i| (0..100).map(move |j| (2.0 + i as f64 * 0.002, 48.0 + j as f64 * 0.002)))
        .map(|(lon, lat)| {
            let coords = WGS84Coordinates::from_f64(lon, lat).expect("coordinates are in bounds");
            hash::align_52bits(hash::encode_wgs84(coords, GEO_STEP_MAX)) as f64
        })
        .collect();
    let mut group = c.benchmark_group("is_within_radius");

    let gf = ffi::GeoFilter {
        fieldIndex: 0,
        lat: LAT,
        lon: LON,
        radius: RADIUS_KM,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: ptr::null_mut(),
    };
    group.bench_function("c", |b| {
        b.iter(|| {
            black_box(&points)
                .iter()
                .filter(|&&point| {
                    let mut distance = 0.0;
                    // SAFETY: `gf` and `distance` are live locals.
                    unsafe { ffi::isWithinRadius(&gf, point, &mut distance) != 0 }
                })
                .count()
        })
    });

    let filter = RadiusFilter {
        lon: LON,
        lat: LAT,
        radius: RADIUS_KM,
        unit: DistanceUnit::Kilometers,
    };
    group.bench_function("rust", |b| {
        b.iter(|| {
            black_box(&points)
                .iter()
                .filter(|&&point| filter.reaches(filter.distance_to(point)))
                .count()
        })
    });

    group.finish();
}

criterion_group!(benches, is_within_radius);
criterion_main!(benches);
