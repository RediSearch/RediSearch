/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Same batches as the GeoIndex C++ benchmark; consumed by `tests/benchmarks/geo_filter.py`.

use std::{ffi::CStr, hint::black_box, time::Instant};

use geo::{DistanceUnit, RadiusFilter};

fn measure(mut run: impl FnMut(usize)) -> f64 {
    const ITERATIONS: usize = 200_000;
    let mut samples = [0.0; 15];
    for sample in &mut samples {
        let start = Instant::now();
        for i in 0..ITERATIONS {
            run(i);
        }
        *sample = start.elapsed().as_nanos() as f64 / ITERATIONS as f64;
    }
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

fn main() {
    use geo::hash::{GEO_STEP_MAX, WGS84Coordinates, align_52bits, encode_wgs84};
    let units = [c"km", c"M", c"ft", c"MI", c"bad"];
    let parse = measure(|i| {
        // The frozen C batch includes strlen; discover the length here too.
        // SAFETY: every pointer is backed by one of the static C string literals above.
        let unit = unsafe { CStr::from_ptr(black_box(units[i % 5].as_ptr())) };
        black_box(DistanceUnit::parse_buffer(unit.to_bytes()));
    });
    let mut filter = RadiusFilter::new(12.5, -42.25, 100.0, Some(b"km"));
    let validate = measure(|i| {
        filter.lon = (i % 180) as f64;
        let _ = black_box(black_box(&filter).validate());
    });
    filter.lon = 12.5;
    let score = align_52bits(encode_wgs84(
        WGS84Coordinates::from_f64(12.6, -42.2).unwrap(),
        GEO_STEP_MAX,
    )) as f64;
    let radius = measure(|_| {
        black_box(black_box(&filter).within_radius(black_box(score)));
    });
    println!(
        "GEO_BENCH {{\"implementation\":\"rust\",\"parse\":{parse:.6},\"validate\":{validate:.6},\"radius\":{radius:.6}}}"
    );
}
