/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod empty;
pub mod geo_shape;
pub mod id_list;
pub mod intersection;
pub mod inverted_index;
pub mod metric;
pub mod not;
pub mod not_optimized;
pub mod optional;
pub mod optional_optimized;
pub mod union;
pub mod wildcard;

use std::time::Duration;

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};

/// Create a Criterion benchmark group configured with the given measurement and warm-up times.
fn group<'a>(
    c: &'a mut Criterion,
    name: &str,
    measurement_time: Duration,
    warm_up_time: Duration,
) -> BenchmarkGroup<'a, WallTime> {
    let mut group = c.benchmark_group(name);
    group.measurement_time(measurement_time);
    group.warm_up_time(warm_up_time);
    group
}
