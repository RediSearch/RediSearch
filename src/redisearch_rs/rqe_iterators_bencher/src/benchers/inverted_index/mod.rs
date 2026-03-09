/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark inverted index iterator.

use std::time::Duration;

use criterion::{BenchmarkGroup, Criterion, measurement::WallTime};

mod numeric;
mod term;
mod wildcard;

pub use numeric::NumericBencher;
pub use term::TermBencher;
pub use wildcard::WildcardBencher;

const MEASUREMENT_TIME: Duration = Duration::from_secs(4);
const WARMUP_TIME: Duration = Duration::from_millis(500);
/// The number of documents in the index.
const INDEX_SIZE: u64 = 1_000_000;
/// The delta between the document IDs in the sparse index.
const SPARSE_DELTA: u64 = 1000;
/// The increment when skipping to a document ID.
const SKIP_TO_STEP: u64 = 100;

fn benchmark_group<'a>(
    c: &'a mut Criterion,
    it_name: &str,
    test: &str,
) -> BenchmarkGroup<'a, WallTime> {
    let label = format!("Iterator - InvertedIndex - {it_name} - {test}");
    let mut group = c.benchmark_group(label);
    group.measurement_time(MEASUREMENT_TIME);
    group.warm_up_time(WARMUP_TIME);
    group
}
