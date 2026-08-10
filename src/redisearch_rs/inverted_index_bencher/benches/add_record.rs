/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Focused bench for `InvertedIndex::add_record` — the indexing write path.
//!
//! Existing `encoding_decoding` benches call codec functions (e.g.
//! `DocIdsOnly::encode`) directly into a `Cursor<Vec<u8>>`, never constructing
//! an `InvertedIndex`. The `garbage_collection` bench builds an index in setup
//! but doesn't measure the build. So neither captures the per-record cost
//! introduced by the `ArcSwap<State>` write path. This bench fills that gap.

use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, doc_ids_only::DocIdsOnly, numeric::Numeric};

const RECORD_COUNTS: [u64; 3] = [1_000, 10_000, 100_000];

fn bench_add_record(c: &mut Criterion) {
    let mut group = c.benchmark_group("add_record");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(800));

    for n in RECORD_COUNTS {
        group.bench_function(BenchmarkId::new("DocIdsOnly", n), |b| {
            b.iter_batched(
                || InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
                |mut ii| {
                    for doc_id in 0..n {
                        let record = RSIndexResult::build_term().doc_id(doc_id).build();
                        ii.add_record(&record).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("Numeric", n), |b| {
            b.iter_batched(
                || InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly),
                |mut ii| {
                    for doc_id in 0..n {
                        let record = RSIndexResult::build_numeric(doc_id as f64 / 10.0)
                            .doc_id(doc_id)
                            .build();
                        ii.add_record(&record).unwrap();
                    }
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_add_record);
criterion_main!(benches);
