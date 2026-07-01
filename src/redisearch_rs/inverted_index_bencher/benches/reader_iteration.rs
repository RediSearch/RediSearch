/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Regression bench for the reader traversal hot path — full forward scan of an
//! [`InvertedIndex`] via [`IndexReader::next_record`].
//!
//! Runs identically on `master` (flat `blocks: ThinVec` walk) and on the snapshot
//! branch (where `reader()` captures a point-in-time snapshot — `Arc` clone of
//! `sealed` + shallow clone of `pending` + deep copy of the `in_progress` tail —
//! then walks it lock-free). Comparing the two branches with `--save-baseline` /
//! `--baseline` isolates the per-reader snapshot-capture cost plus any traversal
//! regression from the extra region indirection.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{IndexReader, InvertedIndex, doc_ids_only::DocIdsOnly};

const RECORD_COUNTS: [u64; 3] = [1_000, 10_000, 100_000];

fn bench_reader_iteration(c: &mut Criterion) {
    let mut group = c.benchmark_group("reader_iteration");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(800));

    for n in RECORD_COUNTS {
        // Build the index once — reading does not mutate it, so it is reused across
        // measured iterations.
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 0..n {
            let record = RSIndexResult::build_term().doc_id(doc_id).build();
            ii.add_record(&record).unwrap();
        }

        group.bench_function(BenchmarkId::new("DocIdsOnly", n), |b| {
            b.iter(|| {
                let mut reader = ii.reader();
                let mut result = RSIndexResult::build_virt().build();
                let mut count = 0u64;
                while reader.next_record(&mut result).unwrap() {
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_reader_iteration);
criterion_main!(benches);
