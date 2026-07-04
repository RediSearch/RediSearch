/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Micro-bench for the reader revalidation hot path — the cost the `_LOCK_FREE_READS`
//! regression fix targets.
//!
//! Lock-free reads release the spec lock per result; on resume the reader asks
//! [`IndexReader::needs_revalidation`] whether its snapshot is still current. Under a
//! concurrent writer, the pre-fix check treated every append as invalidating, so each
//! resumed read did a full `reset()` (re-snapshot from the start) plus a `seek_record`
//! to reposition — turning a linear scan into roughly O(N · seek). The fix keys
//! revalidation off the `sealed` `Arc`'s pointer identity, so appends no longer force a
//! rewind and a resumed read is a plain forward step.
//!
//! The two functions bracket that difference on a single, static index (no real writer
//! needed — we just drive the two revalidation outcomes directly):
//!
//! - `no_rewind` — the post-fix path: pay the (cheap) `needs_revalidation` check per
//!   result, then step forward.
//! - `rewind_each_result` — the pre-fix path under churn: `reset()` + `seek_record` on
//!   every result.
//!
//! The gap between them is the per-result rewind + re-seek overhead the fix removes.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{IndexReader, InvertedIndex, doc_ids_only::DocIdsOnly};
use rqe_core::DocId;

const RECORD_COUNTS: [DocId; 2] = [10_000, 100_000];

/// Build an index with contiguous doc IDs `1..=n`, so `seek_record(k)` lands exactly on
/// the k-th record — letting the rewind path resume with a simple counter.
fn build_index(n: DocId) -> InvertedIndex<DocIdsOnly> {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=n {
        let record = RSIndexResult::build_term().doc_id(doc_id).build();
        ii.add_record(&record).unwrap();
    }
    ii
}

fn bench_revalidation_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("revalidation_churn");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(1500));

    for n in RECORD_COUNTS {
        // Reading never mutates the index, so build it once and reuse it.
        let ii = build_index(n);

        // Post-fix: `needs_revalidation` is false under appends, so the resumed read is a
        // plain forward step. We still call it every result to include its cost.
        group.bench_function(BenchmarkId::new("no_rewind", n), |b| {
            b.iter(|| {
                let mut reader = ii.reader();
                let mut result = RSIndexResult::build_virt().build();
                let mut count = 0u64;
                loop {
                    black_box(reader.needs_revalidation());
                    if !reader.next_record(&mut result).unwrap() {
                        break;
                    }
                    count += 1;
                }
                black_box(count);
            });
        });

        // Pre-fix under churn: revalidation fired on every result, forcing a re-snapshot
        // from the start plus a `seek_record` to reposition at the next doc.
        group.bench_function(BenchmarkId::new("rewind_each_result", n), |b| {
            b.iter(|| {
                let mut reader = ii.reader();
                let mut result = RSIndexResult::build_virt().build();
                let mut count: DocId = 0;
                loop {
                    reader.reset();
                    if !reader.seek_record(count + 1, &mut result).unwrap() {
                        break;
                    }
                    count += 1;
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_revalidation_churn);
criterion_main!(benches);
