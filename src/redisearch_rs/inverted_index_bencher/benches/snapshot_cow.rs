/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Timing benches for the copy-on-write snapshot mechanism (feature-branch only —
//! master has no snapshot API to compare against; master's snapshot cost is zero
//! because it holds the read lock instead of copying).
//!
//! Two components, per `docs/design/snapshot-cow-benchmark-plan.md`:
//!
//! - **C1 — per-snapshot reader copy.** [`InvertedIndex::snapshot`] = `Arc::clone`
//!   of `sealed` + shallow `Vec` clone of `pending` + deep copy of the `in_progress`
//!   tail block. Paid by every reader at construction/revalidation.
//! - **C2 — GC copy-of-pinned-blocks.** When `apply_gc` rebuilds `sealed` while
//!   snapshots are live, each block a snapshot pins (refcount > 1) is deep-cloned
//!   via `Arc::try_unwrap(..).unwrap_or_else(clone)` instead of moved. We vary the
//!   number of held snapshots {0, 1, 8}; the delta over the 0-held baseline is the
//!   COW cost. Byte-level cost is reported by the `snapshot_cow_memory` example.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, numeric::Numeric};

const RECORD_COUNTS: [u64; 2] = [10_000, 100_000];
const PINNED_SNAPSHOTS: [usize; 3] = [0, 1, 8];

/// Delete the oldest 30% of documents — produces GC work spread across early blocks.
fn doc_exist(doc_id: u64, total: u64) -> bool {
    doc_id >= (total * 30 / 100)
}

fn build_index(n: u64) -> InvertedIndex<Numeric> {
    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 0..n {
        ii.add_record(
            &RSIndexResult::build_numeric(doc_id as f64 / 10.0)
                .doc_id(doc_id)
                .build(),
        )
        .unwrap();
    }
    ii
}

/// C1: cost of capturing a snapshot over an index of `n` records.
fn bench_snapshot_capture(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_capture");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(800));

    for n in RECORD_COUNTS {
        let ii = build_index(n);
        group.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.iter(|| {
                // Arc clone of sealed + Vec clone of pending + deep copy of in_progress.
                black_box(ii.snapshot());
            });
        });
    }

    group.finish();
}

/// C2: cost of `apply_gc` while `k` snapshots pin the index's blocks. `k = 0` is the
/// baseline (blocks moved); `k > 0` forces the `Arc::try_unwrap -> clone` COW path.
fn bench_gc_apply_with_pinned(c: &mut Criterion) {
    let mut group = c.benchmark_group("gc_apply_pinned");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(800));

    for n in RECORD_COUNTS {
        for k in PINNED_SNAPSHOTS {
            group.bench_function(BenchmarkId::new(format!("records_{n}"), k), |b| {
                b.iter_batched(
                    || {
                        let ii = build_index(n);
                        let delta = ii
                            .scan_gc(
                                |d| doc_exist(d, n),
                                None::<fn(&RSIndexResult, &inverted_index::RepairContext<'_>)>,
                            )
                            .unwrap()
                            .unwrap();
                        // Hold `k` snapshots so their pinned blocks cannot be moved
                        // (refcount > 1) and must be deep-cloned by apply_gc.
                        let snaps: Vec<_> = (0..k).map(|_| ii.snapshot()).collect();
                        (ii, delta, snaps)
                    },
                    |(mut ii, delta, snaps)| {
                        let info = ii.apply_gc(delta);
                        // Keep the snapshots alive across apply_gc.
                        black_box(&snaps);
                        black_box(info);
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_snapshot_capture, bench_gc_apply_with_pinned);
criterion_main!(benches);
