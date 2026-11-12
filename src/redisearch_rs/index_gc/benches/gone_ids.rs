/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A GC scan's cost with each [`GoneIds`] form, conversion included.
//!
//! Each iteration builds the [`GoneIds`] a cycle would build from its source set
//! and scans every posting list against it, so the decode and rewrite work both
//! forms share is in the measurement. A cap of 0 keeps the roaring set; the
//! default cap turns the same window into a [`GoneIds::Bitset`].

use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_gc::liveness::{GoneIds, MAX_BITMAP_BYTES};
use index_result::RSIndexResult;
use inverted_index::{InvertedIndex, RepairContext, numeric::Numeric};
use roaring::RoaringTreemap;
use rqe_core::DocId;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const DOCS: DocId = 1_000_000;

/// How many posting lists the short layout splits the documents into.
const SHORT_LISTS: DocId = 10_000;

/// A deterministic pseudo-random choice of about `1 / every` of the documents.
fn deleted(every: u64) -> RoaringTreemap {
    let mut state = 42u64;
    (1..=DOCS)
        .filter(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % every == 0
        })
        .collect()
}

fn index_of(ids: impl Iterator<Item = DocId>) -> InvertedIndex<Numeric> {
    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);
    for id in ids {
        ii.add_record(&RSIndexResult::build_numeric(id as f64).doc_id(id).build())
            .unwrap();
    }
    ii
}

/// One posting list holding every document, or [`SHORT_LISTS`] lists that
/// interleave them.
fn postings(long: bool) -> Vec<InvertedIndex<Numeric>> {
    if long {
        vec![index_of(1..=DOCS)]
    } else {
        (0..SHORT_LISTS)
            .map(|k| index_of((1 + k..=DOCS).step_by(SHORT_LISTS as usize)))
            .collect()
    }
}

#[derive(Clone, Copy)]
enum Policy {
    /// Starts from the doc table's deleted IDs.
    Thread,
    /// Starts from the frozen table's live IDs.
    Fork,
}

fn benchmark_gone_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("GoneIds");
    group.measurement_time(Duration::from_secs(2));
    group.warm_up_time(Duration::from_millis(300));

    for (deletions, every) in [("Sparse 0.25%", 400), ("Dense 50%", 2)] {
        let dead = deleted(every);
        let mut live: RoaringTreemap = (1..=DOCS).collect();
        live -= &dead;

        for (layout, long) in [("Long postings", true), ("Short postings", false)] {
            let lists = postings(long);

            for (policy_name, policy) in [("Thread", Policy::Thread), ("Fork", Policy::Fork)] {
                for (form, cap) in [("Bitset", MAX_BITMAP_BYTES), ("Roaring", 0)] {
                    let id = BenchmarkId::new(
                        format!("{policy_name}/{form}"),
                        format!("{deletions}/{layout}"),
                    );
                    group.bench_function(id, |b| {
                        b.iter_batched(
                            || match policy {
                                Policy::Thread => dead.clone(),
                                Policy::Fork => live.clone(),
                            },
                            |set| {
                                let gone = match policy {
                                    Policy::Thread => GoneIds::from_set_with_cap(set, cap),
                                    Policy::Fork => GoneIds::from_live_set(set, DOCS, cap),
                                };
                                for ii in &lists {
                                    ii.scan_gc(
                                        &gone,
                                        None::<fn(&RSIndexResult, &RepairContext<'_>)>,
                                    )
                                    .unwrap();
                                }
                            },
                            BatchSize::LargeInput,
                        )
                    });
                }
            }
        }
    }

    group.finish();
}

criterion_group!(benches, benchmark_gone_ids);
criterion_main!(benches);
