/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Feasibility bench for inline repair of an inverted index's tail block on the
//! write path — the gate for `openspec/changes/inline-block-repair-on-write`.
//!
//! The question it answers: how does repairing one full block compare to the
//! `add_record` that would trigger it? Inline repair is only viable if the
//! repair, amortized over a stride of writes, disappears into the write cost.
//!
//! The existing `garbage_collection` bench measures a scan over a whole index,
//! which conflates per-block cost with block count, and `add_record` measures
//! bulk insertion. Neither gives the per-block-versus-per-write ratio.
//!
//! `IndexBlock::repair` is crate-private, so the measurement goes through
//! `InvertedIndex::scan_gc` on an index built to hold exactly one block: the
//! scan is then one `repair` call plus negligible loop overhead. Each bench
//! asserts the single-block precondition, so a future change to
//! `RECOMMENDED_BLOCK_ENTRIES` or the block-splitting rule fails the bench
//! rather than silently measuring something else.
//!
//! Two `doc_exist` predicates bracket the real cost. The production predicate
//! is `DocTable_Exists`, a C call this crate cannot link (the bencher stubs it
//! with `panic!`), so:
//!
//! - `arith` — an arithmetic hash, inlined and branch-predictable. A floor:
//!   it measures decode cost with the liveness check all but free.
//! - `hashset` — a `HashSet` probe, which pays a hash and a likely cache miss
//!   per entry, as a doc-table lookup does. Closer, still not the real thing.
//!
//! Read the gap between them as the range the real predicate falls in, and
//! treat neither as a substitute for measuring the wired-up write path.

use std::collections::HashSet;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::{
    IndexFlags, IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreByteOffsets,
    IndexFlags_Index_StoreFieldFlags, IndexFlags_Index_StoreFreqs,
    IndexFlags_Index_StoreTermOffsets,
};
use index_result::{RSIndexResult, RSOffsetSlice};
use inverted_index::{
    Encoder, InvertedIndex, RepairContext, doc_ids_only::DocIdsOnly, full::Full, numeric::Numeric,
};
use query_term::RSQueryTerm;

/// Flags for a full-text index carrying frequencies, field masks, and offsets —
/// what the [`Full`] encoder expects, and what an `FT.CREATE` TEXT field produces.
const FULL_FLAGS: IndexFlags = IndexFlags_Index_StoreFreqs
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_StoreFieldFlags
    | IndexFlags_Index_StoreByteOffsets;

/// Deletion arrangements to measure, ordered cheapest-looking to most punishing
/// for the lazy re-encode in `IndexBlock::repair`.
///
/// `Prefix(0)` — a wholly clean block — is the case any triggering heuristic hits
/// most often, and the one whose cost is pure waste.
const PATTERNS: [DeadPattern; 6] = [
    DeadPattern::Prefix(0),
    DeadPattern::Prefix(5),
    DeadPattern::Prefix(25),
    DeadPattern::Prefix(50),
    DeadPattern::EveryNth(10),
    DeadPattern::LastOnly,
];

/// Type-annotated `None` for `scan_gc`'s optional repair callback.
fn no_repair_cb() -> Option<fn(&RSIndexResult, &RepairContext<'_>)> {
    None
}

/// Which documents in a block of `n` entries (doc IDs `1..=n`) are deleted.
///
/// The position of the *first* dead entry, not just how many are dead, drives
/// the cost: `IndexBlock::repair` only starts re-encoding once it sees one, and
/// replays the surviving prefix from the buffer at that point. So a block whose
/// first entry is dead has no prefix to replay, and one whose last entry is dead
/// replays everything.
#[derive(Clone, Copy)]
enum DeadPattern {
    /// The first `pct`% of doc IDs. Survivors stay contiguous and the prefix
    /// replay has nothing to do — the cheapest arrangement.
    Prefix(u64),
    /// Only the final entry. Worst case for the replay: every other entry is
    /// decoded twice.
    LastOnly,
    /// Every `k`-th entry. The first dead entry appears early, but survivors are
    /// interleaved, which is what a real update workload produces.
    EveryNth(u64),
}

impl DeadPattern {
    fn label(self) -> String {
        match self {
            Self::Prefix(pct) => format!("prefix-{pct}%"),
            Self::LastOnly => "last-only".to_string(),
            Self::EveryNth(k) => format!("every-{k}th"),
        }
    }

    /// Whether `doc_id` (in `1..=n`) is still live.
    const fn is_live(self, doc_id: u64, n: u64) -> bool {
        match self {
            Self::Prefix(pct) => doc_id > n * pct / 100,
            Self::LastOnly => doc_id != n,
            Self::EveryNth(k) => !doc_id.is_multiple_of(k),
        }
    }
}

/// Append one record for `doc_id`, in whichever shape the encoder accepts.
///
/// Each encoder validates the record variant it is handed — the [`Numeric`]
/// encoder panics on a term record — so the record shape is bound to the
/// encoding here rather than at the call sites.
fn add_full(ii: &mut InvertedIndex<Full>, doc_id: u64) {
    ii.add_record(
        &RSIndexResult::build_term()
            .borrowed_record(
                Some(RSQueryTerm::new("bench", 1, 0)),
                RSOffsetSlice::from_slice(&[1, 2, 3, 4]),
            )
            .doc_id(doc_id)
            .field_mask(1u128)
            .frequency(1)
            .build(),
    )
    .unwrap();
}

fn add_docids(ii: &mut InvertedIndex<DocIdsOnly>, doc_id: u64) {
    ii.add_record(&RSIndexResult::build_term().doc_id(doc_id).build())
        .unwrap();
}

fn add_numeric(ii: &mut InvertedIndex<Numeric>, doc_id: u64) {
    ii.add_record(
        &RSIndexResult::build_numeric(doc_id as f64 / 10.0)
            .doc_id(doc_id)
            .build(),
    )
    .unwrap();
}

/// Measure a full-block repair for one encoding, across dead-entry proportions
/// and both liveness predicates.
///
/// `new_index` supplies an empty index with the flags the encoding needs, and
/// `add_one` appends a record the encoder accepts. Blocks are filled to
/// [`Encoder::RECOMMENDED_BLOCK_ENTRIES`], the count that fills one block
/// without starting a second.
fn bench_repair<E, N, A>(c: &mut Criterion, encoding: &str, new_index: N, add_one: A)
where
    E: Encoder + inverted_index::DecodedBy,
    N: Fn() -> InvertedIndex<E>,
    A: Fn(&mut InvertedIndex<E>, u64),
{
    let n = u64::from(E::RECOMMENDED_BLOCK_ENTRIES);
    let build = |count: u64| {
        let mut ii = new_index();
        for doc_id in 1..=count {
            add_one(&mut ii, doc_id);
        }
        ii
    };

    let mut group = c.benchmark_group(format!("tail_block_repair/{encoding}"));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(800));

    for pattern in PATTERNS {
        let label = pattern.label();
        let ii = build(n);
        assert_eq!(
            ii.number_of_blocks(),
            1,
            "{encoding}: {n} records must occupy exactly one block for this bench to measure a \
             single block repair — RECOMMENDED_BLOCK_ENTRIES or the block-splitting rule changed"
        );

        // Floor: liveness check is a couple of ALU ops.
        group.bench_function(BenchmarkId::new("arith", &label), |b| {
            b.iter(|| {
                black_box(
                    ii.scan_gc(|doc_id: u64| pattern.is_live(doc_id, n), no_repair_cb())
                        .unwrap(),
                )
            })
        });

        // Closer to `DocTable_Exists`: hash plus a probable cache miss per entry.
        let live: HashSet<u64> = (1..=n).filter(|&id| pattern.is_live(id, n)).collect();
        group.bench_function(BenchmarkId::new("hashset", &label), |b| {
            b.iter(|| {
                black_box(
                    ii.scan_gc(|doc_id: u64| live.contains(&doc_id), no_repair_cb())
                        .unwrap(),
                )
            })
        });
    }

    // The comparison the gate turns on: one `add_record` onto an almost-full
    // tail block — the write that would carry an inline repair. Setup builds
    // `n - 1` records and is excluded from the measurement by `iter_batched`.
    group.bench_function(BenchmarkId::new("add_record", "1 record"), |b| {
        b.iter_batched(
            || build(n - 1),
            |mut ii| add_one(&mut ii, n),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn benchmark_tail_block_repair(c: &mut Criterion) {
    bench_repair::<Full, _, _>(
        c,
        "Full",
        || InvertedIndex::<Full>::new(FULL_FLAGS),
        add_full,
    );
    bench_repair::<DocIdsOnly, _, _>(
        c,
        "DocIdsOnly",
        || InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        add_docids,
    );
    bench_repair::<Numeric, _, _>(
        c,
        "Numeric",
        || InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly),
        add_numeric,
    );
}

criterion_group!(benches, benchmark_tail_block_repair);
criterion_main!(benches);
