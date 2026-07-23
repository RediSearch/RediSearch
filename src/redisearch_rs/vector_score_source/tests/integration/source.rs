/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! End-to-end tests for [`VectorScoreSource`] driving a *real* VecSim index.
//!
//! Data model (mirrors the C++ `testHybridVector` reference in
//! `tests/cpptests/test_cpp_index.cpp`): document `i` is stored as the vector
//! `[i, i, i, i]` and the query vector is `[max_id; d]` under the L2 metric.
//! Distance therefore decreases monotonically with the doc id, so the nearest
//! neighbours are simply the highest ids — making every expected ordering
//! trivially predictable.

use std::{num::NonZeroUsize, ptr::NonNull};

use std::collections::HashSet;

use ffi::{VecSimIndex, VecSimIndex_Free, t_docId};
use index_result::RSResultKind;
use rqe_iterators::{ExpirationChecker, NoOpChecker, RQEIterator};
use rqe_iterators_test_utils::MockExpirationChecker;
use top_k::{TopKIterator, TopKMode};
use vector_score_source::test_utils::{self, asc, collect_ids, make_child, uniform_blob};
use vector_score_source::{
    VectorScoreSource, new_vector_top_k_filtered, new_vector_top_k_unfiltered,
};

const DIM: usize = 4;

/// HNSW index of `n` vectors `[i; DIM]` at this suite's fixed dimensionality.
fn build_hnsw_index(n: usize) -> NonNull<VecSimIndex> {
    test_utils::build_hnsw_index(n, DIM)
}

/// test_utilsing the corner `[n; DIM]` with `efRuntime = n`. `child_est` seeds
/// the batch-size heuristic — production passes `child.num_estimated()`.
///
/// # Safety
///
/// `index` must outlive the returned source (and any iterator built from it).
unsafe fn make_source(
    index: NonNull<VecSimIndex>,
    n: usize,
    k: usize,
    child_est: usize,
) -> VectorScoreSource<'static, NoOpChecker> {
    // SAFETY: caller-upheld `index` lifetime; no expiration filter.
    unsafe { make_source_with_expiration(index, n, k, child_est, NoOpChecker) }
}

/// Same as [`make_source`], but installs a field-expiration filter, consulted
/// at yield time.
///
/// # Safety
///
/// `index` must outlive the returned source (and any iterator built from it).
unsafe fn make_source_with_expiration<E: ExpirationChecker>(
    index: NonNull<VecSimIndex>,
    n: usize,
    k: usize,
    child_est: usize,
    expiration: E,
) -> VectorScoreSource<'static, E> {
    // SAFETY: caller upholds the `index` lifetime contract.
    unsafe {
        test_utils::make_source_with_expiration(
            index,
            uniform_blob(n as f32, DIM),
            n,
            k,
            child_est,
            expiration,
        )
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unfiltered_returns_top_k_nearest_by_score() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: `index` is freed after the iterator is dropped at end of scope.
    let source = unsafe { make_source(index, n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // The unfiltered path streams VecSim's reply ordered by score, so the k
    // nearest neighbours come out best-first.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unfiltered_results_are_metric_kind_with_exact_distance() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // Estimate is capped at k (k < index size here).
    assert_eq!(it.num_estimated(), k);

    // Unfiltered streams by score, so results come out best-first. Each result
    // is a Metric carrying the exact squared-L2 distance DIM*(n-id)^2.
    let mut expected_id = n as t_docId;
    while let Some(res) = it.read().unwrap() {
        assert_eq!(res.kind(), RSResultKind::Metric);
        assert_eq!(res.doc_id, expected_id);
        let dist = res
            .as_numeric()
            .expect("metric result carries a numeric value");
        let want = DIM as f64 * (n as f64 - expected_id as f64).powi(2);
        assert!(
            (dist - want).abs() < 1e-3,
            "id {expected_id}: distance {dist}, expected {want}"
        );
        expected_id -= 1;
    }
    assert_eq!(expected_id, n as t_docId - k as t_docId);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn first_result_after_rewind_is_best() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Batches drains the heap best-first, so after a rewind the first read must
    // again be the single best (lowest-distance) doc: the highest id at distance 0.
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::Batches,
    );

    let _ = collect_ids(&mut it);
    it.rewind();

    assert_eq!(it.num_estimated(), k);
    let res = it.read().unwrap().expect("a result after rewind");
    assert_eq!(res.doc_id, n as t_docId);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_full_child_yields_best_first() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Public constructor auto-selects batches vs adhoc; either way the heap is
    // drained best-first, so ordering is deterministic.
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    // Best (lowest distance) first → highest ids first.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_batches_partial_child_intersects() {
    let n = 100;
    let k = 10;
    let step = 4;
    let index = build_hnsw_index(n);

    let child_ids: Vec<t_docId> = (1..=n)
        .filter(|i| i % step == 0)
        .map(|i| i as t_docId)
        .collect();
    // Seed the heuristic with the real child size so the heap fills within the
    // first (large) batch — mirroring how production sizes batches. Passing a
    // too-large estimate would shrink batches and force many passes, where
    // VecSim's approximate batch iterator can re-surface high-scoring docs.
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::Batches,
    );

    // Only multiples of `step` pass the filter; best-first gives the top-k of
    // those.
    let ids = collect_ids(&mut it);
    let expected: Vec<t_docId> = (0..k).map(|c| (n - step * c) as t_docId).collect();
    assert_eq!(ids, expected);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_adhoc_matches_batches() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    // Force the adhoc-BF path (RAM lookups under shared locks) and verify it
    // produces the same ranking as the batches path.
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::AdhocBF,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_adhoc_drops_nan_distance_docs() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // The adhoc-BF path looks up a distance per child id. Ids that were never
    // added to the vector index have no label, so VecSim returns NaN — the
    // source must drop them. Mixing real ids with phantom ids (> n) exercises
    // that filter against a real index, no mock needed.
    let real: Vec<t_docId> = (91..=100).collect();
    let phantom: Vec<t_docId> = vec![201, 202, 203];
    let mut child_ids = real.clone();
    child_ids.extend(&phantom);
    child_ids.sort_unstable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    // AdhocBF drives per-id lookups; the NaN drop lives only on this path
    // (Batches intersects against VecSim's real-id stream, so phantom ids
    // never reach the distance lookup).
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::AdhocBF,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, real.iter().rev().copied().collect::<Vec<_>>());
    for p in phantom {
        assert!(
            !ids.contains(&p),
            "phantom id {p} with NaN distance must be filtered"
        );
    }

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn rewind_replays_same_results() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, n) };
    let child = make_child((1..=n as t_docId).collect());
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::Batches,
    );

    let first = collect_ids(&mut it);
    assert!(it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    let second = collect_ids(&mut it);
    assert_eq!(first, second);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn disjoint_child_yields_nothing() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // No filter id exists in the index, so the intersection is empty.
    let child_ids = vec![1000, 2000, 3000];
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, n, k, child_ids.len()) };
    let child = make_child(child_ids);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::Batches,
    );

    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unfiltered_skips_expired_docs() {
    let n = 100;
    let k = 10;
    let index = build_hnsw_index(n);

    // Mark the two nearest neighbours expired.
    let checker = MockExpirationChecker::new(HashSet::from([100, 99]));
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source_with_expiration(index, n, k, n, checker) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // The two expired nearest neighbours are
    // dropped without refill.
    let ids = collect_ids(&mut it);
    assert_eq!(ids, (91..=98).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_batches_drops_expired_without_refill() {
    let n = 100;
    let k = 3;
    let index = build_hnsw_index(n);

    // Child filter passes the candidates, so the best k are the closest
    // neighbours. The nearest is expired: it still occupies its heap slot during
    // collection (stopping batch refill), and is dropped at yield — leaving the
    // count short, not refilled from the next-best doc.
    let child_ids: Vec<t_docId> = (90..=100).collect();
    let checker = MockExpirationChecker::new(HashSet::from([100]));
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source_with_expiration(index, n, k, child_ids.len(), checker) };
    let child = make_child(child_ids);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::ForcedBatches,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, vec![99, 98]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn filtered_adhoc_drops_expired_without_refill() {
    let n = 100;
    let k = 3;
    let index = build_hnsw_index(n);

    // Same shape as the batches test, on the adhoc-BF path: the expired nearest
    // claims a heap slot during the child scan and is dropped at yield, so the
    // count shrinks instead of refilling from the next-best doc.
    let child_ids: Vec<t_docId> = (90..=100).collect();
    let checker = MockExpirationChecker::new(HashSet::from([100]));
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source_with_expiration(index, n, k, child_ids.len(), checker) };
    let child = make_child(child_ids);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::AdhocBF,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(ids, vec![99, 98]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn index_size_reflects_added_vectors() {
    let n = 42;
    let index = build_hnsw_index(n);

    // SAFETY: index outlives the source (freed at end of scope).
    let source = unsafe { make_source(index, n, 10, n) };
    assert_eq!(source.index_size(), n);

    drop(source);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}
