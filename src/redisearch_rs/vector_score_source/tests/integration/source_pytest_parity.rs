/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`VectorScoreSource`] tests ported from the Python `tests/pytests` KNN /
//! hybrid suites.
//!
//! These drive a real VecSim index but stand in for the whole query stack: a
//! Python numeric/tag/geo/text filter becomes an [`IdList`] child holding the
//! doc ids that would survive it. The source cannot tell those filters apart,
//! so one filtered-subset test covers the whole family. Each test names the
//! Python test it mirrors.
//!
//! Data model (shared with `source.rs`): unless noted, doc `i` is `[i; dim]`
//! under L2, so distance to query `[q; dim]` is `dim*(q-i)^2`.

use std::num::NonZeroUsize;

use ffi::VecSimIndex_Free;
use rqe_core::DocId;
use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, TopKMode};
use vector_score_source::test_utils::{
    asc, build_flat_cosine_index, build_flat_index, build_hnsw_index, collect_ids, make_child,
    make_source, uniform_blob,
};
use vector_score_source::{new_vector_top_k_filtered, new_vector_top_k_unfiltered};

// FLAT backend coverage (source.rs only exercises HNSW).

/// From `test_hybrid_query_batches_mode_with_text`: unfiltered KNN on FLAT.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn flat_unfiltered_returns_top_k_nearest_by_score() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    // Streamed by score: nearest first, so id 100 (distance 0) down to 91.
    assert_eq!(collect_ids(&mut it), (91..=100).rev().collect::<Vec<_>>());
    assert!(it.at_eof());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_hybrid_query_batches_mode_with_text`: filtered KNN on FLAT.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn flat_filtered_full_child_yields_best_first() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), (91..=100).rev().collect::<Vec<_>>());

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Metric coverage (source.rs only covers L2).

/// From `test_vecsim.py::test_hybrid_query_cosine`: cosine nearest = highest
/// ids. Set membership (not strict order) to absorb FLOAT32 cosine ULP ties,
/// matching the Python tolerance.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn cosine_metric_filtered_top_k_are_highest_ids() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_flat_cosine_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(1.0, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    let ids = collect_ids(&mut it);
    assert_eq!(ids.first().copied(), Some(n as DocId));
    let window: Vec<DocId> = ((n as DocId - 14)..=n as DocId).collect();
    for id in &ids {
        assert!(window.contains(id), "id {id} outside top-15 cosine window");
    }
    assert_eq!(ids.len(), k);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Non-corner query ordering (source.rs always queries the corner [n; dim]).

/// From `test_vecsim.py::test_hybrid_query_batches_mode_with_tags`: query the
/// middle id. Equal-distance pairs `(mid-d, mid+d)` resolve to the lower id
/// first (heap tie-break).
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn middle_query_orders_by_distance_then_lower_id() {
    let (n, k, dim) = (100usize, 10usize, 4usize);
    let mid = (n / 2) as DocId;
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(mid as f32, dim), n, k, n) };
    let child = make_child((1..=n as DocId).collect());
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    let mut expected = vec![mid];
    for d in 1..=4 {
        expected.push(mid - d);
        expected.push(mid + d);
    }
    expected.push(mid - 5);
    assert_eq!(collect_ids(&mut it), expected);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// dim = 1 coverage (test_ft_aggregate_basic).

/// From `test_vecsim.py::test_ft_aggregate_basic` (KNN-3 portion).
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn dim1_unfiltered_knn_top3() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), vec![1, 2, 3]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::test_ft_aggregate_basic` (hybrid `(@n:[0 5])` portion):
/// the filter passes ids 6..=10, the 3 nearest to `[0]` are 6, 7, 8.
///
/// `new_vector_top_k_filtered` auto-selects the mode, matching the real query:
/// a 10-doc index trips VecSim's small-index ADHOC_BF heuristic.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn dim1_filtered_subset_knn_top3() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    let child_ids: Vec<DocId> = (6..=10).collect();
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, child_ids.len()) };
    let child = make_child(child_ids);
    let mut it = new_vector_top_k_filtered(source, child, NonZeroUsize::new(k).unwrap());

    assert_eq!(collect_ids(&mut it), vec![6, 7, 8]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// Regression: a forced Batches scan that switches to adhoc mid-run must not
/// emit a doc id twice. The same dim-1 layout makes `batch_strategy` flip to
/// `SwitchToAdhoc` after the first batch; before the heap was cleared on that
/// switch, `collect_adhoc`'s full rescan re-admitted batch-phase docs (`[6, 6,
/// 7]` instead of `[6, 7, 8]`).
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn batches_switch_to_adhoc_yields_no_duplicates() {
    let (n, k, dim) = (10, 3, 1);
    let index = build_flat_index(n, dim);

    let child_ids: Vec<DocId> = (6..=10).collect();
    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(0.0, dim), n, k, child_ids.len()) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(child_ids)),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    let ids = collect_ids(&mut it);
    assert_eq!(
        it.mode(),
        TopKMode::AdhocBF,
        "expected mid-run switch to adhoc"
    );
    assert_eq!(ids, vec![6, 7, 8]);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

// Mode-selection heuristic (PreferAdHocSearch).

/// From `test_vecsim.py::test_hybrid_query_with_geo`: a small index with a small
/// passing subset prefers adhoc-BF.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn small_index_prefers_adhoc() {
    let (n, k, dim) = (1000, 10, 4);
    let index = build_hnsw_index(n, dim);

    let subset = 31;
    // SAFETY: index outlives the source (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, subset) };
    assert!(source.prefer_adhoc(subset, k, true));

    drop(source);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::test_hybrid_query_with_numeric`: a contiguous high
/// subset yields the same top-k under Batches and AdhocBF.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn numeric_like_subset_batches_matches_adhoc() {
    let (n, k, dim) = (200, 10, 4);
    let child_ids: Vec<DocId> = (150..=n as DocId).collect();
    let expected: Vec<DocId> = ((n as DocId - 9)..=n as DocId).rev().collect();

    for mode in [TopKMode::Batches, TopKMode::AdhocBF] {
        let index = build_hnsw_index(n, dim);
        // SAFETY: index outlives the iterator (freed at end of scope).
        let source =
            unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, child_ids.len()) };
        let mut it = TopKIterator::new_with_mode(
            source,
            Some(make_child(child_ids.clone())),
            NonZeroUsize::new(k).unwrap(),
            asc,
            mode,
        );

        assert_eq!(collect_ids(&mut it), expected, "mode {mode:?}");

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }
}

/// From `test_hybrid.py::test_knn_custom_k`: a non-default k returns exactly k.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn custom_k_returns_exactly_k() {
    let dim = 4;
    let n = 100;
    for k in [2usize, 5] {
        let index = build_hnsw_index(n, dim);
        // SAFETY: index outlives the iterator (freed at end of scope).
        let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
        let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

        // Streamed by score: nearest (highest id) first.
        let expected: Vec<DocId> = ((n as DocId - k as DocId + 1)..=n as DocId).rev().collect();
        assert_eq!(collect_ids(&mut it), expected, "k = {k}");

        drop(it);
        // SAFETY: no live references to the index remain.
        unsafe { VecSimIndex_Free(index.as_ptr()) };
    }
}
