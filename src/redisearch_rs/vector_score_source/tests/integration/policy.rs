/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Constructor mode-selection tests: how `new_vector_top_k_filtered`
//! maps the requested `HYBRID_POLICY` (or its absence) onto a [`TopKMode`].

use std::num::NonZeroUsize;

use ffi::{VecSearchMode_EMPTY_MODE, VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES};
use top_k::{BatchStrategy, ScoreSource, TopKMode};
use vector_score_source::new_vector_top_k_filtered;
use vector_score_source::test_utils::{TestIndex, make_child, uniform_blob};

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_adhoc_policy() {
    let index = TestIndex::flat(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source =
        index.source_with_mode(uniform_blob(0.0, 1), 0, VecSearchMode_HYBRID_ADHOC_BF, 3, 3);
    let it = new_vector_top_k_filtered(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
        false,
    );
    assert_eq!(it.mode(), TopKMode::AdhocBF);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_batches_policy() {
    let index = TestIndex::flat(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source =
        index.source_with_mode(uniform_blob(0.0, 1), 0, VecSearchMode_HYBRID_BATCHES, 3, 3);
    let it = new_vector_top_k_filtered(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
        false,
    );
    assert_eq!(it.mode(), TopKMode::ForcedBatches);
}

/// With no explicit policy the constructor consults the cost heuristic, which
/// yields the switchable `Batches` or `AdhocBF` — never the forced variant.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unset_policy_uses_heuristic() {
    let index = TestIndex::flat(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source = index.source_with_mode(uniform_blob(0.0, 1), 0, VecSearchMode_EMPTY_MODE, 3, 3);
    let it = new_vector_top_k_filtered(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
        false,
    );
    assert!(
        matches!(it.mode(), TopKMode::Batches | TopKMode::AdhocBF),
        "heuristic path must not force batches; got {:?}",
        it.mode()
    );
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn nearly_complete_batches_continue_but_eventually_fall_back() {
    let (n, dim, k) = (60_000, 76, 200);
    let index = TestIndex::hnsw_with_metric(n, dim, ffi::VecSimMetric_VecSimMetric_Cosine);
    let mut source = index.source(uniform_blob(1.0, dim), 10, k, n);

    assert!(!source.prefer_adhoc(n, k, true));
    assert_eq!(source.batch_strategy(k - 1, k), BatchStrategy::Continue);
    // Empty batches must not discard 199 retained results merely because the
    // shrinking filter estimate would favour BF for a new K=200 search.
    for _ in 0..4 {
        assert_eq!(source.batch_strategy(k - 1, k), BatchStrategy::Continue);
    }
    // The filter may contain only 199 matches: continuation must remain bounded.
    assert_eq!(
        source.batch_strategy(k - 1, k),
        BatchStrategy::SwitchToAdhoc
    );

    source.rewind();
    assert_eq!(source.batch_strategy(0, k), BatchStrategy::Continue);
    assert_eq!(source.batch_strategy(0, k), BatchStrategy::Continue);
    assert_eq!(source.batch_strategy(0, k), BatchStrategy::SwitchToAdhoc);
    assert_eq!(source.batch_strategy(k, k), BatchStrategy::Stop);
}
