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
use top_k::TopKMode;
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
