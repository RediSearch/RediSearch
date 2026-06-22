/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Constructor mode-selection tests: how `new_vector_top_k_filtered_boxed`
//! maps the requested `HYBRID_POLICY` (or its absence) onto a [`TopKMode`].

use std::num::NonZeroUsize;

use ffi::{
    VecSearchMode_EMPTY_MODE, VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES,
    VecSimIndex_Free,
};
use top_k::TopKMode;
use vector_score_source::new_vector_top_k_filtered_boxed;
use vector_score_source::test_support::{
    build_flat_index, make_child, make_source_with_mode, uniform_blob,
};

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_adhoc_policy() {
    let index = build_flat_index(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source = unsafe {
        make_source_with_mode(
            index,
            uniform_blob(0.0, 1),
            0,
            VecSearchMode_HYBRID_ADHOC_BF,
            3,
            3,
        )
    };
    let it = new_vector_top_k_filtered_boxed(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
    );
    assert_eq!(it.mode(), TopKMode::AdhocBF);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn explicit_batches_policy() {
    let index = build_flat_index(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source = unsafe {
        make_source_with_mode(
            index,
            uniform_blob(0.0, 1),
            0,
            VecSearchMode_HYBRID_BATCHES,
            3,
            3,
        )
    };
    let it = new_vector_top_k_filtered_boxed(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
    );
    assert_eq!(it.mode(), TopKMode::ForcedBatches);

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// With no explicit policy the constructor consults the cost heuristic, which
/// yields the switchable `Batches` or `AdhocBF` — never the forced variant.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unset_policy_uses_heuristic() {
    let index = build_flat_index(5, 1);
    // SAFETY: index is freed after the iterator is dropped at end of scope.
    let source = unsafe {
        make_source_with_mode(
            index,
            uniform_blob(0.0, 1),
            0,
            VecSearchMode_EMPTY_MODE,
            3,
            3,
        )
    };
    let it = new_vector_top_k_filtered_boxed(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
    );
    assert!(
        matches!(it.mode(), TopKMode::Batches | TopKMode::AdhocBF),
        "heuristic path must not force batches; got {:?}",
        it.mode()
    );

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}
