/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! VecSim-backed [`top_k::ScoreSource`] and [`VectorTopKIterator`] for top-k hybrid queries.
//!
//! # Overview
//!
//! This crate provides the concrete [`VectorScoreSource`] implementation of
//! [`top_k::ScoreSource`] that drives [`TopKIterator`] against a VecSim index.

// Force-link the umbrella `redisearch_rs` crate so the `QueryError_*` (and other)
// Rust FFI symbols that `libredisearch_all.a` calls back into are retained in the
// lib unit-test binary, which links the C archive via the `unittest` feature.
#[cfg(test)]
extern crate redisearch_rs;
// Stub the C symbols (e.g. OpenSSL) that the linked archive references but these
// unit tests never exercise, so the binary links without pulling in those libs.
#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

pub mod score_batch;
pub mod source;
#[cfg(feature = "unittest")]
pub mod test_support;

pub use score_batch::VecSimScoreBatch;
pub use source::VectorScoreSource;

use std::{cmp::Ordering, num::NonZeroUsize};

use ffi::{VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES};
use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, TopKMode};

/// A [`TopKIterator`] parameterised over [`VectorScoreSource`].
///
/// Use [`new_vector_top_k_unfiltered`] or [`new_vector_top_k_filtered`]
/// to construct one; these constructors encode the mode-selection logic.
pub type VectorTopKIterator<'index> = TopKIterator<'index, VectorScoreSource<'index>>;

/// Ascending comparator — lower distance score is better (vector L2/IP/Cosine).
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Construct a pure KNN [`VectorTopKIterator`] (no filter child).
///
/// Results are streamed directly from the VecSim batch without a heap,
/// using the [`TopKMode::Unfiltered`] path.
pub fn new_vector_top_k_unfiltered<'index>(
    source: VectorScoreSource<'index>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index> {
    TopKIterator::new_with_mode(source, None, k, asc_cmp, TopKMode::Unfiltered)
}

/// Construct a hybrid [`VectorTopKIterator`] with a filter child.
///
/// When the user pinned a policy via the `HYBRID_POLICY` query attribute
/// (reflected in [`VectorScoreSource::requested_search_mode`]), that policy is
/// honored. Otherwise the initial mode (Batches vs AdhocBF) is chosen via
/// [`VecSimIndex_PreferAdHocSearch`] using the child's estimated result count,
/// and the source may switch modes mid-execution via
/// [`BatchStrategy::SwitchToAdhoc`].
///
/// The child is boxed.
/// Use [`new_vector_top_k_filtered_boxed`] when you already have a `Box`.
///
/// [`VectorScoreSource::requested_search_mode`]: source::VectorScoreSource::requested_search_mode
/// [`VecSimIndex_PreferAdHocSearch`]: ffi::VecSimIndex_PreferAdHocSearch
/// [`BatchStrategy::SwitchToAdhoc`]: top_k::BatchStrategy::SwitchToAdhoc
pub fn new_vector_top_k_filtered<'index>(
    source: VectorScoreSource<'index>,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index> {
    new_vector_top_k_filtered_boxed(source, Box::new(child), k)
}

/// Construct a hybrid [`VectorTopKIterator`] with a boxed filter child.
///
/// Accepts an already-boxed `Box<dyn RQEIterator>`, avoiding an extra
/// allocation when the caller already holds one.
///
/// Delegates mode selection to source.
///
/// [`VecSimIndex_PreferAdHocSearch`]: ffi::VecSimIndex_PreferAdHocSearch
pub fn new_vector_top_k_filtered_boxed<'index>(
    source: VectorScoreSource<'index>,
    child: Box<dyn RQEIterator<'index> + 'index>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index> {
    // The user pinned a policy via HYBRID_POLICY: honor it verbatim. HYBRID_BATCHES
    // also suppresses the mid-run switch to adhoc — the C reader's
    // `reviewHybridSearchPolicy` returns false for it — which is exactly what
    // `ForcedBatches` (vs `Batches`) encodes. An unset policy falls back to the
    // cost heuristic, which can still switch to adhoc mid-run.
    let requested = source.requested_search_mode();
    let mode = if requested == VecSearchMode_HYBRID_ADHOC_BF {
        TopKMode::AdhocBF
    } else if requested == VecSearchMode_HYBRID_BATCHES {
        TopKMode::ForcedBatches
    } else {
        let child_est = child.num_estimated().min(source.index_size());
        if source.prefer_adhoc(child_est, k.get(), true) {
            TopKMode::AdhocBF
        } else {
            TopKMode::Batches
        }
    };
    TopKIterator::new_with_mode(source, Some(child), k, asc_cmp, mode)
}
