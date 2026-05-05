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
pub mod test_utils;

pub use score_batch::VecSimScoreBatch;
pub use source::VectorScoreSource;

use std::{cmp::Ordering, ffi::CStr, num::NonZeroUsize, ptr::NonNull};

use ffi::{VecSearchMode_HYBRID_ADHOC_BF, VecSearchMode_HYBRID_BATCHES};
use redis_reply::MapBuilder;
use rqe_iterators::{ExpirationChecker, FieldExpirationChecker, RQEIterator};
use rqe_iterators::{c2rust::call_print_profile, profile_print::ProfilePrintCtx};
use top_k::{TopKIterator, TopKMode, TopKSourceProfile};

/// A [`TopKIterator`] parameterised over [`VectorScoreSource`].
///
/// Use [`new_vector_top_k_unfiltered`] or [`new_vector_top_k_filtered`]
/// to construct one; these constructors encode the mode-selection logic.
///
/// `E` is the [`ExpirationChecker`] strategy, defaulting to the production
/// [`FieldExpirationChecker`].
pub type VectorTopKIterator<'index, E = FieldExpirationChecker> =
    TopKIterator<'index, VectorScoreSource<'index, E>>;

/// Ascending comparator — lower distance score is better (vector L2/IP/Cosine).
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Construct a pure KNN [`VectorTopKIterator`] (no filter child).
///
/// Results are streamed directly from the VecSim batch without a heap,
/// using the [`TopKMode::Unfiltered`] path.
pub fn new_vector_top_k_unfiltered<'index, E: ExpirationChecker + 'index>(
    source: VectorScoreSource<'index, E>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
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
pub fn new_vector_top_k_filtered<'index, E: ExpirationChecker + 'index>(
    source: VectorScoreSource<'index, E>,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
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
pub fn new_vector_top_k_filtered_boxed<'index, E: ExpirationChecker + 'index>(
    source: VectorScoreSource<'index, E>,
    child: Box<dyn RQEIterator<'index> + 'index>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index, E> {
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

impl<E: ExpirationChecker> TopKSourceProfile for VectorScoreSource<'_, E> {
    fn print_profile(
        &self,
        mode: TopKMode,
        switches: usize,
        map: &mut MapBuilder<'_>,
        ctx: &mut ProfilePrintCtx<'_>,
    ) {
        map.kv_simple_string(c"Type", c"VECTOR");
        ctx.print_optional_counters(map);

        let mode_cstr: &CStr = match mode {
            TopKMode::Unfiltered => c"STANDARD_KNN",
            TopKMode::Batches | TopKMode::ForcedBatches => c"HYBRID_BATCHES",
            TopKMode::AdhocBF if switches > 0 => c"HYBRID_BATCHES_TO_ADHOC_BF",
            TopKMode::AdhocBF => c"HYBRID_ADHOC_BF",
        };
        map.kv_simple_string(c"Vector search mode", mode_cstr);

        let is_batch_mode = matches!(mode, TopKMode::Batches | TopKMode::ForcedBatches)
            || (matches!(mode, TopKMode::AdhocBF) && switches > 0);
        if is_batch_mode {
            map.kv_long_long(c"Batches number", self.num_iterations as i64);
            map.kv_long_long(c"Largest batch size", self.max_batch_size as i64);
            map.kv_long_long(
                c"Largest batch iteration (zero based)",
                self.max_batch_iteration as i64,
            );
        }

        if let Some(child) = NonNull::new(self.child_raw) {
            let mut child_map = map.kv_map(c"Child iterator");
            let mut child_ctx = ctx.child_ctx();
            // SAFETY: `child_raw` was preserved at construction and points to a
            // valid `QueryIterator` whose `PrintProfile` vtable entry was set
            // by `Profile_AddIters` before this call.
            unsafe { call_print_profile(child, &mut child_map, &mut child_ctx) };
        }
    }
}
