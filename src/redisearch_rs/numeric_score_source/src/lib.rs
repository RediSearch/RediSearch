/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric-field-backed [`top_k::ScoreSource`] and [`NumericTopKIterator`] for
//! top-k optimizer queries.
//!
//! # Overview
//!
//! This crate provides [`NumericScoreSource`], a concrete [`top_k::ScoreSource`]
//! that drives [`TopKIterator`] using a numeric field's values as scores.

// Force-link the umbrella `redisearch_rs` crate so the `QueryError_*` (and other)
// Rust FFI symbols that `libredisearch_c_bundle.a` calls back into are retained in the
// lib unit-test binary, which links the C archive via the `unittest` feature.
#[cfg(test)]
extern crate redisearch_rs;
// Stub the C symbols (e.g. OpenSSL) that the linked archive references but these
// unit tests never exercise, so the binary links without pulling in those libs.
#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();

pub mod range_iterator;
pub mod reducer;
pub mod score_batch;
pub mod source;

pub use range_iterator::NumericRangeIterator;
pub use reducer::{NewNumericTopK, new_numeric_top_k};
pub use score_batch::NumericScoreBatch;
pub use source::{AllValid, DocValidity, NumericScoreSource};

use std::{cmp::Ordering, num::NonZeroUsize};

use redis_reply::MapBuilder;
use rqe_iterators::{
    ExpirationChecker, NoOpChecker, RQEIterator,
    c2rust::CRQEIterator,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::{NoTimeout, TimeoutContext},
};
use top_k::{TopKIterator, TopKMode, TopKSourceProfile};

/// A [`TopKIterator`] driven by a [`NumericScoreSource`].
///
/// Construct one with [`new_numeric_top_k_unfiltered`] or
/// [`new_numeric_top_k_filtered`]. The child parameter `I` defaults to
/// [`CRQEIterator`] so the production FFI path and tests share one iterator type.
pub type NumericTopKIterator<
    'index,
    V = AllValid,
    E = NoOpChecker,
    T = NoTimeout,
    I = CRQEIterator,
> = TopKIterator<'index, NumericScoreSource<'index, V, E, T>, I>;

/// Pick the heap comparator for the query's sort direction.
fn cmp_for(ascending: bool) -> fn(&f64, &f64) -> Ordering {
    if ascending {
        f64::total_cmp
    } else {
        |a, b| b.total_cmp(a)
    }
}

/// Construct an unfiltered [`NumericTopKIterator`] (no filter child).
///
/// Every record is fed through the heap, which retains the top `k` by numeric
/// value: a numeric source has no native top-k, so the heap ([`TopKMode::Batches`]
/// with no child) performs the selection. The sort direction is taken from the
/// `source` (`SORTBY field ASC`/`DESC`).
pub fn new_numeric_top_k_unfiltered<
    'index,
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
    T: TimeoutContext + 'index,
>(
    source: NumericScoreSource<'index, V, E, T>,
    k: NonZeroUsize,
) -> NumericTopKIterator<'index, V, E, T> {
    let cmp = cmp_for(source.ascending());
    TopKIterator::new_with_mode(source, None, k, cmp, TopKMode::Batches)
}

/// Construct a filtered [`NumericTopKIterator`] with a filter child.
///
/// Uses [`TopKMode::Batches`]: the source's batch is intersected with the
/// child filter, and the heap keeps the top `k` by numeric value. The child's
/// concrete type `C` is preserved (rather than boxed) so the production FFI path
/// yields a single [`NumericTopKIterator`] monomorphization; tests may pass any
/// [`RQEIterator`].
pub fn new_numeric_top_k_filtered<
    'index,
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
    T: TimeoutContext + 'index,
    C: RQEIterator<'index> + 'index,
>(
    source: NumericScoreSource<'index, V, E, T>,
    child: C,
    k: NonZeroUsize,
) -> NumericTopKIterator<'index, V, E, T, C> {
    let cmp = cmp_for(source.ascending());
    TopKIterator::new_with_mode(source, Some(child), k, cmp, TopKMode::Batches)
}

impl<V: DocValidity, E: ExpirationChecker, T: TimeoutContext> TopKSourceProfile
    for NumericScoreSource<'_, V, E, T>
{
    /// Render the numeric optimizer's profile entry.
    ///
    /// Mirrors the C `OptimizerIterator` header (`Type: OPTIMIZER`) and adds the
    /// batch/window counters the C reader keeps internally but never surfaces in
    /// FT.PROFILE. `mode` is unused: the numeric source has no runtime mode
    /// string, and the C `Optimizer mode` (a pre-execution query-optimization
    /// type) has no analog here.
    fn print_profile(
        &self,
        _mode: TopKMode,
        switches: usize,
        map: &mut MapBuilder<'_>,
        ctx: &mut ProfilePrintCtx<'_>,
        child: Option<&dyn ProfilePrint>,
    ) {
        map.kv_simple_string(c"Type", c"OPTIMIZER");
        ctx.print_optional_counters(map);
        map.kv_long_long(c"Batches number", self.num_batches() as i64);
        // `switches` is the iterator's strategy-switch count, which for the
        // numeric source is exactly its disjoint-window expansions.
        map.kv_long_long(c"Window expansions", switches as i64);

        if let Some(child) = child {
            let mut child_map = map.kv_map(c"Child iterator");
            let mut child_ctx = ctx.child_ctx();
            child.print_profile(&mut child_map, &mut child_ctx);
        }
    }
}
