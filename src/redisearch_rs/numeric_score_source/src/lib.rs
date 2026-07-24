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
pub mod score_batch;
pub mod source;

pub use range_iterator::NumericRangeIterator;
pub use score_batch::NumericScoreBatch;
pub use source::{AllValid, DocValidity, NumericScoreSource};

use std::{cmp::Ordering, num::NonZeroUsize};

use rqe_iterators::{ExpirationChecker, NoOpChecker, RQEIterator};
use top_k::{TopKIterator, TopKMode};

/// A [`TopKIterator`] driven by a [`NumericScoreSource`].
///
/// Construct one with [`new_numeric_top_k_unfiltered`] or
/// [`new_numeric_top_k_filtered`].
pub type NumericTopKIterator<'index, V = AllValid, E = NoOpChecker> =
    TopKIterator<'index, NumericScoreSource<'index, V, E>>;

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
>(
    source: NumericScoreSource<'index, V, E>,
    k: NonZeroUsize,
) -> NumericTopKIterator<'index, V, E> {
    let cmp = cmp_for(source.ascending());
    TopKIterator::new_with_mode(source, None, k, cmp, TopKMode::Batches)
}

/// Construct a filtered [`NumericTopKIterator`] with a filter child.
///
/// Uses [`TopKMode::Batches`]: the source's batch is intersected with the
/// child filter, and the heap keeps the top `k` by numeric value.
pub fn new_numeric_top_k_filtered<
    'index,
    V: DocValidity + 'index,
    E: ExpirationChecker + 'index,
>(
    source: NumericScoreSource<'index, V, E>,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
) -> NumericTopKIterator<'index, V, E> {
    let cmp = cmp_for(source.ascending());
    TopKIterator::new_with_mode(
        source,
        Some(Box::new(child) as Box<dyn RQEIterator<'index> + 'index>),
        k,
        cmp,
        TopKMode::Batches,
    )
}
