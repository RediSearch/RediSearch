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

pub mod score_batch;
pub mod source;

pub use score_batch::NumericScoreBatch;
pub use source::NumericScoreSource;

use std::{cmp::Ordering, num::NonZeroUsize};

use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, TopKMode};

/// A [`TopKIterator`] parameterised over [`NumericScoreSource`].
///
/// Construct one with [`new_numeric_top_k_unfiltered`] or
/// [`new_numeric_top_k_filtered`].
pub type NumericTopKIterator<'index, S> = TopKIterator<'index, NumericScoreSource<'index, S>>;

/// Ascending comparator — lower numeric value is "better" (`SORTBY field ASC`).
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Descending comparator — higher numeric value is "better" (`SORTBY field DESC`).
fn desc_cmp(a: f64, b: f64) -> Ordering {
    asc_cmp(a, b).reverse()
}

/// Pick the heap comparator for the query's sort direction.
fn cmp_for(ascending: bool) -> fn(f64, f64) -> Ordering {
    if ascending { asc_cmp } else { desc_cmp }
}

/// Construct an unfiltered [`NumericTopKIterator`] (no filter child).
///
/// Every record is fed through the heap, which retains the top `k` by numeric
/// value: a numeric source has no native top-k, so the heap ([`TopKMode::Batches`]
/// with no child) performs the selection. `ascending` selects the sort
/// direction (`SORTBY field ASC`/`DESC`).
pub fn new_numeric_top_k_unfiltered<'index, S: RQEIterator<'index> + 'index>(
    source: NumericScoreSource<'index, S>,
    k: NonZeroUsize,
    ascending: bool,
) -> NumericTopKIterator<'index, S> {
    TopKIterator::new_with_mode(source, None, k, cmp_for(ascending), TopKMode::Batches)
}

/// Construct a filtered [`NumericTopKIterator`] with a filter child.
///
/// Uses [`TopKMode::Batches`]: the source's batch is intersected with the
/// child filter, and the heap keeps the top `k` by numeric value.
pub fn new_numeric_top_k_filtered<'index, S: RQEIterator<'index> + 'index>(
    source: NumericScoreSource<'index, S>,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
    ascending: bool,
) -> NumericTopKIterator<'index, S> {
    TopKIterator::new_with_mode(
        source,
        Some(Box::new(child) as Box<dyn RQEIterator<'index> + 'index>),
        k,
        cmp_for(ascending),
        TopKMode::Batches,
    )
}
