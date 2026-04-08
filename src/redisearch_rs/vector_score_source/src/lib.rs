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
//! All VecSim FFI knowledge is confined here; `top_k` and `rqe_iterators` remain
//! VecSim-free.
//!
//! # Type alias
//!
//! ```text
//! pub type VectorTopKIterator<'index> = TopKIterator<'index, VectorScoreSource>;
//! ```
//!
//! # Constructors
//!
//! - [`new_vector_top_k_unfiltered`] — pure KNN, no child filter.
//! - [`new_vector_top_k_filtered`] — hybrid, child filter provided; mode
//!   (Batches vs AdhocBF) chosen automatically via VecSim heuristics.

pub mod batch_cursor;
pub mod source;

pub use batch_cursor::VecSimScoreBatchCursor;
pub use source::VectorScoreSource;

use std::{cmp::Ordering, num::NonZeroUsize};

use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, TopKMode};

/// A [`TopKIterator`] parameterised over [`VectorScoreSource`].
///
/// Use [`new_vector_top_k_unfiltered`] or [`new_vector_top_k_filtered`]
/// to construct one; these constructors encode the mode-selection logic.
pub type VectorTopKIterator<'index> = TopKIterator<'index, VectorScoreSource>;

/// Ascending comparator — lower distance score is better (vector L2/IP/Cosine).
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// Construct a pure KNN [`VectorTopKIterator`] (no filter child).
///
/// Results are streamed directly from the VecSim batch without a heap,
/// using the [`TopKMode::Unfiltered`] path.
pub fn new_vector_top_k_unfiltered(
    source: VectorScoreSource,
    k: NonZeroUsize,
) -> VectorTopKIterator<'static> {
    TopKIterator::new_with_mode(source, None, k, asc_cmp, TopKMode::Unfiltered)
}

/// Construct a hybrid [`VectorTopKIterator`] with a filter child.
///
/// The initial mode (Batches vs AdhocBF) is chosen via
/// [`VecSimIndex_PreferAdHocSearch`] using the child's estimated result
/// count. The source may switch modes mid-execution via
/// [`CollectionStrategy::SwitchToAdhoc`].
///
/// The child is boxed.
/// Use [`new_vector_top_k_filtered_boxed`] when you already have a `Box`.
///
/// [`VecSimIndex_PreferAdHocSearch`]: ffi::VecSimIndex_PreferAdHocSearch
/// [`CollectionStrategy::SwitchToAdhoc`]: top_k::CollectionStrategy::SwitchToAdhoc
pub fn new_vector_top_k_filtered<'index>(
    source: VectorScoreSource,
    child: impl RQEIterator<'index> + 'index,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index> {
    new_vector_top_k_filtered_boxed(source, Box::new(child), k)
}

/// Construct a hybrid [`VectorTopKIterator`] with a boxed filter child.
///
/// Accepts an already-boxed `Box<dyn RQEIterator>`, avoiding an extra
/// allocation when the caller already holds one.
pub fn new_vector_top_k_filtered_boxed<'index>(
    source: VectorScoreSource,
    child: Box<dyn RQEIterator<'index> + 'index>,
    k: NonZeroUsize,
) -> VectorTopKIterator<'index> {
    let child_est = child.num_estimated().min(source.index_size());
    let mode = if source.prefer_adhoc(child_est, k.get(), true) {
        TopKMode::AdhocBF
    } else {
        TopKMode::Batches
    };
    TopKIterator::new_with_mode(source, Some(child), k, asc_cmp, mode)
}
