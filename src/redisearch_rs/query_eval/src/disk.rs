/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared machinery for node types that read from a search-on-disk index.

use std::ptr::NonNull;

use query_error::QueryErrorCode;
use query_term::RSQueryTerm;
use rqe_core::FieldMask;
use rqe_iterators::{
    ConstantBound, RQEIteratorPrintable, TermScoringContext, c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
};
use search_disk::SearchDiskHandle;

use crate::{
    Config, EvalResult, QueryEvalContext,
    constant_leaf_bound::{ConstantLeaf, constant_leaf_bound},
    effective_scorer,
    scorers::BuiltInScorer,
};

/// Build a search-on-disk reader for a single term, resolving the query's disk
/// view and turning a failure into a query error.
///
/// `disk` must wrap the spec's own disk index. `term` must already carry its
/// scoring metadata: the document count its IDF is derived from is looked up
/// differently per caller, so computing it stays with the caller. `num_docs` is
/// that count; the iterator reports it as its estimate.
/// `needs_offsets` selects the offset-carrying iterator variant. `scorer` is
/// the query's built-in scorer, passed on in the iterator's
/// [`TermScoringContext`].
///
/// Returns `None` — after recording a
/// [`DiskIteratorCreation`](QueryErrorCode::DiskIteratorCreation) error, so the
/// query aborts rather than silently reading fewer results — when the iterator
/// cannot be built.
pub(crate) fn new_term_iterator<'index>(
    ctx: &'index mut QueryEvalContext,
    disk: SearchDiskHandle,
    term: Box<RSQueryTerm>,
    field_mask: FieldMask,
    weight: f64,
    num_docs: usize,
    needs_offsets: bool,
    scorer: Option<BuiltInScorer>,
) -> Option<Box<dyn RQEIteratorPrintable<'index> + 'index>> {
    let view = NonNull::new(ctx.sctx().diskQueryView)
        .expect("query.sctx.diskQueryView is null for a disk-backed query");
    let idf = match scorer {
        Some(BuiltInScorer::Bm25Std | BuiltInScorer::Bm25StdTanh | BuiltInScorer::Bm25StdNorm) => {
            term.bm25_idf()
        }
        _ => term.idf(),
    };
    let scoring = TermScoringContext {
        scorer,
        idf,
        weight,
        avg_doc_len: avg_doc_len(&ctx.spec().stats.scoring),
    };
    // SAFETY: `disk` wraps the spec's disk index, valid for `'index`
    // (`QueryEvalContext` invariants 1/2), and single-threaded query evaluation
    // gives us the only live reference to it; the enterprise iterators are
    // registered whenever a disk index is in use; `view` is the disk
    // view taken at query start.
    match unsafe {
        disk.new_term_iterator(term, field_mask, scoring, num_docs, needs_offsets, view)
    } {
        Ok(it) => Some(it),
        Err(err) => {
            ctx.status()
                .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
            None
        }
    }
}

/// The bound every document of a disk `leaf` gets under the query's scorer, or `None` when
/// the index keeps no score bounds or the scorer has none for the leaf.
///
/// Read once per leaf, when it is built: the index's maximum document score only grows, so
/// the value stays valid for the whole query.
pub(crate) fn constant_bound_for(
    ctx: &QueryEvalContext,
    config: Config,
    disk: &SearchDiskHandle,
    leaf: ConstantLeaf,
) -> Option<f64> {
    if !disk.score_skipping_enabled() {
        return None;
    }
    constant_leaf_bound(
        effective_scorer(ctx, config)?,
        leaf,
        disk.max_doc_score(),
        avg_doc_len(&ctx.spec().stats.scoring),
    )
}

/// `leaf`, reporting `max_score` for every document when it is `Some`.
pub(crate) fn with_constant_bound<'index>(
    leaf: EvalResult<'index>,
    max_score: Option<f64>,
) -> EvalResult<'index> {
    match max_score {
        Some(max_score) => Box::new(ConstantBound::new(leaf, max_score)),
        None => leaf,
    }
}

/// [`with_constant_bound`] for a leaf a C constructor built, lowered back to the C ABI.
///
/// # Safety
///
/// `leaf` must be an owning query iterator with all the callbacks [`CRQEIterator::new`]
/// requires.
pub(crate) unsafe fn with_constant_bound_c(
    leaf: NonNull<ffi::QueryIterator>,
    max_score: Option<f64>,
) -> NonNull<ffi::QueryIterator> {
    let Some(max_score) = max_score else {
        return leaf;
    };
    // SAFETY: guaranteed by the caller.
    let leaf: EvalResult<'_> = Box::new(unsafe { CRQEIterator::new(leaf) });
    // Lowered as a boxed trait object, the shape every disk leaf already has on the C side.
    RQEIteratorWrapper::boxed_new(with_constant_bound(leaf, Some(max_score)))
}

/// The average document length the C scorers read from
/// `RSIndexStats::avgDocLen`, computed the same way as
/// `RSIndexStats_FromScoringStats`.
fn avg_doc_len(stats: &ffi::ScoringIndexStats) -> f64 {
    if stats.numDocuments == 0 {
        0.0
    } else {
        stats.totalDocsLen as f64 / stats.numDocuments as f64
    }
}
