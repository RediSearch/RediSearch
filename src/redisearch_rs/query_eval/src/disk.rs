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
use rqe_iterators::{RQEIteratorPrintable, TermScoringContext};
use search_disk::SearchDiskHandle;

use crate::{QueryEvalContext, scorers::BuiltInScorer};

/// Build a search-on-disk reader for a single term, resolving the query's disk
/// snapshot and turning a failure into a query error.
///
/// `disk` must wrap the spec's own disk index. `term` must already carry its
/// scoring metadata: the document count its IDF is derived from is looked up
/// differently per caller, so computing it stays with the caller.
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
    needs_offsets: bool,
    scorer: Option<BuiltInScorer>,
) -> Option<Box<dyn RQEIteratorPrintable<'index> + 'index>> {
    let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
        .expect("query.sctx.diskSnapshot is null for a disk-backed query");
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
    // registered whenever a disk index is in use; `snapshot` is the disk
    // snapshot taken at query start.
    match unsafe { disk.new_term_iterator(term, field_mask, scoring, needs_offsets, snapshot) } {
        Ok(it) => Some(it),
        Err(err) => {
            ctx.status()
                .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
            None
        }
    }
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
