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
#[expect(clippy::too_many_arguments)]
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
    let scoring = scoring_context(&term, scorer, weight, &ctx.spec().stats.scoring);
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

/// The scoring inputs of a term iterator: the IDF `scorer` reads (`BM25STD` and its
/// normalized variants use the BM25 IDF, every other scorer the TF-IDF one), the
/// node's weight and the index's average document length.
fn scoring_context(
    term: &RSQueryTerm,
    scorer: Option<BuiltInScorer>,
    weight: f64,
    stats: &ffi::ScoringIndexStats,
) -> TermScoringContext {
    let idf = match scorer {
        Some(BuiltInScorer::Bm25Std | BuiltInScorer::Bm25StdTanh | BuiltInScorer::Bm25StdNorm) => {
            term.bm25_idf()
        }
        _ => term.idf(),
    };
    TermScoringContext {
        scorer,
        idf,
        weight,
        avg_doc_len: avg_doc_len(stats),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn term() -> Box<RSQueryTerm> {
        let mut term = RSQueryTerm::new("t", 1, 0);
        term.set_idf(2.0);
        term.set_bm25_idf(3.0);
        term
    }

    fn stats(num_documents: usize, total_docs_len: usize) -> ffi::ScoringIndexStats {
        // SAFETY: `ScoringIndexStats` is plain old data; all-zero is a valid value.
        let mut stats: ffi::ScoringIndexStats = unsafe { std::mem::zeroed() };
        stats.numDocuments = num_documents as _;
        stats.totalDocsLen = total_docs_len as _;
        stats
    }

    #[test]
    fn bm25std_scorers_read_the_bm25_idf() {
        for scorer in [
            BuiltInScorer::Bm25Std,
            BuiltInScorer::Bm25StdTanh,
            BuiltInScorer::Bm25StdNorm,
        ] {
            let context = scoring_context(&term(), Some(scorer), 1.0, &stats(1, 1));
            assert_eq!(context.idf, 3.0, "{scorer:?}");
        }
    }

    #[test]
    fn other_scorers_read_the_tfidf_idf() {
        for scorer in [
            None,
            Some(BuiltInScorer::TfIdf),
            Some(BuiltInScorer::TfIdfDocNorm),
            Some(BuiltInScorer::Bm25),
            Some(BuiltInScorer::DocScore),
        ] {
            let context = scoring_context(&term(), scorer, 1.0, &stats(1, 1));
            assert_eq!(context.idf, 2.0, "{scorer:?}");
        }
    }

    #[test]
    fn context_carries_the_scorer_weight_and_average_length() {
        let context = scoring_context(&term(), Some(BuiltInScorer::TfIdf), 0.5, &stats(4, 10));
        assert_eq!(context.scorer, Some(BuiltInScorer::TfIdf));
        assert_eq!(context.weight, 0.5);
        assert_eq!(context.avg_doc_len, 2.5);
        assert_eq!(
            scoring_context(&term(), None, 1.0, &stats(0, 0)).avg_doc_len,
            0.0
        );
    }
}
