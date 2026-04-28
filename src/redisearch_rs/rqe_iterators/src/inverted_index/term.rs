/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RS_FIELDMASK_ALL, RedisSearchCtx, t_docId};
use inverted_index::{RSIndexResult, RSOffsetSlice, TermReader};
use query_term::RSQueryTerm;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    expiration_checker::ExpirationChecker,
};

use super::core::InvIndIterator;

/// An iterator over term inverted index entries.
///
/// This iterator can be used to query a term inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
/// * `E` - The expiration checker type used to check for expired documents.
pub struct Term<'index, R, E = crate::expiration_checker::NoOpChecker> {
    it: InvIndIterator<'index, R, E>,
}

impl<'index, R, E> Term<'index, R, E>
where
    R: TermReader<'index>,
    E: ExpirationChecker,
{
    /// Create an iterator returning results from a term inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`inverted_index::FilterMaskReader`].
    ///
    /// `term` is the query term that brought up this iterator. It is stored
    /// in the result and persists across all reads.
    ///
    /// `weight` is the scoring weight applied to the result record. It is
    /// typically derived from the query node and used by the scoring function
    /// to scale the relevance of results from this iterator.
    ///
    /// `expiration_checker` is used to check for expired documents when reading from the inverted index.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    pub unsafe fn new(
        reader: R,
        context: NonNull<RedisSearchCtx>,
        mut term: Box<RSQueryTerm>,
        weight: f64,
        expiration_checker: E,
    ) -> Self {
        // Compute IDF scores on the term.
        // SAFETY: 1. guarantee context is valid.
        let context_ref = unsafe { context.as_ref() };
        // SAFETY: 2. guarantee spec is valid.
        let spec = unsafe { &*context_ref.spec };
        let total_docs = spec.stats.scoring.numDocuments;
        let term_docs = reader.unique_docs() as usize;
        term.set_idf(idf::calculate_idf(total_docs, term_docs));
        term.set_bm25_idf(idf::calculate_idf_bm25(total_docs, term_docs));

        let result = RSIndexResult::build_term()
            .borrowed_record(Some(term), RSOffsetSlice::empty())
            .field_mask(RS_FIELDMASK_ALL)
            .weight(weight)
            .build();
        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
        }
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &R {
        &self.it.reader
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The term's inverted index may have been garbage-collected and
    /// replaced with a new allocation. If the index pointer returned by
    /// [`ffi::Redis_OpenInvertedIndex`] no longer matches the reader's
    /// stored index, the iterator must [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    unsafe fn should_abort(&self, context: NonNull<RedisSearchCtx>) -> bool {
        // SAFETY: 1. guarantees `context` is valid.
        let sctx_ref = unsafe { context.as_ref() };
        // SAFETY: 2. guarantees `spec` is a valid, non-null pointer.
        let spec = unsafe { &*sctx_ref.spec };

        // Redis_OpenInvertedIndex() relies on keysDict to open the II.
        // It should always be set in production flows but some tests do not set up a full spec.
        if spec.keysDict.is_null() {
            return false;
        }

        let term = self
            .it
            .result
            .as_term()
            .expect("Term iterator should always have a term result")
            .query_term()
            .expect("Term iterator should always have a query term");

        let str_ptr = term
            .as_bytes()
            .map_or(std::ptr::null(), |b| b.as_ptr().cast());

        // SAFETY: 1. guarantees `context` is a valid `RedisSearchCtx` and
        // `str_ptr` is a valid byte slice of `term.len()` bytes.
        let idx = unsafe {
            ffi::Redis_OpenInvertedIndex(
                context.as_ptr(),
                str_ptr,
                term.len(),
                false,
                std::ptr::null_mut(),
            )
        };

        let Some(idx) = NonNull::new(idx) else {
            // The inverted index was collected entirely by GC.
            return true;
        };

        let opaque = idx.cast::<inverted_index::opaque::InvertedIndex>();
        // SAFETY: `Redis_OpenInvertedIndex` returned a non-null pointer to a
        // valid opaque `InvertedIndex`.
        let opaque = unsafe { opaque.as_ref() };
        !self.it.reader.points_to_the_same_opaque_index(opaque)
    }
}

#[cfg(feature = "unittest")]
impl<'index, Enc: inverted_index::DecodedBy, E>
    Term<'index, inverted_index::FilterMaskReader<inverted_index::IndexReaderCore<'index, Enc>>, E>
{
    /// Swap the underlying inverted index of the reader.
    ///
    /// Used by tests to trigger [revalidation](RQEIterator::revalidate).
    pub const fn swap_index(&mut self, index: &mut &'index inverted_index::InvertedIndex<Enc>) {
        self.it.reader.swap_index(index);
    }
}

impl<'index, R, E> RQEIterator<'index> for Term<'index, R, E>
where
    R: TermReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        ctx: NonNull<RedisSearchCtx>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: The caller (result processor) guarantees conditions 1-2
        // of `should_abort` (`ctx` points to a valid `RedisSearchCtx` with a
        // valid `spec`) while the spec read lock is held.
        if unsafe { self.should_abort(ctx) } {
            return Ok(RQEValidateStatus::Aborted);
        }

        // SAFETY: Delegating to inner iterator with the same `ctx` passed by our caller.
        unsafe { self.it.revalidate(ctx) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxTerm
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}
