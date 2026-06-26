/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{
    RedisSearchCtx, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK,
};
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{PointsToOpaqueIndex, RefreshOutcome, ResumableReader, SuspendableReader, TermReader};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, SkipToOutcome,
    expiration_checker::ExpirationChecker,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::core::{InvIndIterator, RawInvIndIterator};

/// An iterator over term inverted index entries, parameterised over a
/// [`Ref`] mode. See [`Term`] for the [`Active`] instantiation that
/// implements [`RQEIterator`].
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode (see [`RawInvIndIterator`] for details).
/// * `R` - The reader type used to read the inverted index.
/// * `E` - The expiration checker type used to check for expired documents.
#[repr(C)]
pub struct RawTerm<Rf: Ref, R, E = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<Rf, R, E>,
}

/// Alias for an [`Active`] [`RawTerm`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Term<'index, R, E = crate::expiration_checker::NoOpChecker> =
    RawTerm<Active<'index>, R, E>;

impl<Rf: Ref, R, E> RawTerm<Rf, R, E> {
    /// Cached [`ffi::IndexFlags`] of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`]. Mode-independent.
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }
}

impl<Rf: Ref, R: PointsToOpaqueIndex, E> RawTerm<Rf, R, E> {
    /// Check if the iterator should abort revalidation.
    ///
    /// The term's inverted index may have been garbage-collected and
    /// replaced with a new allocation. If the index pointer looked up via
    /// `spec.keysDict` no longer matches the reader's stored index, the
    /// iterator must [abort](RQEValidateStatus::Aborted).
    ///
    /// # Why mode-independent
    ///
    /// The body uses only mode-independent reads:
    /// - The cached query term is fetched via
    ///   [`RawTermRecord::query_term_owned`](index_result::RawTermRecord::query_term_owned),
    ///   which works for the `Borrowed` term-record variant that `Term::new`
    ///   constructs.
    /// - The reader is queried via [`PointsToOpaqueIndex`], whose body
    ///   ([`E::from_opaque`](inverted_index::DecodedBy) + `points_to_ii`)
    ///   reads only mode-independent reader fields (`ii.as_raw()` and
    ///   `ii_unique_id`).
    ///
    /// This means `should_abort` is callable on `RawTerm<Suspended, RS, E>`
    /// inside `RQESuspendedIterator::resume` *before* the
    /// `Box<Suspended>` → `Box<Active<'a>>` cast — no `&'a` borrow of any
    /// potentially-stale field is materialized.
    ///
    /// # Safety
    ///
    /// The raw pointers inside `spec` (e.g. `keysDict`) must be valid and
    /// dereferenceable for the duration of the call.
    pub(crate) fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
        // Redis_OpenInvertedIndex() relies on keysDict to open the II.
        // It should always be set in production flows but some tests do not set up a full spec.
        if !spec.has_keys_dict() {
            return false;
        }

        let term = self
            .it
            .result
            .as_term()
            .expect("Term iterator should always have a term result")
            .query_term_owned()
            .expect("Term iterator should always have a query term");

        let str_ptr = term
            .as_bytes()
            .map_or(std::ptr::null(), |b| b.as_ptr().cast());

        // SAFETY: spec.as_mut_ptr() points to a valid IndexSpec for the duration
        // of this call, guaranteed by the caller holding the spec read lock.
        // `str_ptr` is a valid byte slice of `term.len()` bytes.
        let idx = unsafe {
            ffi::Redis_OpenInvertedIndex(
                spec.as_mut_ptr(),
                str_ptr,
                term.len(),
                false,
                std::ptr::null_mut(),
            )
        };

        let Some(idx) = NonNull::new(idx as *mut _) else {
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
        doc_id: DocId,
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
    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort(spec) {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxTerm
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, R, E> ProfilePrint for Term<'index, R, E>
where
    R: inverted_index::TermReader<'index>,
    E: crate::expiration_checker::ExpirationChecker,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"TEXT");
        if let Some(term_bytes) = self.it.query_term_bytes() {
            map.kv_string_buffer(c"Term", term_bytes);
        }
        ctx.print_optional_counters(map);
        map.kv_long_long(c"Estimated number of matches", self.num_estimated() as i64);
    }
}

impl<'index, R, E> RQEIteratorBoxed<'index> for Term<'index, R, E>
where
    R: TermReader<'index> + SuspendableReader + 'index,
    R::Suspended: ResumableReader + PointsToOpaqueIndex,
    for<'a> <R::Suspended as ResumableReader>::Resumed<'a>: TermReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Suspended = RawTerm<Suspended, R::Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawTerm` is `#[repr(C)]` containing only a
        // `RawInvIndIterator<Rf, R, E>` field, whose layout is identical
        // across `Active`/`Suspended` instantiations (see
        // `InvIndIterator::suspend`). Box::from_raw reuses the same heap
        // allocation; the active drop won't run on the moved-out bytes.
        unsafe { Box::from_raw(raw as *mut RawTerm<Suspended, R::Suspended, E>) }
    }
}

impl<RS, E> RQESuspendedIterator for RawTerm<Suspended, RS, E>
where
    RS: ResumableReader + PointsToOpaqueIndex,
    for<'a> RS::Resumed<'a>: TermReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Resumed<'a> = Term<'a, RS::Resumed<'a>, E>;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort on the suspended form. The check reads
        // only mode-independent fields (the cached query term and the
        // reader's `ii.as_raw()` / `ii_unique_id`), so it materializes
        // no `&'a` borrow of any potentially-stale pointer.
        let abort = self.should_abort(spec);

        // Step 2: refresh reader pointers in place while still
        // suspended. Skipped on the abort path — the iterator is about
        // to be dropped and we don't want to read the stale `ii` block
        // list under a stale ABA pointer.
        let outcome = if abort {
            RefreshOutcome::Ok
        } else {
            self.it.refresh_pointers()
        };

        // Step 3: whole-box cast Suspended → Active. The heap address
        // is preserved across the cast.
        let raw = Box::into_raw(self);
        // SAFETY: `RawTerm` is `#[repr(C)]` over `RawInvIndIterator<Rf, R, E>`,
        // whose `Active`/`Suspended` instantiations are layout-compatible
        // (see `RawInvIndIterator::suspend`). The caller's read lock
        // (witnessed by `spec`) plus the `refresh_pointers` step above
        // together ensure every pointer inside the iterator is valid for
        // `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut Term<'a, RS::Resumed<'a>, E>) };

        if abort {
            return (active, ValidateStatus_VALIDATE_ABORTED);
        }

        // Step 4: if GC ran, re-seek to the previous last_doc_id.
        let status = match outcome {
            RefreshOutcome::Ok => ValidateStatus_VALIDATE_OK,
            RefreshOutcome::NeedsReseek { last_doc_id } => {
                active.it.reseek_after_refresh(last_doc_id)
            }
        };
        (active, status)
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated_field()
    }
}