/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RedisSearchCtx, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    FilterMaskReader, IndexReader, PointsToOpaqueIndex, RawIndexReaderCore, RefreshOutcome,
    ResumableReader, SuspendableReader, TermReader, doc_ids_only::DocIdsOnly, fields_offsets,
    fields_only, freqs_fields, freqs_offsets, freqs_only, full, offsets_only,
    opaque::InvertedIndex, raw_doc_ids_only::RawDocIdsOnly,
};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome,
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
pub struct RawTerm<'query, Rf: Ref, R, E = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<'query, Rf, R, E>,
}

/// Alias for an [`Active`] [`RawTerm`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Term<'index, R, E = crate::expiration_checker::NoOpChecker> =
    RawTerm<'index, Active<'index>, R, E>;

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

impl<'query, Rf: Ref, R, E> RawTerm<'query, Rf, R, E> {
    /// Cached [`IndexFlags`](ffi::IndexFlags) of the underlying inverted index —
    /// see [`RawInvIndIterator::flags`]. Mode-independent, so FFI introspection
    /// (`FT.PROFILE`) can read it whether the iterator is [`Active`] or
    /// [`Suspended`].
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }
}

/// `should_abort` is available in both the [`Active`] and [`Suspended`] modes:
/// [`revalidate`](RQEIterator::revalidate) calls it on the live iterator, while
/// [`resume`](RQESuspendedIterator::resume) calls it on the suspended carrier
/// before promoting back to [`Active`]. It only reads mode-independent state
/// (the cached query term and the reader's opaque-index identity, via
/// [`PointsToOpaqueIndex`]), so it materializes no `&'index` borrow of a
/// possibly-stale pointer.
impl<'query, Rf: Ref, R, E> RawTerm<'query, Rf, R, E>
where
    R: PointsToOpaqueIndex,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The term's inverted index may have been garbage-collected and
    /// replaced with a new allocation. If the index pointer looked up via
    /// `spec.keysDict` no longer matches the reader's stored index, the
    /// iterator must [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// The raw pointers inside `spec` (e.g. `keysDict`) must be valid and
    /// dereferenceable for the duration of the call.
    fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
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

/// A reader over any term-compatible inverted index encoding, erasing the
/// encoding type behind a single enum so callers don't need to be generic over
/// it. Parameterised over a [`Ref`] mode `Rf`; see [`TermIndexReader`] for the
/// [`Active`] instantiation (the live reader implementing [`IndexReader`]).
/// `RawTermIndexReader<Suspended>` is its passive carrier across a lock
/// release/reacquire cycle (implementing [`ResumableReader`]).
///
/// Encodings that track a field mask filter their records through a
/// [`FilterMaskReader`]; encodings without field-mask data use the bare
/// [`RawIndexReaderCore`].
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawTermIndexReader<Active<'index>>`
///    and `RawTermIndexReader<Suspended>` are layout-identical, so the owning
///    `RawInvIndIterator` can suspend/resume by a same-allocation reinterpretation
///    — the [`SuspendableReader`]/[`ResumableReader`] contract. This holds because
///    it is a single `#[repr(C)]` generic whose `Rf` flows only through the
///    per-variant [`RawIndexReaderCore`]/[`FilterMaskReader`] payloads, each itself
///    layout-compatible (invariant 1 on those types). Enforced by the `const _`
///    proof below.
#[repr(C)]
pub enum RawTermIndexReader<Rf: Ref> {
    // Field-mask-tracking encodings (filtered through `FilterMaskReader`).
    Full(FilterMaskReader<RawIndexReaderCore<Rf, full::Full>>),
    FullWide(FilterMaskReader<RawIndexReaderCore<Rf, full::FullWide>>),
    FreqsFields(FilterMaskReader<RawIndexReaderCore<Rf, freqs_fields::FreqsFields>>),
    FreqsFieldsWide(FilterMaskReader<RawIndexReaderCore<Rf, freqs_fields::FreqsFieldsWide>>),
    FieldsOnly(FilterMaskReader<RawIndexReaderCore<Rf, fields_only::FieldsOnly>>),
    FieldsOnlyWide(FilterMaskReader<RawIndexReaderCore<Rf, fields_only::FieldsOnlyWide>>),
    FieldsOffsets(FilterMaskReader<RawIndexReaderCore<Rf, fields_offsets::FieldsOffsets>>),
    FieldsOffsetsWide(FilterMaskReader<RawIndexReaderCore<Rf, fields_offsets::FieldsOffsetsWide>>),
    // Encodings without field-mask data (no `FilterMaskReader`).
    FreqsOnly(RawIndexReaderCore<Rf, freqs_only::FreqsOnly>),
    OffsetsOnly(RawIndexReaderCore<Rf, offsets_only::OffsetsOnly>),
    FreqsOffsets(RawIndexReaderCore<Rf, freqs_offsets::FreqsOffsets>),
    DocIdsOnly(RawIndexReaderCore<Rf, DocIdsOnly>),
    RawDocIdsOnly(RawIndexReaderCore<Rf, RawDocIdsOnly>),
}

/// Active-form alias of [`RawTermIndexReader`] — the live term reader.
pub type TermIndexReader<'index> = RawTermIndexReader<Active<'index>>;

// Compile-time proof of invariant 1 on `RawTermIndexReader`: its `Active` and
// `Suspended` instantiations are layout-identical. As an enum we assert size and
// alignment equality; the per-variant payloads are layout-compatible by their own
// invariant 1, so a divergence here would be a build error.
const _: () = {
    use std::mem::{align_of, size_of};
    type A = RawTermIndexReader<Active<'static>>;
    type S = RawTermIndexReader<Suspended>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// Dispatch a method call to the reader held by the current variant. Works for
/// any `Rf` because every variant carries the same name across modes.
macro_rules! term_ir_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            RawTermIndexReader::Full(r) => r.$method($($args),*),
            RawTermIndexReader::FullWide(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsFields(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsFieldsWide(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOnlyWide(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOffsets(r) => r.$method($($args),*),
            RawTermIndexReader::FieldsOffsetsWide(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::OffsetsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::FreqsOffsets(r) => r.$method($($args),*),
            RawTermIndexReader::DocIdsOnly(r) => r.$method($($args),*),
            RawTermIndexReader::RawDocIdsOnly(r) => r.$method($($args),*),
        }
    };
}

impl<'index> IndexReader<'index> for TermIndexReader<'index> {
    #[inline(always)]
    fn next_record(&mut self, result: &mut RSIndexResult<'index>) -> std::io::Result<bool> {
        term_ir_dispatch!(self, next_record, result)
    }

    #[inline(always)]
    fn seek_record(
        &mut self,
        doc_id: DocId,
        result: &mut RSIndexResult<'index>,
    ) -> std::io::Result<bool> {
        term_ir_dispatch!(self, seek_record, doc_id, result)
    }

    #[inline(always)]
    fn skip_to(&mut self, doc_id: DocId) -> bool {
        term_ir_dispatch!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn reset(&mut self) {
        term_ir_dispatch!(self, reset)
    }

    #[inline(always)]
    fn unique_docs(&self) -> u64 {
        term_ir_dispatch!(self, unique_docs)
    }

    #[inline(always)]
    fn has_duplicates(&self) -> bool {
        term_ir_dispatch!(self, has_duplicates)
    }

    #[inline(always)]
    fn flags(&self) -> ffi::IndexFlags {
        term_ir_dispatch!(self, flags)
    }

    #[inline(always)]
    fn needs_revalidation(&self) -> bool {
        term_ir_dispatch!(self, needs_revalidation)
    }

    #[inline(always)]
    fn refresh_buffer_pointers(&mut self) {
        term_ir_dispatch!(self, refresh_buffer_pointers)
    }
}

/// Resolve an opaque index and compare it against the current variant's reader.
/// Implemented for every `Rf` so it is callable from the suspended-side
/// `should_abort` check as well as the active reader.
impl<Rf: Ref> PointsToOpaqueIndex for RawTermIndexReader<Rf> {
    fn points_to_the_same_opaque_index(
        &self,
        opaque: &inverted_index::opaque::InvertedIndex,
    ) -> bool {
        term_ir_dispatch!(self, points_to_the_same_opaque_index, opaque)
    }
}

impl<'index> TermReader<'index> for TermIndexReader<'index> {}

/// `TermIndexReader<'index>` suspends to `RawTermIndexReader<Suspended>` — the
/// same generic enum with the ref mode weakened.
///
/// SAFETY: layout compatibility is invariant 1 on [`RawTermIndexReader`] (const
/// proof there).
unsafe impl<'index> SuspendableReader for TermIndexReader<'index> {
    type Suspended = RawTermIndexReader<Suspended>;
}

/// Inverse of the above: `RawTermIndexReader<Suspended>` resumes to
/// `TermIndexReader<'a>` at any index lifetime `'a`. `refresh_pointers` forwards
/// to the suspended reader held by the current variant.
///
/// SAFETY: layout compatibility is invariant 1 on [`RawTermIndexReader`] (const
/// proof there).
unsafe impl ResumableReader for RawTermIndexReader<Suspended> {
    type Resumed<'a> = TermIndexReader<'a>;

    unsafe fn refresh_pointers(&mut self) -> RefreshOutcome {
        // SAFETY: our caller upholds `ResumableReader::refresh_pointers`'s
        // no-concurrent-aliasing obligation, which we forward unchanged to the
        // per-variant suspended reader.
        unsafe { term_ir_dispatch!(self, refresh_pointers) }
    }
}

/// Build a term-query iterator over an in-memory inverted index.
///
/// Selects the reader for `idx`'s encoding, filters it by `field_mask_or_index`
/// (a field mask reads only matching fields; a field index reads all fields and
/// filters via the expiration checker), and returns a [`Term`] iterator that
/// yields one entry per document containing `term`. `term`'s IDF scores are
/// (re)computed from the opened index inside [`Term::new`].
///
/// # Safety
///
/// 1. `idx` must be a valid, non-null pointer to a term [`InvertedIndex`],
///    remaining valid — and stable between [`revalidate`](RQEIterator::revalidate)
///    calls — for `'index`. (Revalidation detects a GC replacement by comparing
///    the index pointer looked up via `Redis_OpenInvertedIndex`.)
/// 2. `sctx` and `sctx.spec` must be valid and remain valid for `'index`.
/// 3. `term` is a heap-allocated [`RSQueryTerm`] whose ownership is transferred
///    to the returned iterator.
///
/// # Panics
///
/// Panics if `idx` is a numeric index, which has no term reader.
pub unsafe fn build_term_iterator<'index>(
    idx: *const ffi::InvertedIndex,
    sctx: NonNull<RedisSearchCtx>,
    field_mask_or_index: FieldMaskOrIndex,
    term: Box<RSQueryTerm>,
    weight: f64,
) -> Term<'index, TermIndexReader<'index>, FieldExpirationChecker> {
    debug_assert!(!idx.is_null(), "idx must not be null");

    let idx_ptr: *const InvertedIndex = idx.cast();
    // SAFETY: precondition (1) — `idx` is a valid, non-null `InvertedIndex`.
    let idx = unsafe { &*idx_ptr };

    // A field mask reads only its fields; a field index reads all fields (index
    // filtering happens later, via the expiration checker).
    let mask = match field_mask_or_index {
        FieldMaskOrIndex::Mask(m) => m,
        FieldMaskOrIndex::Index(_) => RS_FIELDMASK_ALL,
    };

    let reader = match idx {
        InvertedIndex::Full(ii) => TermIndexReader::Full(ii.reader(mask)),
        InvertedIndex::FullWide(ii) => TermIndexReader::FullWide(ii.reader(mask)),
        InvertedIndex::FreqsFields(ii) => TermIndexReader::FreqsFields(ii.reader(mask)),
        InvertedIndex::FreqsFieldsWide(ii) => TermIndexReader::FreqsFieldsWide(ii.reader(mask)),
        InvertedIndex::FieldsOnly(ii) => TermIndexReader::FieldsOnly(ii.reader(mask)),
        InvertedIndex::FieldsOnlyWide(ii) => TermIndexReader::FieldsOnlyWide(ii.reader(mask)),
        InvertedIndex::FieldsOffsets(ii) => TermIndexReader::FieldsOffsets(ii.reader(mask)),
        InvertedIndex::FieldsOffsetsWide(ii) => TermIndexReader::FieldsOffsetsWide(ii.reader(mask)),
        InvertedIndex::FreqsOnly(ii) => TermIndexReader::FreqsOnly(ii.reader()),
        InvertedIndex::OffsetsOnly(ii) => TermIndexReader::OffsetsOnly(ii.reader()),
        InvertedIndex::FreqsOffsets(ii) => TermIndexReader::FreqsOffsets(ii.reader()),
        InvertedIndex::DocIdsOnly(ii) => TermIndexReader::DocIdsOnly(ii.reader()),
        InvertedIndex::RawDocIdsOnly(ii) => TermIndexReader::RawDocIdsOnly(ii.reader()),
        InvertedIndex::Numeric(_) | InvertedIndex::NumericFloatCompression(_) => {
            panic!("numeric inverted indices have no term reader")
        }
    };

    // SAFETY: precondition (2) — `sctx`/`sctx.spec` are valid for `'index`.
    let expiration_checker = unsafe {
        FieldExpirationChecker::new(
            sctx,
            FieldFilterContext {
                field: field_mask_or_index,
                predicate: FieldExpirationPredicate::Default,
            },
            reader.flags(),
        )
    };

    // SAFETY: `reader` was just built from the valid `idx`; preconditions (2)/(3)
    // uphold the remaining `Term::new` requirements (valid `sctx`, owned `term`).
    unsafe { Term::new(reader, sctx, term, weight, expiration_checker) }
}

impl<'index, R, E> RQEIteratorBoxed<'index> for Term<'index, R, E>
where
    R: TermReader<'index> + SuspendableReader + 'index,
    R::Suspended: ResumableReader + PointsToOpaqueIndex,
    for<'a> <R::Suspended as ResumableReader>::Resumed<'a>: TermReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Suspended = RawTerm<'index, Suspended, R::Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawTerm` is `#[repr(C)]` containing only a
        // `RawInvIndIterator<Rf, R, E>` field, whose layout is identical
        // across `Active`/`Suspended` instantiations (see
        // `InvIndIterator::suspend`). Box::from_raw reuses the same heap
        // allocation; the active drop won't run on the moved-out bytes.
        unsafe { Box::from_raw(raw as *mut RawTerm<'index, Suspended, R::Suspended, E>) }
    }
}

impl<'query, RS, E> RQESuspendedIterator<'query> for RawTerm<'query, Suspended, RS, E>
where
    RS: ResumableReader + PointsToOpaqueIndex,
    for<'a> RS::Resumed<'a>: TermReader<'a>,
    E: ExpirationChecker + 'static,
{
    type Resumed<'a>
        = Term<'a, RS::Resumed<'a>, E>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: should_abort on the suspended form. The check reads
        // only mode-independent fields (the cached query term and the
        // reader's `ii.as_raw()` / `ii_unique_id`), so it materializes
        // no `&'a` borrow of any potentially-stale pointer. On abort we
        // drop the suspended iterator without promoting it to Active —
        // nothing is materialized.
        if self.should_abort(spec) {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: refresh reader pointers in place while still
        // suspended. `spec` witnesses the read lock the refresh requires.
        let outcome = self.it.refresh_pointers(spec);

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

        // Step 4: if GC ran, re-seek to the previous last_doc_id.
        let status = match outcome {
            RefreshOutcome::Ok => ValidateStatus_VALIDATE_OK,
            RefreshOutcome::NeedsReseek { last_doc_id } => {
                active.it.reseek_after_refresh(last_doc_id)
            }
        };
        Ok(if status == ValidateStatus_VALIDATE_MOVED {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }
}
