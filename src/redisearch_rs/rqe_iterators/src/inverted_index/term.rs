/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::RedisSearchCtx;
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    FilterMaskReader, IndexReader, IndexReaderCore, TermReader, block_max_score::BlockScorer,
    doc_ids_only::DocIdsOnly, fields_offsets, fields_only, freqs_fields, freqs_offsets, freqs_only,
    full, offsets_only, opaque::InvertedIndex, raw_doc_ids_only::RawDocIdsOnly,
};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
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
            .query_term()
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
    fn read_with_threshold(
        &mut self,
        min_score: f64,
        scorer: &BlockScorer,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read_with_threshold(min_score, scorer)
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
/// it.
///
/// Encodings that track a field mask filter their records through a
/// [`FilterMaskReader`]; encodings without field-mask data use the bare
/// [`IndexReaderCore`].
pub enum TermIndexReader<'index> {
    // Field-mask-tracking encodings (filtered through `FilterMaskReader`).
    Full(FilterMaskReader<IndexReaderCore<'index, full::Full>>),
    FullWide(FilterMaskReader<IndexReaderCore<'index, full::FullWide>>),
    FreqsFields(FilterMaskReader<IndexReaderCore<'index, freqs_fields::FreqsFields>>),
    FreqsFieldsWide(FilterMaskReader<IndexReaderCore<'index, freqs_fields::FreqsFieldsWide>>),
    FieldsOnly(FilterMaskReader<IndexReaderCore<'index, fields_only::FieldsOnly>>),
    FieldsOnlyWide(FilterMaskReader<IndexReaderCore<'index, fields_only::FieldsOnlyWide>>),
    FieldsOffsets(FilterMaskReader<IndexReaderCore<'index, fields_offsets::FieldsOffsets>>),
    FieldsOffsetsWide(FilterMaskReader<IndexReaderCore<'index, fields_offsets::FieldsOffsetsWide>>),
    // Encodings without field-mask data (no `FilterMaskReader`).
    FreqsOnly(IndexReaderCore<'index, freqs_only::FreqsOnly>),
    OffsetsOnly(IndexReaderCore<'index, offsets_only::OffsetsOnly>),
    FreqsOffsets(IndexReaderCore<'index, freqs_offsets::FreqsOffsets>),
    DocIdsOnly(IndexReaderCore<'index, DocIdsOnly>),
    RawDocIdsOnly(IndexReaderCore<'index, RawDocIdsOnly>),
}

macro_rules! term_ir_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            TermIndexReader::Full(r) => r.$method($($args),*),
            TermIndexReader::FullWide(r) => r.$method($($args),*),
            TermIndexReader::FreqsFields(r) => r.$method($($args),*),
            TermIndexReader::FreqsFieldsWide(r) => r.$method($($args),*),
            TermIndexReader::FieldsOnly(r) => r.$method($($args),*),
            TermIndexReader::FieldsOnlyWide(r) => r.$method($($args),*),
            TermIndexReader::FieldsOffsets(r) => r.$method($($args),*),
            TermIndexReader::FieldsOffsetsWide(r) => r.$method($($args),*),
            TermIndexReader::FreqsOnly(r) => r.$method($($args),*),
            TermIndexReader::OffsetsOnly(r) => r.$method($($args),*),
            TermIndexReader::FreqsOffsets(r) => r.$method($($args),*),
            TermIndexReader::DocIdsOnly(r) => r.$method($($args),*),
            TermIndexReader::RawDocIdsOnly(r) => r.$method($($args),*),
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

    #[inline(always)]
    fn current_block_max_score(&self, scorer: &BlockScorer) -> f64 {
        term_ir_dispatch!(self, current_block_max_score, scorer)
    }

    #[inline(always)]
    fn advance_to_next_promising_block(&mut self, min_score: f64, scorer: &BlockScorer) -> bool {
        term_ir_dispatch!(self, advance_to_next_promising_block, min_score, scorer)
    }
}

impl<'index> TermReader<'index> for TermIndexReader<'index> {
    fn points_to_the_same_opaque_index(
        &self,
        opaque: &inverted_index::opaque::InvertedIndex,
    ) -> bool {
        term_ir_dispatch!(self, points_to_the_same_opaque_index, opaque)
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
