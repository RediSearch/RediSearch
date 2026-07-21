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
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RawIndexReaderCore,
    opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{InvIndIterator, core::RawInvIndIterator, core::ResumeStatus};

/// Resolves a tag value to the inverted index currently stored for it in a
/// tag-index container.
pub trait TagLookup<E> {
    /// The inverted index currently stored for `tag`, if any.
    fn find(&self, tag: &[u8]) -> Option<&inverted_index::InvertedIndex<E>>;
}

/// An iterator over documents matching a specific tag value, parameterised
/// over a [`Ref`] mode. See [`Tag`] for the [`Active`] instantiation that
/// implements [`RQEIterator`].
///
/// Used for tag queries where the goal is to match every document that has
/// a specific tag value indexed.
///
/// This iterator supports per-field expiration checks via
/// [`FieldExpirationChecker`](crate::FieldExpirationChecker) using the
/// [`Default`](field::FieldExpirationPredicate::Default) predicate.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode (see [`RawInvIndIterator`] for details).
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
/// * `C` - The expiration checker type.
/// * `L` - The [`TagLookup`] used to detect GC changes during revalidation.
#[repr(C)]
pub struct RawTag<'query, Rf: Ref, E, L, C = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<'query, Rf, RawIndexReaderCore<Rf, E>, C>,
    lookup: L,
}

/// Alias for an [`Active`] [`RawTag`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Tag<'index, E, L, C = crate::expiration_checker::NoOpChecker> =
    RawTag<'index, Active<'index>, E, L, C>;

impl<'query, Rf: Ref, E, L, C> RawTag<'query, Rf, E, L, C> {
    /// Cached [`IndexFlags`](ffi::IndexFlags) of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`].
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }
}

impl<'query, Rf: Ref, E, L, C> RawTag<'query, Rf, E, L, C>
where
    E: DecodedBy,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    L: TagLookup<E>,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from a tag value's
    /// inverted index or replace it with a new allocation. In both cases the
    /// reader's pointer is stale and the iterator must
    /// [`abort`](RQEValidateStatus::Aborted).
    ///
    /// Defined on the `Rf`-generic [`RawTag`] so the suspended form can run it
    /// from [`RQESuspendedIterator::resume`] before promoting to [`Active`].
    fn should_abort(&self) -> bool {
        let term = self
            .it
            .result
            .as_term()
            .expect("Tag iterator should always have a term result")
            .query_term()
            .expect("Tag iterator should always have a query term");

        let term_bytes = term
            .as_bytes()
            .expect("Tag iterator query term should have a non-null string");

        match self.lookup.find(term_bytes) {
            // The inverted index was collected entirely by GC, or the
            // value is a null sentinel (disk mode).
            None => true,
            Some(ii) => !self.it.reader.points_to_ii(ii),
        }
    }
}

impl<'index, E, L, C> Tag<'index, E, L, C>
where
    E: DecodedBy + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
    L: TagLookup<E>,
{
    /// Create an iterator returning documents matching the given tag value.
    ///
    /// `term` is the query term representing the tag value. It is stored in the
    /// result and used during revalidation to look up the tag's inverted index
    /// through `lookup`.
    ///
    /// `weight` is the scoring weight applied to the result record.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    /// 3. `lookup` must resolve tags in the container that holds the inverted
    ///    index `reader` reads from. Otherwise revalidation may wrongly
    ///    conclude the reader's index is still alive after GC freed it,
    ///    leading to a use-after-free.
    pub unsafe fn new(
        reader: IndexReaderCore<'index, E>,
        context: NonNull<RedisSearchCtx>,
        lookup: L,
        mut term: Box<RSQueryTerm>,
        weight: f64,
        expiration_checker: C,
    ) -> Self {
        // Compute IDF scores on the term.
        // SAFETY: 1. guarantees context is valid.
        let context_ref = unsafe { context.as_ref() };
        debug_assert!(!context_ref.spec.is_null(), "context.spec must be non-null",);
        // SAFETY: 2. guarantees spec is valid.
        let spec = unsafe { &*context_ref.spec };
        let total_docs = spec.stats.scoring.numDocuments;
        let term_docs = reader.unique_docs() as usize;
        term.set_idf(idf::calculate_idf(total_docs, term_docs));
        term.set_bm25_idf(idf::calculate_idf_bm25(total_docs, term_docs));

        // The trie entry's encoding variant must match E.
        debug_assert!(
            {
                let term_bytes = term
                    .as_bytes()
                    .expect("Tag iterator query term should have a non-null string");
                let _ = lookup.find(term_bytes);
                true
            },
            "the lookup entry for the tag value must have an encoding variant matching E",
        );

        let result = RSIndexResult::build_term()
            .borrowed_record(Some(term), RSOffsetSlice::empty())
            .doc_id(0)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .weight(weight)
            .build();

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            lookup,
        }
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E, L, C> RQEIterator<'index> for Tag<'index, E, L, C>
where
    E: DecodedBy + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
    L: TagLookup<E>,
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
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxTag
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, E, L, C> ProfilePrint for Tag<'index, E, L, C>
where
    E: inverted_index::DecodedBy + 'index,
    <E as inverted_index::DecodedBy>::Decoder: inverted_index::DocIdsDecoder,
    C: crate::expiration_checker::ExpirationChecker,
    L: TagLookup<E>,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"TAG");
        if let Some(term_bytes) = self.it.query_term_bytes() {
            map.kv_string_buffer(c"Term", term_bytes);
        }
        ctx.print_optional_counters(map);
        map.kv_long_long(c"Estimated number of matches", self.num_estimated() as i64);
    }
}

impl<'index, E, L, C> RQEIteratorBoxed<'index> for Tag<'index, E, L, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
    L: TagLookup<E> + 'static,
{
    // The inner reader is concretely `RawIndexReaderCore<Rf, E>`, so the core
    // iterator's frozen `RA` slot defaults to the `Active` reader and stays
    // identical across the cast while the inner reader weakens.
    type Suspended = RawTag<'index, Suspended, E, L, C>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawTag` is a `#[repr(C)]` struct whose only `Rf`-dependent
        // field is the inner `RawInvIndIterator`, layout-identical across modes
        // by invariant 1 on [`RawInvIndIterator`] (const proof there); the
        // `lookup` field carries no `Rf`. `Box::from_raw` reuses the same heap
        // allocation.
        unsafe { Box::from_raw(raw as *mut RawTag<'index, Suspended, E, L, C>) }
    }
}

impl<'query, E, L, C> RQESuspendedIterator<'query> for RawTag<'query, Suspended, E, L, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
    L: TagLookup<E> + 'static,
{
    type Resumed<'a>
        = Tag<'a, E, L, C>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: identity check on the suspended form. On abort we drop the
        // suspended iterator without promoting it to Active — nothing is
        // materialized.
        if self.should_abort() {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: run the shared in-place resume transition on the inner
        // core iterator (refresh pointers, reset stale offsets, promote the
        // result, and re-seek if GC moved us). `guard` witnesses the read lock
        // the refresh requires.
        let status = self.it.resume_in_place(guard)?;

        // Step 3: reinterpret the owning box's type. The heap address is
        // preserved across the cast.
        let raw = Box::into_raw(self);
        // SAFETY: `RawTag` is a `#[repr(C)]` struct whose only `Rf`-dependent
        // field is the inner `RawInvIndIterator`, layout-identical across modes
        // by invariant 1 on [`RawInvIndIterator`] (const proof there); the
        // `lookup` field carries no `Rf`. `resume_in_place` left the inner
        // iterator as a valid active iterator, so the whole `RawTag` is now a
        // valid `Tag<'a, E, L, C>`.
        let active = unsafe { Box::from_raw(raw as *mut Tag<'a, E, L, C>) };

        Ok(match status {
            ResumeStatus::Unchanged => ResumeOutcome::Ok(active),
            ResumeStatus::Moved => ResumeOutcome::Moved(active),
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }
}
