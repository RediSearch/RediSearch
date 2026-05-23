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
    RedisSearchCtx, TagIndex, ValidateStatus, ValidateStatus_VALIDATE_ABORTED,
    ValidateStatus_VALIDATE_OK,
};
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RawIndexReaderCore, RefreshOutcome,
    opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{InvIndIterator, core::RawInvIndIterator};

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
#[repr(C)]
pub struct RawTag<Rf: Ref, E, C = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>,
    tag_index: NonNull<TagIndex>,
}

/// Alias for an [`Active`] [`RawTag`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Tag<'index, E, C = crate::expiration_checker::NoOpChecker> = RawTag<Active<'index>, E, C>;

impl<Rf: Ref, E, C> RawTag<Rf, E, C> {
    /// Cached [`ffi::IndexFlags`] of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`]. Mode-independent.
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }
}

impl<Rf: Ref, E, C> RawTag<Rf, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from a tag value's
    /// inverted index or replace it with a new allocation. In both cases
    /// the reader's pointer is stale and the iterator must
    /// [`abort`](RQEValidateStatus::Aborted).
    ///
    /// # Why mode-independent
    ///
    /// The body reads only mode-independent state:
    /// - The cached query term via
    ///   [`RawTermRecord::query_term_owned`](index_result::RawTermRecord::query_term_owned)
    ///   (works for the `Borrowed` variant `Tag::new` constructs).
    /// - The `tag_index` `NonNull` pointer (mode-independent).
    /// - The `TrieMap` lookup of the tag value (encoding-aware but does
    ///   not touch any reader buffer).
    /// - The reader's `points_to_ii` (mode-independent: only uses
    ///   `ii.as_raw()` and `ii_unique_id`).
    ///
    /// This means `should_abort` is callable on `RawTag<Suspended, E, C>`
    /// inside `RQESuspendedIterator::resume` *before* the
    /// `Box<Suspended>` → `Box<Active<'a>>` cast.
    pub fn should_abort(&self) -> bool {
        let term = self
            .it
            .result
            .as_term()
            .expect("Tag iterator should always have a term result")
            .query_term_owned()
            .expect("Tag iterator should always have a query term");

        // Look up the tag value in the TagIndex's TrieMap.
        // SAFETY: 3. and 4. guarantee `tag_index` is valid.
        let tag_idx = unsafe { self.tag_index.as_ref() };

        debug_assert!(
            !tag_idx.values.is_null(),
            "tag_index.values must be non-null",
        );
        let term_bytes = term
            .as_bytes()
            .expect("Tag iterator query term should have a non-null string");
        // SAFETY: 5. guarantees `tag_idx.values` is a valid `triemap_ffi::TrieMap`
        // created by `NewTrieMap`.
        let trie = unsafe { &*tag_idx.values.cast::<trie_rs::TrieMapOpaque>() };

        let Some(idx) = trie.find(term_bytes) else {
            // The inverted index was collected entirely by GC, or the
            // value is a null sentinel (disk mode).
            return true;
        };

        let opaque = idx.cast::<inverted_index::opaque::InvertedIndex>().as_ptr();
        // SAFETY: 6. guarantees the encoding variant matches E.
        // `find` guarantees the pointer is non-null.
        let ii = E::from_opaque(unsafe { &*opaque });

        !self.it.reader.points_to_ii(ii)
    }
}

impl<'index, E, C> Tag<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
    C: ExpirationChecker,
{
    /// Forwarding shim: re-seek the inner [`RawInvIndIterator`] after a
    /// GC cycle invalidated the cached block offset. Used by enum-level
    /// `RQESuspendedIterator::resume` implementations in
    /// `iterators_ffi` that need to drive the active-side reseek step
    /// from outside this crate.
    pub fn reseek_after_refresh(&mut self, last_doc_id: DocId) -> ValidateStatus {
        self.it.reseek_after_refresh(last_doc_id)
    }
}

impl<E, C> RawTag<Suspended, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    C: ExpirationChecker + 'static,
{
    /// Forwarding shim: refresh the inner [`RawInvIndIterator`]'s reader
    /// pointers while still in [`Suspended`] mode. Used by enum-level
    /// `RQESuspendedIterator::resume` implementations in
    /// `iterators_ffi` that need to drive the suspended-side refresh
    /// step from outside this crate.
    pub fn refresh_pointers(&mut self) -> inverted_index::RefreshOutcome {
        self.it.refresh_pointers()
    }
}

impl<'index, E, C> Tag<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
{
    /// Create an iterator returning documents matching the given tag value.
    ///
    /// `term` is the query term representing the tag value. It is stored in the
    /// result and used during revalidation to look up the tag's inverted index
    /// in the [`TagIndex`]'s [`TrieMapOpaque`](trie_rs::TrieMapOpaque).
    ///
    /// `weight` is the scoring weight applied to the result record.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    /// 3. `tag_index` must point to a valid [`TagIndex`].
    /// 4. `tag_index` must remain valid for the lifetime of the iterator.
    /// 5. `tag_index.values` must be a valid non-null [`TrieMapOpaque`](trie_rs::TrieMapOpaque) pointer.
    /// 6. The entry in `tag_index.values` for the tag value, when non-null,
    ///    must point to an opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex) whose encoding
    ///    variant matches `E`.
    pub unsafe fn new(
        reader: IndexReaderCore<'index, E>,
        context: NonNull<RedisSearchCtx>,
        tag_index: NonNull<TagIndex>,
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

        // Check 6.: the trie entry's encoding variant matches E.
        debug_assert!(
            {
                // SAFETY: 3. and 4. guarantee tag_index is valid.
                let tag_idx = unsafe { tag_index.as_ref() };
                if !tag_idx.values.is_null() {
                    let term_bytes = term
                        .as_bytes()
                        .expect("Tag iterator query term should have a non-null string");
                    // SAFETY: 5. guarantees values is a valid TrieMap.
                    let trie = unsafe { &*tag_idx.values.cast::<trie_rs::TrieMapOpaque>() };
                    // If the entry exists, `from_opaque` panics when the variant doesn't match E.
                    if let Some(idx) = trie.find(term_bytes) {
                        let opaque = idx.cast::<inverted_index::opaque::InvertedIndex>().as_ptr();
                        // SAFETY: 6. guarantees the trie entry points to a valid opaque InvertedIndex.
                        let _ = E::from_opaque(unsafe { &*opaque });
                    }
                }
                true
            },
            "tag_index entry for the tag value must have an encoding variant matching E",
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
            tag_index,
        }
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E, C> RQEIterator<'index> for Tag<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
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

impl<'index, E, C> ProfilePrint for Tag<'index, E, C>
where
    E: inverted_index::DecodedBy
        + inverted_index::opaque::OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>
        + 'index,
    <E as inverted_index::DecodedBy>::Decoder: inverted_index::DocIdsDecoder,
    C: crate::expiration_checker::ExpirationChecker,
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

impl<'index, E, C> RQEIteratorBoxed<'index> for Tag<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
{
    type Suspended = RawTag<Suspended, E, C>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawTag` is `#[repr(C)]`. The `Rf`-dependent field is the
        // inner `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>`, whose
        // layout is identical across modes (see [`InvIndIterator::suspend`]).
        // `tag_index: NonNull<TagIndex>` carries no `Rf` and survives the
        // cast unchanged. Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawTag<Suspended, E, C>) }
    }
}

impl<E, C> RQESuspendedIterator for RawTag<Suspended, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
{
    type Resumed<'a> = Tag<'a, E, C>;

    fn resume<'a>(
        mut self: Box<Self>,
        _guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort on the suspended form. Reads only
        // mode-independent fields (cached term, `tag_index`, and the
        // reader's `points_to_ii`).
        let abort = self.should_abort();

        // Step 2: refresh reader pointers in place while still
        // suspended. Skipped on the abort path.
        let outcome = if abort {
            RefreshOutcome::Ok
        } else {
            self.it.refresh_pointers()
        };

        // Step 3: whole-box cast Suspended → Active.
        let raw = Box::into_raw(self);
        // SAFETY: `RawTag` is `#[repr(C)]` over the inner
        // `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>` and a
        // mode-independent `tag_index: NonNull<TagIndex>`. The
        // `RawInvIndIterator` layout is identical across modes (see
        // `RawInvIndIterator::suspend`). The caller's read lock
        // (witnessed by `_guard`) plus the `refresh_pointers` step
        // above together ensure every pointer inside the iterator is
        // valid for `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut Tag<'a, E, C>) };

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
}