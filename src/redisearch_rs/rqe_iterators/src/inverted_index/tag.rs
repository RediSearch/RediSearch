/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RedisSearchCtx, TagIndex};
use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RawIndexReaderCore,
    opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;
use ref_mode::{Active, Ref};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{InvIndIterator, core::RawInvIndIterator};

/// Resolves a tag value to the inverted index currently stored for it in a
/// tag-index container.
pub trait TagLookup<E> {
    /// The inverted index currently stored for `tag`, if any.
    fn find(&self, tag: &[u8]) -> Option<&inverted_index::InvertedIndex<E>>;
}

/// [`TagLookup`] over the C `TagIndex`'s opaque `TrieMap` (`tag_index.values`).
pub struct CTagIndexLookup(NonNull<TagIndex>);

impl CTagIndexLookup {
    /// Create a lookup over the given C [`TagIndex`].
    ///
    /// # Safety
    ///
    /// 1. `tag_index` must point to a valid [`TagIndex`] and remain valid for
    ///    the lifetime of this lookup (and of any iterator holding it).
    /// 2. `tag_index.values`, when non-null, must be a valid
    ///    [`TrieMapOpaque`](trie_rs::TrieMapOpaque) pointer.
    /// 3. The entries in `tag_index.values` must point to opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex)es whose
    ///    encoding variant matches the `E` this lookup is used with.
    pub const unsafe fn new(tag_index: NonNull<TagIndex>) -> Self {
        Self(tag_index)
    }
}

impl<E> TagLookup<E> for CTagIndexLookup
where
    E: OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
{
    fn find(&self, tag: &[u8]) -> Option<&inverted_index::InvertedIndex<E>> {
        // SAFETY: 1. in `Self::new` guarantees `tag_index` is valid.
        let tag_idx = unsafe { self.0.as_ref() };
        if tag_idx.values.is_null() {
            // No values trie means no postings for any tag.
            return None;
        }
        // SAFETY: 2. in `Self::new` guarantees `values` is a valid `TrieMapOpaque`.
        let trie = unsafe { &*tag_idx.values.cast::<trie_rs::TrieMapOpaque>() };
        let idx = trie.find(tag)?;
        // SAFETY: 3. in `Self::new` guarantees the trie entry points to a valid
        // opaque `InvertedIndex`.
        let opaque = idx.cast::<inverted_index::opaque::InvertedIndex>().as_ptr();
        // SAFETY: 3. `from_opaque` panics when the encoding
        // variant doesn't match `E`.
        Some(E::from_opaque(unsafe { &*opaque }))
    }
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
pub struct RawTag<
    'query,
    Rf: Ref,
    E,
    C = crate::expiration_checker::NoOpChecker,
    L = CTagIndexLookup,
> {
    it: RawInvIndIterator<'query, Rf, RawIndexReaderCore<Rf, E>, C>,
    lookup: L,
}

/// Alias for an [`Active`] [`RawTag`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Tag<'index, E, C = crate::expiration_checker::NoOpChecker, L = CTagIndexLookup> =
    RawTag<'index, Active<'index>, E, C, L>;

impl<'index, E, C, L> Tag<'index, E, C, L>
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
    /// `lookup` must resolve tags in the container that holds the inverted
    /// index `reader` reads from.
    ///
    /// `weight` is the scoring weight applied to the result record.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
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

    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from a tag value's
    /// inverted index or replace it with a new allocation. In both cases the
    /// reader's pointer is stale and the iterator must
    /// [`abort`](RQEValidateStatus::Aborted).
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

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E, C, L> RQEIterator<'index> for Tag<'index, E, C, L>
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

impl<'index, E, C, L> ProfilePrint for Tag<'index, E, C, L>
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
