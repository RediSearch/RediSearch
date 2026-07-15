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
pub struct RawTag<'query, Rf: Ref, E, C = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<'query, Rf, RawIndexReaderCore<Rf, E>, C>,
    tag_index: NonNull<TagIndex>,
}

/// Alias for an [`Active`] [`RawTag`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Tag<'index, E, C = crate::expiration_checker::NoOpChecker> =
    RawTag<'index, Active<'index>, E, C>;

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
