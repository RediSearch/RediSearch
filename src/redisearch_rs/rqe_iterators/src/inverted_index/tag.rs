/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RS_FIELDMASK_ALL, RedisSearchCtx, TagIndex, t_docId};
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RSIndexResult, RSOffsetSlice,
    opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
};

use super::InvIndIterator;

/// An iterator over documents matching a specific tag value.
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
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
/// * `C` - The expiration checker type.
pub struct Tag<'index, E, C = crate::expiration_checker::NoOpChecker> {
    it: InvIndIterator<'index, IndexReaderCore<'index, E>, C>,
    tag_index: NonNull<TagIndex>,
}

impl<'index, E, C> Tag<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
{
    /// Create an iterator returning documents matching the given tag value.
    ///
    /// `term` is the query term representing the tag value. It is stored in the
    /// result and used during revalidation to look up the tag's inverted index
    /// in the [`TagIndex`]'s [`TrieMap`](trie_rs::opaque::TrieMap).
    ///
    /// `weight` is the scoring weight applied to the result record.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    /// 3. `tag_index` must point to a valid [`TagIndex`].
    /// 4. `tag_index` must remain valid for the lifetime of the iterator.
    /// 5. `tag_index.values` must be a valid non-null [`TrieMap`](trie_rs::opaque::TrieMap) pointer.
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
                    let trie = unsafe { &*tag_idx.values.cast::<trie_rs::opaque::TrieMap>() };
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
        let trie = unsafe { &*tag_idx.values.cast::<trie_rs::opaque::TrieMap>() };

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
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
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
        spec: NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { self.it.revalidate(spec) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxTag
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}
