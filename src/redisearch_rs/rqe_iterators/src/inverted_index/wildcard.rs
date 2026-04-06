/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RedisSearchCtx, t_docId};
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReaderCore, RSIndexResult, opaque::OpaqueEncoding,
};

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    expiration_checker::NoOpChecker,
};

use super::core::InvIndIterator;

/// An iterator over all existing documents in an index.
///
/// Used for wildcard queries (`*`), where the goal is to match every document
/// rather than filtering by a specific term or numeric range. The set of
/// existing documents is maintained by the index spec in its `existingDocs`
/// inverted index.
///
/// Unlike [`super::Term`] and [`super::Numeric`], this iterator does not support
/// per-field expiration checks — it always uses [`NoOpChecker`].
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
pub struct Wildcard<'index, E: DecodedBy> {
    it: InvIndIterator<'index, IndexReaderCore<'index, E>>,
}

impl<'index, E> Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Create an iterator returning all documents from the `existingDocs`
    /// inverted index.
    ///
    /// `weight` is the score weight applied to every returned result.
    pub fn new(reader: IndexReaderCore<'index, E>, weight: f64) -> Self {
        use ffi::RS_FIELDMASK_ALL;

        let result = RSIndexResult::build_virt()
            .weight(weight)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .build();

        Self {
            // Wildcard iterator does not support expiration check
            it: InvIndIterator::new(reader, result, NoOpChecker),
        }
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may either null out `existingDocs` (after
    /// collecting all documents) or replace it with a new allocation. In
    /// both cases the reader's pointer is stale and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec).
    /// 3. `spec.existingDocs`, when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::InvertedIndex) whose encoding
    ///    variant matches `E`.
    unsafe fn should_abort(&self, context: NonNull<RedisSearchCtx>) -> bool {
        // SAFETY: 1. guarantees `context` is valid.
        let sctx_ref = unsafe { context.as_ref() };
        // SAFETY: 2. guarantees `spec` is a valid, non-null pointer.
        let spec = unsafe { &*sctx_ref.spec };

        let existing_docs = spec
            .existingDocs
            .cast::<inverted_index::opaque::InvertedIndex>();
        if existing_docs.is_null() {
            // the garbage collector may set existing_docs to NULL after garbage collecting all documents
            return true;
        }

        // SAFETY: 3. guarantees `existingDocs` is valid when non-null, and we just checked it's not null.
        let existing_docs = unsafe { &*existing_docs };
        // SAFETY: 3. guarantees the encoding variant matches E.
        let ii = E::from_opaque(existing_docs);

        !self.it.reader.points_to_ii(ii)
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E> RQEIterator<'index> for Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
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
    fn revalidate(
        &mut self,
        ctx: NonNull<RedisSearchCtx>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: The caller (result processor) guarantees conditions 1-2
        // of `should_abort` (`ctx` points to a valid `RedisSearchCtx` with a
        // valid `spec`) while the spec read lock is held. Condition 3
        // (existingDocs encoding match) is a structural invariant: the
        // encoding is determined at index creation and cannot change.
        if unsafe { self.should_abort(ctx) } {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(ctx)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxWildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}
