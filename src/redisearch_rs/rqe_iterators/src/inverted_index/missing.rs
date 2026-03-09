/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{RedisSearchCtx, t_docId, t_fieldIndex};
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReaderCore, RSIndexResult, opaque::OpaqueEncoding,
};

use crate::{ExpirationChecker, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

use super::InvIndIterator;

/// An iterator over documents that are missing a specific field.
///
/// Used for `ismissing(@field)` queries, where the goal is to match every
/// document that does not have the specified field indexed. The set of such
/// documents is maintained per-field in the index spec's `missingFieldDict`.
///
/// This iterator supports per-field expiration checks via
/// [`FieldExpirationChecker`](crate::FieldExpirationChecker) using the
/// [`Missing`](field::FieldExpirationPredicate::Missing) predicate.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
/// * `C` - The expiration checker type.
pub struct Missing<'index, E: DecodedBy, C = crate::expiration_checker::NoOpChecker> {
    it: InvIndIterator<'index, IndexReaderCore<'index, E>, C>,
    context: NonNull<RedisSearchCtx>,
    field_index: t_fieldIndex,
}

impl<'index, E, C> Missing<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
{
    /// Create an iterator returning documents missing the given field.
    ///
    /// `field_index` is the index of the field in `spec.fields` whose missing
    /// documents inverted index this iterator reads from.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid `IndexSpec`.
    /// 3. Both 1 and 2 must remain valid for the lifetime of the iterator.
    /// 4. `field_index` must be a valid index into `context.spec.fields`.
    /// 5. `context.spec.missingFieldDict` must be a non-null, valid dict pointer.
    /// 6. The entry in `missingFieldDict` for `spec.fields[field_index].fieldName`,
    ///    when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex) whose encoding
    ///    variant matches `E`.
    pub unsafe fn new(
        reader: IndexReaderCore<'index, E>,
        context: NonNull<RedisSearchCtx>,
        field_index: t_fieldIndex,
        expiration_checker: C,
    ) -> Self {
        debug_assert!(
            // SAFETY: pre-condition 1 guarantees `context` is valid.
            !unsafe { context.as_ref() }.spec.is_null(),
            "pre-condition 2: context.spec must be non-null",
        );
        let result = RSIndexResult::virt()
            .weight(0.0)
            .field_mask(ffi::RS_FIELDMASK_ALL)
            .frequency(1);

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            context,
            field_index,
        }
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from the missing-field
    /// inverted index or replace it with a new allocation. In both cases the
    /// reader's pointer is stale and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    fn should_abort(&self) -> bool {
        // SAFETY: 1. and 3. guarantee `context` is valid for the iterator's lifetime.
        let sctx_ref = unsafe { self.context.as_ref() };
        // SAFETY: 2. and 3. guarantee `spec` is a valid, non-null pointer for the iterator's lifetime.
        let spec = unsafe { &*sctx_ref.spec };

        debug_assert!(
            !spec.missingFieldDict.is_null(),
            "spec.missingFieldDict must be non-null",
        );

        // SAFETY: 4. guarantees `field_index` is a valid index into `spec.fields`.
        let field_ptr = unsafe { spec.fields.offset(self.field_index as isize) };
        // SAFETY: the pointer is valid per the above.
        let field = unsafe { &*field_ptr };
        // SAFETY: 5. guarantees the dict is valid.
        let missing_ii_ptr =
            unsafe { ffi::RS_dictFetchValue(spec.missingFieldDict, field.fieldName as *mut _) };

        if missing_ii_ptr.is_null() {
            // The inverted index was removed from the dict (garbage collected).
            return true;
        }

        let missing_ii = missing_ii_ptr.cast::<inverted_index::opaque::InvertedIndex>();
        // SAFETY: 6. guarantees the encoding variant matches E.
        let ii = E::from_opaque(unsafe { &*missing_ii });

        !self.it.reader.points_to_ii(ii)
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }

    /// Get a mutable reference to the underlying reader.
    ///
    /// This is used by C tests to swap the index pointer for revalidation testing.
    pub const fn reader_mut(&mut self) -> &mut IndexReaderCore<'index, E> {
        &mut self.it.reader
    }
}

impl<'index, E, C> RQEIterator<'index> for Missing<'index, E, C>
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
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate()
    }
}
