/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CString, c_char},
    ptr::NonNull,
};

use ffi::{RedisSearchCtx, t_docId, t_fieldIndex};
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReaderCore, RSIndexResult, opaque::OpaqueEncoding,
};

use crate::{
    ExpirationChecker, IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus,
    SkipToOutcome,
};

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
    field_index: t_fieldIndex,
    /// Owned copy of the field name, extracted from the spec at construction
    /// time. Owning the string means the iterator no longer borrows from
    /// `spec.fields`, therefore `context`/`spec` only need to be valid at
    /// construction time (not for the iterator's entire lifetime).
    field_name: CString,
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
    /// 3. `field_index` must be a valid index into `context.spec.fields`.
    /// 4. `context.spec.missingFieldDict` must be a non-null, valid dict pointer.
    /// 5. The entry in `missingFieldDict` for `spec.fields[field_index].fieldName`,
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
        let result = RSIndexResult::build_virt()
            .weight(0.0)
            .field_mask(ffi::RS_FIELDMASK_ALL)
            .frequency(1)
            .build();

        // Copy the field name into an owned CString so the iterator does not
        // borrow into spec.fields beyond construction.
        // SAFETY: pre-conditions 1, 2, and 3 guarantee context, spec, and field_index validity.
        let field_name = {
            // SAFETY: pre-condition 1 guarantees `context` points to a valid `RedisSearchCtx`.
            let sctx = unsafe { context.as_ref() };
            // SAFETY: pre-condition 2 guarantees `spec` is non-null and valid.
            let spec = unsafe { &*sctx.spec };
            if spec.fields.is_null() {
                CString::default()
            } else {
                // SAFETY: pre-condition 3 guarantees `field_index` is in bounds.
                let field_ptr = unsafe { spec.fields.add(field_index as usize) };
                // SAFETY: `field_ptr` is valid per the above bounds guarantee.
                let field = unsafe { &*field_ptr };
                let mut len = 0;
                // SAFETY: `field.fieldName` is valid per spec field validity.
                let name = unsafe { ffi::HiddenString_GetUnsafe(field.fieldName, &mut len) };
                // SAFETY: `name` points to `len` valid bytes (per HiddenString contract).
                let bytes = unsafe { std::slice::from_raw_parts(name.cast::<u8>(), len) };
                CString::new(bytes).expect("field name contains interior nul byte")
            }
        };

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            field_index,
            field_name,
        }
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from the missing-field
    /// inverted index or replace it with a new allocation. In both cases the
    /// reader's pointer is stale and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// 1. `self.field_index` must be a valid index into `spec.fields`.
    /// 2. `spec.missingFieldDict` must be a non-null, valid dict pointer.
    /// 3. The entry in `missingFieldDict` for `spec.fields[field_index].fieldName`,
    ///    when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex) whose encoding
    ///    variant matches `E`.
    unsafe fn should_abort(&self, spec: &ffi::IndexSpec) -> bool {
        debug_assert!(
            !spec.missingFieldDict.is_null(),
            "spec.missingFieldDict must be non-null",
        );

        // SAFETY: 1. guarantees `field_index` is a valid index into `spec.fields`.
        let field_ptr = unsafe { spec.fields.offset(self.field_index as isize) };
        // SAFETY: the pointer is valid per the above.
        let field = unsafe { &*field_ptr };
        // SAFETY: 2. guarantees the dict is non-null and valid.
        let missing_ii_ptr =
            unsafe { ffi::RS_dictFetchValue(spec.missingFieldDict, field.fieldName as *mut _) };

        if missing_ii_ptr.is_null() {
            // The inverted index was removed from the dict (garbage collected).
            return true;
        }

        let missing_ii = missing_ii_ptr.cast::<inverted_index::opaque::InvertedIndex>();
        // SAFETY: 3. guarantees the encoding variant matches E.
        let ii = E::from_opaque(unsafe { &*missing_ii });

        !self.it.reader.points_to_ii(ii)
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }

    /// Get the field name tracked by this missing-field iterator.
    pub fn field_name(&self) -> (*const c_char, usize) {
        (self.field_name.as_ptr(), self.field_name.as_bytes().len())
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
    unsafe fn revalidate(
        &mut self,
        spec: NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: The caller guarantees `spec` points to a valid `IndexSpec`
        // while the spec read lock is held.
        let spec_ref = unsafe { spec.as_ref() };
        // SAFETY: `spec_ref` satisfies `should_abort`'s safety requirements.
        // Conditions 1-3 (field_index validity, missingFieldDict, encoding
        // match) are structural invariants guaranteed by the constructor's
        // pre-conditions.
        if unsafe { self.should_abort(spec_ref) } {
            return Ok(RQEValidateStatus::Aborted);
        }

        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { self.it.revalidate(spec) }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxMissing
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}
