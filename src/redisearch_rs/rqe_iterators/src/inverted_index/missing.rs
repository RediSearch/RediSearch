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

use ffi::RedisSearchCtx;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RawIndexReaderCore,
    opaque::OpaqueEncoding,
};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, FieldIndex, RS_FIELDMASK_ALL};

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};

use crate::{
    ExpirationChecker, FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed,
    RQEIteratorError, RQEIteratorPrintable, RQESuspendedIterator, RQEValidateStatus, ResumeOutcome,
    SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{InvIndIterator, core::RawInvIndIterator, core::ResumeStatus};

/// An iterator over documents that are missing a specific field, parameterised
/// over a [`Ref`] mode. See [`Missing`] for the [`Active`] instantiation that
/// implements [`RQEIterator`].
///
/// Used for `ismissing(@field)` queries, where the goal is to match every
/// document that does not have the specified field indexed. The set of such
/// documents is maintained per-field in the index spec's `missingFieldDict`.
///
/// This iterator supports per-field expiration checks via
/// [`FieldExpirationChecker`] using the
/// [`Missing`](field::FieldExpirationPredicate::Missing) predicate.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode (see [`RawInvIndIterator`] for details).
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
/// * `C` - The expiration checker type.
#[repr(C)]
pub struct RawMissing<
    'query,
    Rf: Ref,
    E: DecodedBy,
    C = crate::expiration_checker::NoOpChecker,
    // Frozen active reader for the inner iterator's dispatch pointers; hardcoded
    // to the `Active` reader regardless of `Rf` (see `RawInvIndIterator`'s `RA`).
    RA = RawIndexReaderCore<Active<'query>, E>,
> {
    it: RawInvIndIterator<'query, Rf, RawIndexReaderCore<Rf, E>, C, RA>,
    field_index: FieldIndex,
    /// Owned copy of the field name, extracted from the spec at construction
    /// time. Owning the string means the iterator no longer borrows from
    /// `spec.fields`, therefore `context`/`spec` only need to be valid at
    /// construction time (not for the iterator's entire lifetime).
    field_name: CString,
}

/// Alias for an [`Active`] [`RawMissing`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Missing<'index, E, C = crate::expiration_checker::NoOpChecker> =
    RawMissing<'index, Active<'index>, E, C>;

impl<'query, Rf: Ref, E: DecodedBy, C, RA> RawMissing<'query, Rf, E, C, RA> {
    /// Cached [`IndexFlags`](ffi::IndexFlags) of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`].
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }

    /// Get the field name tracked by this missing-field iterator.
    /// The field name is stored as an owned [`CString`].
    pub fn field_name(&self) -> (*const c_char, usize) {
        (self.field_name.as_ptr(), self.field_name.as_bytes().len())
    }
}

impl<'query, Rf: Ref, E, C, RA> RawMissing<'query, Rf, E, C, RA>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from the
    /// missing-field inverted index or replace it with a new allocation.
    /// In both cases the reader's pointer is stale and the iterator
    /// must [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// 1. `self.field_index` must be a valid index into `spec.fields`.
    /// 2. `spec.missingFieldDict` must be a non-null, valid dict pointer.
    /// 3. The entry in `missingFieldDict` for `spec.fields[field_index].fieldName`,
    ///    when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex) whose encoding
    ///    variant matches `E`.
    fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
        debug_assert!(
            !spec.missing_field_dict().is_null(),
            "spec.missing_field_dict() must be non-null",
        );

        // SAFETY: field_index is a valid index into spec.fields (guaranteed by constructor pre-conditions).
        let field_ptr = unsafe { spec.fields_ptr().offset(self.field_index as isize) };
        // SAFETY: the pointer is valid per the above.
        let field = unsafe { &*field_ptr };
        // SAFETY: The dict is non-null and valid (guaranteed by constructor pre-conditions).
        let missing_ii_ptr =
            unsafe { ffi::RS_dictFetchValue(spec.missing_field_dict(), field.fieldName as *mut _) };

        if missing_ii_ptr.is_null() {
            // The inverted index was removed from the dict (garbage collected).
            return true;
        }

        let missing_ii = missing_ii_ptr.cast::<inverted_index::opaque::InvertedIndex>();
        // SAFETY: 3. guarantees the encoding variant matches E.
        let ii = E::from_opaque(unsafe { &*missing_ii });

        !self.it.reader.points_to_ii(ii)
    }
}

impl<'index, E, C> Missing<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
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
        field_index: FieldIndex,
        expiration_checker: C,
    ) -> Self {
        debug_assert!(
            // SAFETY: pre-condition 1 guarantees `context` is valid.
            !unsafe { context.as_ref() }.spec.is_null(),
            "pre-condition 2: context.spec must be non-null",
        );
        let result = RSIndexResult::build_virt()
            .weight(0.0)
            .field_mask(RS_FIELDMASK_ALL)
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

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E, C> RQEIterator<'index> for Missing<'index, E, C>
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
        // Conditions (field_index validity, missingFieldDict, encoding
        // match) are structural invariants guaranteed by the constructor's
        // pre-conditions.
        if self.should_abort(spec) {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxMissing
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, E, C> ProfilePrint for Missing<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"MISSING");
        let field_bytes = self.field_name.as_bytes();
        if !field_bytes.is_empty() {
            map.kv_string_buffer(c"Field", field_bytes);
        }
        ctx.print_optional_counters(map);
        map.kv_long_long(c"Estimated number of matches", self.num_estimated() as i64);
    }
}

/// Create a missing-field iterator from an opaque [`InvertedIndex`](inverted_index::opaque::InvertedIndex),
/// dispatching on the encoding type.
///
/// # Safety
///
/// 1. `sctx` must point to a valid [`RedisSearchCtx`] whose `spec` is
///    non-null and valid.
/// 2. `field_index` must be a valid index into `sctx.spec.fields`.
/// 3. `sctx.spec.missingFieldDict` must be a non-null, valid dict pointer.
/// 4. The opaque inverted index must use either
///    [`DocIdsOnly`](inverted_index::doc_ids_only::DocIdsOnly) or
///    [`RawDocIdsOnly`](inverted_index::raw_doc_ids_only::RawDocIdsOnly)
///    encoding.
pub unsafe fn new_missing_iterator<'index>(
    ii: &'index inverted_index::opaque::InvertedIndex,
    sctx: NonNull<RedisSearchCtx>,
    field_index: FieldIndex,
) -> Box<dyn RQEIteratorPrintable<'index> + 'index> {
    let filter_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Missing,
    };

    match ii {
        inverted_index::opaque::InvertedIndex::DocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: caller guarantees sctx and spec validity (1-3).
            let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };
            // SAFETY: caller guarantees sctx, spec, field_index, and
            // missingFieldDict validity (1-3).
            Box::new(unsafe { Missing::new(reader, sctx, field_index, checker) })
        }
        inverted_index::opaque::InvertedIndex::RawDocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: caller guarantees sctx and spec validity (1-3).
            let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };
            // SAFETY: caller guarantees sctx, spec, field_index, and
            // missingFieldDict validity (1-3).
            Box::new(unsafe { Missing::new(reader, sctx, field_index, checker) })
        }
        _ => panic!(
            "Missing iterator requires a DocIdsOnly or RawDocIdsOnly inverted index, got: {:?}",
            std::mem::discriminant(ii)
        ),
    }
}

impl<'index, E, C> RQEIteratorBoxed<'index> for Missing<'index, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
{
    type Suspended = RawMissing<'index, Suspended, E, C>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawMissing` is a `#[repr(C)]` newtype whose only
        // `Rf`-dependent field is the inner `RawInvIndIterator`, layout-identical
        // across modes by invariant 1 on [`RawInvIndIterator`] (const proof
        // there); `field_index` and `field_name` carry no `Rf`. `Box::from_raw`
        // reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawMissing<'index, Suspended, E, C>) }
    }
}

impl<'query, E, C, RA> RQESuspendedIterator<'query> for RawMissing<'query, Suspended, E, C, RA>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
{
    type Resumed<'a>
        = Missing<'a, E, C>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: identity check on the suspended form. On abort we drop the
        // suspended iterator without promoting it to Active — nothing is
        // materialized.
        if self.should_abort(spec) {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: run the shared in-place resume transition on the inner
        // core iterator (refresh pointers, reset stale offsets, promote the
        // result, and re-seek if GC moved us). `spec` witnesses the read lock
        // the refresh requires.
        let status = self.it.resume_in_place(spec)?;

        // Step 3: reinterpret the owning box's type. The heap address is
        // preserved across the cast.
        let raw = Box::into_raw(self);
        // SAFETY: `RawMissing` is a `#[repr(C)]` newtype whose only
        // `Rf`-dependent field is the inner `RawInvIndIterator`, layout-identical
        // across modes by invariant 1 on [`RawInvIndIterator`] (const proof
        // there); `field_index` and `field_name` carry no `Rf`. `resume_in_place`
        // left the inner iterator as a valid active iterator, so the whole
        // `RawMissing` is now a valid `Missing<'a, E, C>`.
        let active = unsafe { Box::from_raw(raw as *mut Missing<'a, E, C>) };

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
