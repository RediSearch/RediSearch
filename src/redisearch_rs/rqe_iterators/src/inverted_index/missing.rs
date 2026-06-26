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

use ffi::{
    RedisSearchCtx, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK,
};
use index_result::RSIndexResult;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, RawIndexReaderCore, RefreshOutcome,
    opaque::OpaqueEncoding,
};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, FieldIndex, RS_FIELDMASK_ALL};

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};

use crate::{
    ExpirationChecker, FieldExpirationChecker, IteratorType, RQEIterator, RQEIteratorBoxed,
    RQEIteratorError, RQESuspendedIterator, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::{InvIndIterator, core::RawInvIndIterator};
use index_spec::IndexSpecReadGuard;

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
pub struct RawMissing<Rf: Ref, E: DecodedBy, C = crate::expiration_checker::NoOpChecker> {
    it: RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>,
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
    RawMissing<Active<'index>, E, C>;

impl<Rf: Ref, E: DecodedBy, C> RawMissing<Rf, E, C> {
    /// Cached [`ffi::IndexFlags`] of the underlying inverted index — see
    /// [`RawInvIndIterator::flags`]. Mode-independent.
    pub const fn flags(&self) -> ffi::IndexFlags {
        self.it.flags()
    }

    /// Get the field name tracked by this missing-field iterator.
    /// Mode-independent — the field name is stored as an owned [`CString`].
    pub fn field_name(&self) -> (*const c_char, usize) {
        (self.field_name.as_ptr(), self.field_name.as_bytes().len())
    }
}

impl<'index, E: DecodedBy + 'index, C> Missing<'index, E, C>
where
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

impl<E: DecodedBy + 'static, C: ExpirationChecker + 'static> RawMissing<Suspended, E, C>
where
    for<'a> RawIndexReaderCore<ref_mode::Active<'a>, E>: inverted_index::IndexReader<'a>,
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

impl<Rf: Ref, E, C> RawMissing<Rf, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may remove all documents from the
    /// missing-field inverted index or replace it with a new allocation.
    /// In both cases the reader's pointer is stale and the iterator
    /// must abort.
    ///
    /// # Why mode-independent
    ///
    /// The body reads only mode-independent state:
    /// - `field_index` / `spec.fields_ptr()` / `spec.missing_field_dict()`
    ///   are not affected by the iterator's `Rf`.
    /// - The reader's `points_to_ii` is mode-independent (only uses
    ///   `ii.as_raw()` and `ii_unique_id`).
    ///
    /// This means `should_abort` is callable on `RawMissing<Suspended, E, C>`
    /// inside `RQESuspendedIterator::resume` *before* the
    /// `Box<Suspended>` → `Box<Active<'a>>` cast.
    ///
    /// # Safety
    ///
    /// 1. `self.field_index` must be a valid index into `spec.fields`.
    /// 2. `spec.missingFieldDict` must be a non-null, valid dict pointer.
    /// 3. The entry in `missingFieldDict` for `spec.fields[field_index].fieldName`,
    ///    when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::opaque::InvertedIndex) whose encoding
    ///    variant matches `E`.
    pub fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
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
) -> crate::BoxedRQEIterator<'index> {
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
            crate::BoxedRQEIterator::new(Box::new(unsafe {
                Missing::new(reader, sctx, field_index, checker)
            }))
        }
        inverted_index::opaque::InvertedIndex::RawDocIdsOnly(ii) => {
            let reader = ii.reader();
            // SAFETY: caller guarantees sctx and spec validity (1-3).
            let checker = unsafe { FieldExpirationChecker::new(sctx, filter_ctx, reader.flags()) };
            // SAFETY: caller guarantees sctx, spec, field_index, and
            // missingFieldDict validity (1-3).
            crate::BoxedRQEIterator::new(Box::new(unsafe {
                Missing::new(reader, sctx, field_index, checker)
            }))
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
    type Suspended = RawMissing<Suspended, E, C>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawMissing` is `#[repr(C)]`. The `Rf`-dependent field is
        // the inner `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>`,
        // whose layout is identical across modes (see
        // [`InvIndIterator::suspend`]). `field_index: FieldIndex` and
        // `field_name: CString` carry no `Rf` and survive the cast.
        // Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawMissing<Suspended, E, C>) }
    }
}

impl<E, C> RQESuspendedIterator for RawMissing<Suspended, E, C>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
    C: ExpirationChecker + 'static,
{
    type Resumed<'a> = Missing<'a, E, C>;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort on the suspended form. Reads only
        // mode-independent state (`field_index`, the spec lookups, and
        // the reader's `points_to_ii`).
        let abort = self.should_abort(spec);

        // Step 2: refresh reader pointers in place while still
        // suspended. Skipped on the abort path.
        let outcome = if abort {
            RefreshOutcome::Ok
        } else {
            self.it.refresh_pointers()
        };

        // Step 3: whole-box cast Suspended → Active.
        let raw = Box::into_raw(self);
        // SAFETY: `RawMissing` is `#[repr(C)]` over the inner
        // `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>, C>`, plus
        // mode-independent `field_index: FieldIndex` and
        // `field_name: CString`. The `RawInvIndIterator` layout is
        // identical across modes (see `RawInvIndIterator::suspend`).
        // The caller's read lock (witnessed by `spec`) plus the
        // `refresh_pointers` step above together ensure every pointer
        // inside the iterator is valid for `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut Missing<'a, E, C>) };

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

    fn num_estimated(&self) -> usize {
        self.it.num_estimated_field()
    }
}
