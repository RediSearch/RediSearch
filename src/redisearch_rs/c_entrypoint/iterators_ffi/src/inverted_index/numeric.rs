/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{FieldSpec, RSGlobalConfig};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use query_node_type::QueryNodeType;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    IteratorsConfig, NumericIteratorVariant, RQEIteratorBoxed, RQESuspendedIterator,
    c2rust::CRQEIterator,
    interop::{InnerState, RQEIteratorWrapper},
    open_numeric_or_geo_index, profile_print,
};

/// Suspended counterpart of [`NumericIterator`] — produced by
/// [`RQEIteratorBoxed::suspend`] and consumed by [`RQESuspendedIterator::resume`].
///
/// `#[repr(C)]` matches the layout of [`NumericIterator`] so that
/// [`RQEIteratorBoxed::suspend`] / [`RQESuspendedIterator::resume`]
/// can perform whole-`Box` pointer casts that preserve the heap
/// allocation across the cycle — see
/// [`super::tag::TagIteratorSuspended`] for the same argument.
#[repr(C)]
pub(super) struct NumericIteratorSuspended {
    filter: Option<NonNull<NumericFilter>>,
    iterator: <NumericIteratorVariant<'static> as RQEIteratorBoxed<'static>>::Suspended,
}

impl<'index> RQEIteratorBoxed<'index> for NumericIterator<'index> {
    type Suspended = NumericIteratorSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `NumericIterator<'index>` and `NumericIteratorSuspended`
        // are both `#[repr(C)]` with the same field order and
        // layout-compatible fields (`filter` is mode-independent; the
        // `iterator` field's Active/Suspended counterparts are
        // layout-compatible via `NumericIteratorVariant`'s own
        // `#[repr(C, u8)]` design). `Box::from_raw` reuses the same
        // heap allocation.
        unsafe { Box::from_raw(raw as *mut NumericIteratorSuspended) }
    }
}

impl RQESuspendedIterator for NumericIteratorSuspended {
    type Resumed<'a> = NumericIterator<'a>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ffi::ValidateStatus) {
        // Drive the inner variant's resume in place by transmuting the
        // outer Box bytewise. We can't move the inner `iterator` field
        // out and back via a destructure-and-rebuild because that would
        // shift its heap address; the FFI wrapper's `header.current`
        // pointer (set by the previous `read` to point inside the
        // inner result) would be invalidated.
        //
        // Step 1: ptr-cast the outer `Box<NumericIteratorSuspended>` to
        // a raw pointer; both halves of the conversion are
        // `#[repr(C)]`, so the result is layout-compatible with
        // `Box<NumericIterator<'a>>`.
        let raw = Box::into_raw(self);
        // SAFETY: see `suspend` for the layout argument.
        let active_raw = raw as *mut NumericIterator<'a>;

        // Step 2: drive the inner variant's resume via the
        // `NumericIteratorVariantSuspended` impl, which does its own
        // whole-`Box` cast on the inner variant. To call it on the
        // inner without consuming the outer, we read it out, pass it
        // to resume (which allocates+frees a temporary Box internally
        // around its whole-box-cast), and write back. The variant's
        // own heap layout is preserved by its whole-box-cast impl, so
        // the byte position of the inner result struct relative to
        // the outer `active_raw` is unchanged across this operation.
        //
        // SAFETY: `active_raw` points to a valid `NumericIterator<'a>`
        // (just produced from `Box::into_raw`). The `iterator` field is
        // at a known `#[repr(C)]` offset.
        let inner_suspended_ptr = unsafe { ptr::addr_of_mut!((*active_raw).iterator) }
            as *mut <NumericIteratorVariant<'static> as RQEIteratorBoxed<'static>>::Suspended;
        let inner_suspended_box: Box<
            <NumericIteratorVariant<'static> as RQEIteratorBoxed<'static>>::Suspended,
        > = {
            // Build a Box that owns the read-out bytes. We allocate a
            // fresh Box around the read value so the type machinery
            // can drive the trait method.
            //
            // SAFETY: the bytes at `inner_suspended_ptr` are currently a
            // valid `NumericIteratorVariantSuspended` (the outer box was
            // suspended; only the type label flipped). We take ownership
            // by reading the bytes out.
            let read_bytes = unsafe { ptr::read(inner_suspended_ptr) };
            Box::new(read_bytes)
        };
        let (active_inner, status) =
            <_ as RQESuspendedIterator>::resume(inner_suspended_box, guard);
        // SAFETY: `active_raw` points to a valid `NumericIterator<'a>` box,
        // so `addr_of_mut!((*active_raw).iterator)` is a valid raw pointer
        // to its `iterator` field.
        let iterator_slot = unsafe { ptr::addr_of_mut!((*active_raw).iterator) };
        // SAFETY: write the resumed inner bytes back into the outer
        // box's `iterator` slot. The slot has the right size and
        // alignment for `NumericIteratorVariant<'a>` (it was just
        // labelled as such by the outer cast).
        unsafe {
            ptr::write(iterator_slot, *active_inner);
        }

        // SAFETY: outer box is now fully Active.
        let active = unsafe { Box::from_raw(active_raw) };
        (active, status)
    }

    fn last_doc_id(&self) -> u64 {
        self.iterator.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.iterator.num_estimated()
    }
}

/// Wrapper around [`NumericIteratorVariant`].
///
/// Needed to keep the `filter` pointer around so it can be returned in
/// [`NumericInvIndIterator_GetNumericFilter`].
///
/// `#[repr(C)]` so that the layout matches [`NumericIteratorSuspended`].
#[repr(C)]
pub(super) struct NumericIterator<'index> {
    /// Optional pointer to the [`NumericFilter`] this iterator was created with.
    /// Threaded through suspend/resume so that FFI accessors can read it in
    /// either typestate without re-deriving it from the variant.
    filter: Option<NonNull<NumericFilter>>,
    /// The iterator variant (unfiltered, filtered numeric, or geo).
    iterator: NumericIteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
    /// Wrap a variant with no associated filter — used by
    /// [`crate::inverted_index::geo`].
    pub(super) const fn new(iterator: NumericIteratorVariant<'index>) -> Self {
        Self {
            filter: None,
            iterator,
        }
    }

    /// Wrap a variant with a numeric/geo filter pointer for use by
    /// [`NewNumericFilterIterator`].
    pub(super) const fn with_filter(
        filter: NonNull<NumericFilter>,
        iterator: NumericIteratorVariant<'index>,
    ) -> Self {
        Self {
            filter: Some(filter),
            iterator,
        }
    }

    /// Get the flags from the underlying reader.
    pub(super) const fn flags(&self) -> ffi::IndexFlags {
        self.iterator.flags()
    }

    /// Get the filter pointer.
    pub(super) const fn filter(&self) -> Option<NonNull<NumericFilter>> {
        self.filter
    }

    /// Get the range minimum value for profiling.
    pub(super) const fn range_min(&self) -> f64 {
        self.iterator.range_min()
    }

    /// Get the range maximum value for profiling.
    pub(super) const fn range_max(&self) -> f64 {
        self.iterator.range_max()
    }
}

impl NumericIteratorSuspended {
    /// Mirror of [`NumericIterator::flags`] on the suspended side.
    pub(super) const fn flags(&self) -> ffi::IndexFlags {
        self.iterator.flags()
    }

    /// Mirror of [`NumericIterator::filter`] on the suspended side.
    pub(super) const fn filter(&self) -> Option<NonNull<NumericFilter>> {
        self.filter
    }

    /// Mirror of [`NumericIterator::range_min`] on the suspended side.
    pub(super) const fn range_min(&self) -> f64 {
        self.iterator.range_min()
    }

    /// Mirror of [`NumericIterator::range_max`] on the suspended side.
    pub(super) const fn range_max(&self) -> f64 {
        self.iterator.range_max()
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for NumericIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
        self.iterator.current()
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        self.iterator.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        self.iterator.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.iterator.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.iterator.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> u64 {
        self.iterator.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.iterator.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxNumeric
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// Gets the numeric filter from a numeric inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` wrapping a [`NumericIterator`].
///
/// # Returns
///
/// A pointer to the numeric filter, or NULL if no filter was provided when creating the iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetNumericFilter(
    it: *const ffi::QueryIterator,
) -> *const ffi::NumericFilter {
    debug_assert!(!it.is_null());
    // SAFETY: we just checked for NULL and 1 ensure `it` is an iterator.
    debug_assert!(unsafe { &*it }.type_ == IteratorType::InvIdxNumeric);

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };

    // The filter pointer is the same Copy value in either typestate — the
    // underlying numeric iterator carries it across suspend/resume unchanged.
    // SAFETY: The filter is pinned and has a stable address for the lifetime
    // of the iterator. Both types have the same #[repr(C)] layout so we can
    // cast the pointer.
    let filter = match wrapper.state() {
        InnerState::Active(it) => it.filter(),
        InnerState::Suspended(it) => it.filter(),
    };
    filter
        .map(|f| f.as_ptr() as *const ffi::NumericFilter)
        .unwrap_or(std::ptr::null())
}

/// Gets the minimum range value for profiling a numeric iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` wrapping a [`NumericIterator`].
///
/// # Returns
///
/// The minimum range value from the filter, or negative infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMin(
    it: *const ffi::QueryIterator,
) -> f64 {
    debug_assert!(!it.is_null());
    // SAFETY: we just checked for NULL and 1 ensure `it` is an iterator.
    debug_assert!(unsafe { &*it }.type_ == IteratorType::InvIdxNumeric);

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    match wrapper.state() {
        InnerState::Active(it) => it.range_min(),
        InnerState::Suspended(it) => it.range_min(),
    }
}

/// Gets the maximum range value for profiling a numeric iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` wrapping a [`NumericIterator`].
///
/// # Returns
///
/// The maximum range value from the filter, or positive infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMax(
    it: *const ffi::QueryIterator,
) -> f64 {
    debug_assert!(!it.is_null());
    // SAFETY: we just checked for NULL and 1 ensure `it` is an iterator.
    debug_assert!(unsafe { &*it }.type_ == IteratorType::InvIdxNumeric);

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    match wrapper.state() {
        InnerState::Active(it) => it.range_max(),
        InnerState::Suspended(it) => it.range_max(),
    }
}

///
/// # Safety
///
/// 1. `spec` must be a valid non-null pointer to an [`ffi::IndexSpec`].
/// 2. `fs` must be a valid non-null pointer to a [`FieldSpec`] for a numeric or geo field.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn openNumericOrGeoIndex(
    spec: *mut ffi::IndexSpec,
    fs: *mut FieldSpec,
    create_if_missing: bool,
) -> *mut ffi::NumericRangeTree {
    debug_assert!(!spec.is_null());
    debug_assert!(!fs.is_null());
    // SAFETY: 1. guarantees spec is valid and non-null.
    let spec = unsafe { &mut *spec };
    // SAFETY: 2. guarantees fs is valid and non-null.
    let fs = unsafe { &mut *fs };

    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 1. and 2. are forwarded from this function's safety contract.
    match unsafe { open_numeric_or_geo_index(spec, fs, create_if_missing, compress) } {
        Some(tree) => std::ptr::from_mut(tree).cast::<ffi::NumericRangeTree>(),
        None => ptr::null_mut(),
    }
}

/// Opens the numeric/geo index and creates an iterator over all matching sub-ranges.
///
/// # Returns
///
/// - `NULL` if the index doesn't exist for this field (i.e., no documents have been indexed
///   for it yet).
/// - `NULL` if no sub-ranges in the tree match the filter.
/// - A single iterator if exactly one sub-range matches.
/// - A union iterator over all matching sub-ranges otherwise.
///
/// # Safety
///
/// 1. `ctx` must be a valid non-NULL pointer to a [`ffi::RedisSearchCtx`], remaining valid
///    for the lifetime of the returned iterator.
/// 2. `ctx.spec` must be a valid non-NULL pointer to an [`ffi::IndexSpec`].
/// 3. `flt` must be a valid non-NULL pointer to a [`NumericFilter`] whose `field_spec` field
///    is a valid non-NULL pointer to a [`FieldSpec`], remaining valid for the lifetime of the
///    returned iterator.
/// 4. `config` must be a valid non-NULL pointer to an [`IteratorsConfig`].
/// 5. `filter_ctx` must be a valid non-NULL pointer to a [`FieldFilterContext`] with a field
///    index (not a field mask).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericFilterIterator(
    ctx: *const ffi::RedisSearchCtx,
    flt: *const NumericFilter,
    _for_type: ffi::FieldType,
    config: *const IteratorsConfig,
    filter_ctx: *const FieldFilterContext,
) -> *mut ffi::QueryIterator {
    debug_assert!(!ctx.is_null());
    debug_assert!(!flt.is_null());

    // SAFETY: 3. guarantees flt is valid and non-null.
    let flt_ref = unsafe { &*flt };
    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).min_union_iter_heap } as usize;

    // SAFETY: 1. guarantees ctx is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: 5. guarantees filter_ctx is valid and non-null.
    let field_ctx = unsafe { &*filter_ctx };

    let node_type = if flt_ref.is_numeric_filter() {
        QueryNodeType::Numeric
    } else {
        QueryNodeType::Geo
    };

    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec_ptr = unsafe { sctx.as_ref() }.spec;
    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec = unsafe { &mut *spec_ptr };
    // SAFETY: 3. guarantees flt.field_spec is valid and non-null.
    let fs = unsafe { &mut *(flt_ref.field_spec as *mut ffi::FieldSpec) };
    // SAFETY: 1.–3. are forwarded from this function's safety contract.
    let Some(tree) = (unsafe { open_numeric_or_geo_index(spec, fs, false, compress) }) else {
        return ptr::null_mut();
    };

    // SAFETY: 1. and 5.
    let variants = unsafe { NumericIteratorVariant::from_tree(tree, sctx, flt_ref, field_ctx) };
    if variants.is_empty() {
        return ptr::null_mut();
    }

    // SAFETY: 3. guarantees flt is non-null.
    let filter_ptr = unsafe { NonNull::new_unchecked(flt as *mut NumericFilter) };
    let children: Vec<CRQEIterator> = variants
        .into_iter()
        .map(|iterator| {
            let ptr = RQEIteratorWrapper::boxed_new(NumericIterator::with_filter(
                filter_ptr, iterator,
            ));
            // SAFETY: `boxed_new` uses `Box::into_raw`, which is guaranteed non-null.
            let ptr = unsafe { NonNull::new_unchecked(ptr) };
            // SAFETY: `ptr` is a valid, uniquely-owned `QueryIterator`.
            unsafe { CRQEIterator::new(ptr) }
        })
        .collect();

    crate::union::build_union_from_children(
        children,
        true,
        min_union_iter_heap,
        node_type,
        ptr::null(),
        1.0,
    )
}

impl profile_print::ProfilePrint for NumericIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut profile_print::ProfilePrintCtx<'_>,
    ) {
        self.iterator.print_profile(map, ctx);
    }
}
