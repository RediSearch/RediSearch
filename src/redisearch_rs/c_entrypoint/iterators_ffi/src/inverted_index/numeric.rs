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
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    IteratorsConfig, NumericIteratorVariant, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, ResumeOutcome,
    interop::{InnerState, RQEIteratorWrapper},
    open_numeric_or_geo_index,
};

/// Suspended counterpart of [`NumericIterator`] — produced by
/// [`RQEIteratorBoxed::suspend`] and consumed by [`RQESuspendedIterator::resume`].
pub(super) struct NumericIteratorSuspended<'query> {
    filter: Option<NonNull<NumericFilter>>,
    iterator: <NumericIteratorVariant<'query> as RQEIteratorBoxed<'query>>::Suspended,
}

impl<'index> RQEIteratorBoxed<'index> for NumericIterator<'index> {
    type Suspended = NumericIteratorSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let Self { filter, iterator } = *self;
        Box::new(NumericIteratorSuspended {
            filter,
            iterator: *<NumericIteratorVariant<'index> as RQEIteratorBoxed<'index>>::suspend(
                Box::new(iterator),
            ),
        })
    }
}

impl<'query> RQESuspendedIterator<'query> for NumericIteratorSuspended<'query> {
    type Resumed<'a>
        = NumericIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let Self { filter, iterator } = *self;
        // Forward the inner variant's outcome, re-wrapping the resumed variant
        // and threading the retained filter through.
        Ok(
            match <_ as RQESuspendedIterator>::resume(Box::new(iterator), guard)? {
                ResumeOutcome::Aborted => ResumeOutcome::Aborted,
                ResumeOutcome::Ok(resumed) => {
                    ResumeOutcome::Ok(Box::new(NumericIterator {
                        filter,
                        iterator: *resumed,
                    }))
                }
                ResumeOutcome::Moved(resumed) => {
                    ResumeOutcome::Moved(Box::new(NumericIterator {
                        filter,
                        iterator: *resumed,
                    }))
                }
            },
        )
    }

    fn last_doc_id(&self) -> u64 {
        self.iterator.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.iterator.num_estimated()
    }
}

/// Wrapper around [`NumericIteratorVariant`].
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
    #[expect(
        dead_code,
        reason = "constructor for the geo FFI accessor path, wired in a later commit"
    )]
    pub(super) const fn new(iterator: NumericIteratorVariant<'index>) -> Self {
        Self {
            filter: None,
            iterator,
        }
    }

    /// Wrap a variant with a numeric/geo filter pointer for use by
    /// [`NewNumericFilterIterator`].
    #[expect(
        dead_code,
        reason = "constructor for the numeric-filter FFI path, wired in a later commit"
    )]
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

impl<'query> NumericIteratorSuspended<'query> {
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
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        self.iterator.revalidate(spec)
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
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).min_union_iter_heap } as usize;
    // SAFETY: 1. guarantees ctx is valid and non-null.
    let sctx = unsafe { &*ctx };
    // SAFETY: 5. guarantees filter_ctx is valid and non-null.
    let field_ctx = unsafe { &*filter_ctx };
    // SAFETY: `RSGlobalConfig` is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };

    // SAFETY: preconditions 1–3/5 map directly to those of
    // `build_numeric_filter_iterator`.
    unsafe {
        rqe_iterators::build_numeric_filter_iterator(
            sctx,
            flt_ref,
            min_union_iter_heap,
            field_ctx,
            compress,
        )
    }
    .map_or(ptr::null_mut(), NonNull::as_ptr)
}
