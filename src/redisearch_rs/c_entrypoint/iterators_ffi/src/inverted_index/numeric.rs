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
    NumericIteratorVariant, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    open_numeric_or_geo_index,
};

/// Wrapper around [`NumericIteratorVariant`].
/// Needed to keep the `filter` pointer around so it can be returned in
/// [`NumericInvIndIterator_GetNumericFilter`].
pub(super) struct NumericIterator<'index> {
    /// The user numeric filter, or None if no filter was provided.
    ///
    /// Kept here (rather than in `rqe_iterators`) solely so that
    /// `NumericInvIndIterator_GetNumericFilter` can hand the pointer back to C callers.
    /// Once those callers are ported to Rust, this field and `NumericIterator` itself can be
    /// removed — callers will use [`NumericIteratorVariant`] directly.
    filter: Option<NonNull<NumericFilter>>,
    /// The iterator variant (unfiltered, filtered numeric, or geo).
    iterator: NumericIteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
    /// Wrap a variant with a filter, for use by [`crate::inverted_index::geo`].
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
    pub(super) fn flags(&self) -> ffi::IndexFlags {
        self.iterator.flags()
    }

    /// Get the range minimum value for profiling.
    const fn range_min(&self) -> f64 {
        self.iterator.range_min()
    }

    /// Get the range maximum value for profiling.
    const fn range_max(&self) -> f64 {
        self.iterator.range_max()
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for NumericIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        self.iterator.current()
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
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
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        // SAFETY: Delegating to inner iterator with the same `spec` passed by our caller.
        unsafe { self.iterator.revalidate(spec) }
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

    // Return a pointer to the pinned filter, or NULL if no filter was provided
    // SAFETY: The filter is pinned and has a stable address for the lifetime of the iterator
    // Both types have the same #[repr(C)] layout so we can cast the pointer
    wrapper
        .inner
        .filter
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
    wrapper.inner.range_min()
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
    wrapper.inner.range_max()
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
/// 4. `config` must be a valid non-NULL pointer to an [`ffi::IteratorsConfig`].
/// 5. `filter_ctx` must be a valid non-NULL pointer to a [`FieldFilterContext`] with a field
///    index (not a field mask).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericFilterIterator(
    ctx: *const ffi::RedisSearchCtx,
    flt: *const NumericFilter,
    _for_type: ffi::FieldType,
    config: *const ffi::IteratorsConfig,
    filter_ctx: *const FieldFilterContext,
) -> *mut ffi::QueryIterator {
    debug_assert!(!ctx.is_null());
    debug_assert!(!flt.is_null());

    // SAFETY: 3. guarantees flt is valid and non-null.
    let flt_ref = unsafe { &*flt };
    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    // SAFETY: 1. guarantees ctx is valid and non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: 5. guarantees filter_ctx is valid and non-null.
    let field_ctx = unsafe { &*filter_ctx };

    let filter_nn = NonNull::from(flt_ref);
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

    let children: Vec<CRQEIterator> = variants
        .into_iter()
        .map(|iterator| {
            let ptr = RQEIteratorWrapper::boxed_new(NumericIterator {
                filter: Some(filter_nn),
                iterator,
            });
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
