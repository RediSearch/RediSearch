/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use field::FieldFilterContext;
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use query_node_type::QueryNodeType;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{NumericIteratorVariant, c2rust::CRQEIterator, interop::RQEIteratorWrapper};

/// Wrapper around [`NumericIteratorVariant`].
/// Needed to keep the `filter` pointer around so it can be returned in
/// [`NumericInvIndIterator_GetNumericFilter`].
pub(super) struct NumericIterator<'index> {
    /// The user numeric filter, or None if no filter was provided.
    ///
    /// C-Code: kept here (rather than in `rqe_iterators`) solely so that
    /// `NumericInvIndIterator_GetNumericFilter` can hand the pointer back to C callers.
    /// Once those callers are ported to Rust, this field and `NumericIterator` itself can be
    /// removed — callers will use [`NumericIteratorVariant`] directly.
    filter: Option<NonNull<NumericFilter>>,
    /// The iterator variant (unfiltered, filtered numeric, or geo).
    iterator: NumericIteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
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
    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        self.iterator.revalidate()
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

/// Creates a union iterator from a numeric filter over all matching sub-ranges in the tree.
///
/// # Returns
///
/// - `NULL` if no ranges match the filter, or if `f` is NULL.
/// - The single matching iterator directly if exactly one range matches.
/// - A union iterator over all matching ranges otherwise.
///
/// # Safety
///
/// 1. `t` must be a valid non-NULL pointer to a [`NumericRangeTree`], remaining valid for the
///    lifetime of all returned iterators.
/// 2. `sctx` and `sctx.spec` must be valid non-NULL pointers, remaining valid for the lifetime of
///    all returned iterators.
/// 3. `f` may be NULL. If non-NULL, it must be a valid pointer to a [`NumericFilter`], remaining
///    valid for the lifetime of all returned iterators.
/// 4. `config` must be a valid non-NULL pointer to an [`ffi::IteratorsConfig`].
/// 5. `field_ctx` must be a valid non-NULL pointer to a [`FieldFilterContext`] with a field index
///    (not a field mask).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CreateNumericIterator(
    sctx: *const ffi::RedisSearchCtx,
    t: *const NumericRangeTree,
    f: *const NumericFilter,
    config: *const ffi::IteratorsConfig,
    field_ctx: *const FieldFilterContext,
) -> *mut ffi::QueryIterator {
    // SAFETY: 1. guarantees t is valid and non-null.
    let tree = unsafe { &*t };
    // SAFETY: 2. guarantees sctx is valid and non-null.
    let sctx_nn = unsafe { NonNull::new_unchecked(sctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: 5. guarantees field_ctx is valid and non-null.
    let field_ctx_ref = unsafe { &*field_ctx };

    // SAFETY: 3. A NULL filter means there is nothing to search for.
    let Some(filter) = (unsafe { f.as_ref() }) else {
        return std::ptr::null_mut();
    };

    // SAFETY: caller upholds requirements 1–3, 5.
    let variants =
        unsafe { NumericIteratorVariant::from_tree(tree, sctx_nn, filter, field_ctx_ref) };

    if variants.is_empty() {
        return std::ptr::null_mut();
    }

    let filter_nn = NonNull::from(filter);
    // NULL `f` or a numeric filter → QN_NUMERIC; a geo filter → QN_GEO.
    let node_type = if filter.is_numeric_filter() {
        QueryNodeType::Numeric
    } else {
        QueryNodeType::Geo
    };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    let children: Vec<CRQEIterator> = variants
        .into_iter()
        .map(|iterator| {
            let ptr = RQEIteratorWrapper::boxed_new(NumericIterator {
                filter: Some(filter_nn),
                iterator,
            });
            // SAFETY: `boxed_new` never returns null.
            let ptr = unsafe { NonNull::new_unchecked(ptr) };
            // SAFETY: `ptr` is a valid, boxed `RQEIteratorWrapper`.
            unsafe { CRQEIterator::new(ptr) }
        })
        .collect();

    crate::union::build_union_from_children(
        children,
        true,
        min_union_iter_heap,
        node_type,
        std::ptr::null(),
        1.0,
    )
}
