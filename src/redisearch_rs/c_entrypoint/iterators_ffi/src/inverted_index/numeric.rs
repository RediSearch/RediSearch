/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use field::{FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{FilterGeoReader, FilterNumericReader, IndexReader, NumericFilter};
use numeric_range_tree::{NumericIndex, NumericIndexReader, NumericRange, NumericRangeTree};
use rqe_iterators::{FieldExpirationChecker, inverted_index::Numeric};
use rqe_iterators_interop::RQEIteratorWrapper;

/// Enum holding either a numeric or geo iterator variant.
/// This allows all iterator types to share the same iterator wrapper structure.
enum IteratorVariant<'index> {
    /// Numeric iterator without a filter (uses the reader directly).
    Numeric(Numeric<'index, NumericIndexReader<'index>, FieldExpirationChecker>),
    /// Numeric iterator with a user filter applied.
    NumericFiltered(
        Numeric<
            'index,
            FilterNumericReader<'index, NumericIndexReader<'index>>,
            FieldExpirationChecker,
        >,
    ),
    /// Geo iterator (always has a filter).
    Geo(
        Numeric<
            'index,
            FilterGeoReader<'index, NumericIndexReader<'index>>,
            FieldExpirationChecker,
        >,
    ),
}

/// Wrapper around the actual Numeric iterator.
/// Needed as we need to keep the `filter` pointer around so it can be returned in
/// [`NumericInvIndIterator_GetNumericFilter`].
pub(super) struct NumericIterator<'index> {
    /// The user numeric filter, or None if no filter was provided.
    filter: Option<NonNull<NumericFilter>>,
    /// The iterator variant (numeric or geo).
    iterator: IteratorVariant<'index>,
}

impl<'index> NumericIterator<'index> {
    /// Get the flags from the underlying reader.
    pub(super) fn flags(&self) -> ffi::IndexFlags {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.reader().flags(),
            IteratorVariant::NumericFiltered(iter) => iter.reader().flags(),
            IteratorVariant::Geo(iter) => iter.reader().flags(),
        }
    }

    /// Get the range minimum value for profiling.
    const fn range_min(&self) -> f64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.range_min(),
            IteratorVariant::NumericFiltered(iter) => iter.range_min(),
            IteratorVariant::Geo(iter) => iter.range_min(),
        }
    }

    /// Get the range maximum value for profiling.
    const fn range_max(&self) -> f64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.range_max(),
            IteratorVariant::NumericFiltered(iter) => iter.range_max(),
            IteratorVariant::Geo(iter) => iter.range_max(),
        }
    }
}

impl<'index> rqe_iterators::RQEIterator<'index> for NumericIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.current(),
            IteratorVariant::NumericFiltered(iter) => iter.current(),
            IteratorVariant::Geo(iter) => iter.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.read(),
            IteratorVariant::NumericFiltered(iter) => iter.read(),
            IteratorVariant::Geo(iter) => iter.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.skip_to(doc_id),
            IteratorVariant::NumericFiltered(iter) => iter.skip_to(doc_id),
            IteratorVariant::Geo(iter) => iter.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.revalidate(),
            IteratorVariant::NumericFiltered(iter) => iter.revalidate(),
            IteratorVariant::Geo(iter) => iter.revalidate(),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match &mut self.iterator {
            IteratorVariant::Numeric(iter) => iter.rewind(),
            IteratorVariant::NumericFiltered(iter) => iter.rewind(),
            IteratorVariant::Geo(iter) => iter.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.num_estimated(),
            IteratorVariant::NumericFiltered(iter) => iter.num_estimated(),
            IteratorVariant::Geo(iter) => iter.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> u64 {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.last_doc_id(),
            IteratorVariant::NumericFiltered(iter) => iter.last_doc_id(),
            IteratorVariant::Geo(iter) => iter.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match &self.iterator {
            IteratorVariant::Numeric(iter) => iter.at_eof(),
            IteratorVariant::NumericFiltered(iter) => iter.at_eof(),
            IteratorVariant::Geo(iter) => iter.at_eof(),
        }
    }
}

#[unsafe(no_mangle)]
/// Creates a new numeric inverted index iterator for querying numeric fields.
///
/// # Parameters
///
/// * `idx` - Pointer to the inverted index to query.
/// * `sctx` - Pointer to the Redis search context for expiration checking.
/// * `field_ctx` - Pointer to the field filter context (field index and expiration predicate).
/// * `flt` - Optional pointer to a numeric filter for value filtering (can be NULL).
/// * `rt` - Optional pointer to the numeric range tree for revalidation (can be NULL).
/// * `range_min` - Minimum value of the numeric range.
/// * `range_max` - Maximum value of the numeric range.
///
/// # Returns
///
/// A pointer to a `QueryIterator` that can be used from C code.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `idx` must be a valid pointer to a numeric `InvertedIndex` and cannot be NULL.
/// 2. `idx` must remain valid for the lifetime of the returned iterator.
/// 3. `sctx` must be a valid pointer to a `RedisSearchCtx` and cannot be NULL.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of the returned iterator.
/// 5. `field_ctx` must be a valid pointer to a `FieldFilterContext` and cannot be NULL.
/// 6. `field_ctx.field` must be a field index (tag == FieldMaskOrIndex_Index), not a field mask.
///    Numeric queries require a specific field index.
/// 7. If `flt` is not NULL, it must be a valid pointer to a `NumericFilter` and must
///    remain valid for the lifetime of the returned iterator.
/// 8. If `rt` is not NULL, it must be a valid pointer to a `NumericRangeTree` and must
///    remain valid for the lifetime of the returned iterator.
/// 9. `range_min` is smaller or equal to `range_max`.
pub unsafe extern "C" fn NewInvIndIterator_NumericQuery(
    idx: *const NumericIndex,
    sctx: *const ffi::RedisSearchCtx,
    field_ctx: *const FieldFilterContext,
    flt: *const NumericFilter,
    rt: *const NumericRangeTree,
    range_min: f64,
    range_max: f64,
) -> *mut ffi::QueryIterator {
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!field_ctx.is_null(), "field_ctx must not be null");

    // SAFETY: 1. guarantees idx is valid and non-null
    let ii_ref = unsafe { &*idx };

    // Get field index and predicate from field context
    // SAFETY: 5. guarantees field_ctx is valid and non-null
    let field_ctx = unsafe { &*field_ctx };

    let field_index = match field_ctx.field {
        field::FieldMaskOrIndex::Index(index) => index,
        field::FieldMaskOrIndex::Mask(_) => {
            // SAFETY: Guaranteed by safety requirement 6.
            panic!("Numeric queries require a field index, not a field mask");
        }
    };

    // SAFETY: 3.
    let sctx = unsafe { NonNull::new_unchecked(sctx as *mut _) };
    let filter = NonNull::new(flt as *mut NumericFilter);
    let range_tree = NonNull::new(rt as *mut _).map(|t|
            // SAFETY: 8.
            unsafe { t.as_ref() });

    let reader = ii_ref.reader();

    // Create the expiration checker
    // Note: The caller guarantees sctx is valid and non-null (see safety contract in new())
    let expiration_checker = FieldExpirationChecker::new(
        sctx,
        FieldFilterContext {
            field: FieldMaskOrIndex::Index(field_index),
            predicate: field_ctx.predicate,
        },
        reader.flags(),
    );

    let iterator = match filter {
        Some(filter) => {
            // SAFETY: 7.
            let filter_ref = unsafe { filter.as_ref() };
            if !filter_ref.geo_filter.is_null() {
                // Geo filter
                let filter_reader = FilterGeoReader::new(filter_ref, reader);
                // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
                let iter = unsafe {
                    Numeric::new(
                        filter_reader,
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                NumericIterator {
                    filter: Some(filter),
                    iterator: IteratorVariant::Geo(iter),
                }
            } else {
                // Numeric filter (no geo)
                let filter_reader = FilterNumericReader::new(filter_ref, reader);
                // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
                let iter = unsafe {
                    Numeric::new(
                        filter_reader,
                        expiration_checker,
                        range_tree,
                        Some(range_min),
                        Some(range_max),
                    )
                };
                NumericIterator {
                    filter: Some(filter),
                    iterator: IteratorVariant::NumericFiltered(iter),
                }
            }
        }
        None => {
            // No filter - use the reader directly
            // SAFETY: 8. guarantees `range_tree` validity for the iterator's lifetime.
            let iter = unsafe {
                Numeric::new(
                    reader,
                    expiration_checker,
                    range_tree,
                    Some(range_min),
                    Some(range_max),
                )
            };
            NumericIterator {
                filter: None,
                iterator: IteratorVariant::Numeric(iter),
            }
        }
    };

    RQEIteratorWrapper::boxed_new(ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR, iterator)
}

/// Gets the numeric filter from a numeric inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `NumericInvIndIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// A pointer to the numeric filter, or NULL if no filter was provided when creating the iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetNumericFilter(
    it: *const ffi::NumericInvIndIterator,
) -> *const ffi::NumericFilter {
    debug_assert!(!it.is_null());

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
/// 1. `it` must be a valid pointer to a `QueryIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// The minimum range value from the filter, or negative infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMin(
    it: *const ffi::NumericInvIndIterator,
) -> f64 {
    debug_assert!(!it.is_null());

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    wrapper.inner.range_min()
}

/// Gets the maximum range value for profiling a numeric iterator.
///
/// # Safety
///
/// 1. `it` must be a valid pointer to a `QueryIterator` created by `NewInvIndIterator_NumericQuery`.
///
/// # Returns
///
/// The maximum range value from the filter, or positive infinity if no filter was provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericInvIndIterator_GetProfileRangeMax(
    it: *const ffi::NumericInvIndIterator,
) -> f64 {
    debug_assert!(!it.is_null());

    // SAFETY: 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast()) };
    wrapper.inner.range_max()
}

/// Result of creating numeric range iterators for all matching ranges.
///
/// The `iterators` array is allocated with `RedisModule_Calloc`, so it can be
/// passed directly to `NewUnionIterator` (which takes ownership and frees with
/// `rm_free`). For the 0-range or 1-range cases, the caller must `rm_free`
/// the array themselves.
#[repr(C)]
pub struct NumericRangeIteratorsResult {
    /// Array of iterators. NULL when `len == 0`.
    pub iterators: *mut *mut ffi::QueryIterator,
    /// Number of iterators in the array.
    pub len: usize,
}

/// Creates numeric range iterators for all ranges in the tree matching the filter.
///
/// This combines the tree lookup and per-range iterator creation into a single
/// call, eliminating the need for C-side loops over intermediate `Vector` results.
///
/// # Returns
///
/// A [`NumericRangeIteratorsResult`] containing the array of iterators and its length.
/// The array is allocated with `RedisModule_Calloc`. When `len == 0`, `iterators` is NULL.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
///
/// 1. `t` must be a valid non-NULL pointer to a [`NumericRangeTree`].
/// 2. `t` must remain valid for the lifetime of all returned iterators.
/// 3. `sctx` must be a valid non-NULL pointer to a `RedisSearchCtx`.
/// 4. `sctx` and `sctx.spec` must remain valid for the lifetime of all returned iterators.
/// 5. `f` must be a valid non-NULL pointer to a [`NumericFilter`].
/// 6. `f` must remain valid for the lifetime of all returned iterators.
/// 7. `field_ctx` must be a valid non-NULL pointer to a `FieldFilterContext`.
/// 8. `field_ctx.field` must be a field index (tag == FieldMaskOrIndex_Index), not a field mask.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CreateNumericRangeIterators(
    t: *const NumericRangeTree,
    sctx: *const ffi::RedisSearchCtx,
    f: *const NumericFilter,
    field_ctx: *const FieldFilterContext,
) -> NumericRangeIteratorsResult {
    debug_assert!(!t.is_null(), "t must not be null");
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!f.is_null(), "f must not be null");
    debug_assert!(!field_ctx.is_null(), "field_ctx must not be null");

    // SAFETY: 1. guarantees t is valid and non-null
    let tree = unsafe { &*t };
    // SAFETY: 5. guarantees f is valid and non-null
    let filter = unsafe { &*f };

    let ranges: Vec<&NumericRange> = tree.find(filter);

    if ranges.is_empty() {
        return NumericRangeIteratorsResult {
            iterators: std::ptr::null_mut(),
            len: 0,
        };
    }

    // Allocate the output array with RedisModule_Calloc so C can free it with rm_free.
    // SAFETY: RedisModule_Calloc is always initialized when the module is loaded.
    let calloc = unsafe { ffi::RedisModule_Calloc.unwrap() };
    // SAFETY: the length of the array is guaranteed to be non-zero, and
    // the size of each element is guaranteed to be non-zero.
    let iterators = unsafe {
        calloc(ranges.len(), std::mem::size_of::<*mut ffi::QueryIterator>())
            as *mut *mut ffi::QueryIterator
    };

    // Pass the tree as the revalidation tree only when field_spec is non-null,
    // matching the existing C behavior in NewNumericRangeIterator.
    let rt: *const NumericRangeTree = if filter.field_spec.is_null() {
        std::ptr::null()
    } else {
        t
    };

    for (i, range) in ranges.iter().enumerate() {
        let min_val = range.min_val();
        let max_val = range.max_val();

        // Determine if we can skip the filter: if the filter is numeric (not geo)
        // and both the range min and max are within the filter bounds, the reader
        // doesn't need to check the filter for each record.
        let reader_filter: *const NumericFilter = if filter.is_numeric_filter()
            && filter.value_in_range(min_val)
            && filter.value_in_range(max_val)
        {
            std::ptr::null()
        } else {
            f
        };

        let entries: *const NumericIndex = range.entries();

        // SAFETY: All pointer requirements are upheld by the caller's contract
        // (safety requirements 1-8) and our own invariants above.
        let it = unsafe {
            NewInvIndIterator_NumericQuery(
                entries,
                sctx,
                field_ctx,
                reader_filter,
                rt,
                min_val,
                max_val,
            )
        };

        // SAFETY: iterators was allocated with enough space for ranges.len() elements,
        // and i < ranges.len().
        let ith_slot = unsafe { iterators.add(i) };
        // SAFETY: ith_slot is a valid pointer to a slot in iterators, and we've exclusive
        // access to it.
        unsafe { ith_slot.write(it) };
    }

    NumericRangeIteratorsResult {
        iterators,
        len: ranges.len(),
    }
}
