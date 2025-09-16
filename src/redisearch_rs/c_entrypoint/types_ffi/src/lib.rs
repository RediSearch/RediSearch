/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use std::{ffi::c_char, ptr};

use inverted_index::{
    NumericFilter, RSAggregateResult, RSAggregateResultIter, RSIndexResult, RSOffsetVector,
    RSQueryTerm, RSTermRecord, t_fieldMask,
};

pub use inverted_index::{
    ReadFilter,
    debug::{BlockSummary, Summary},
};

/// Check if this is a numeric filter and not a geo filter
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `filter` must point to a valid `NumericFilter` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericFilter_IsNumeric(filter: *const NumericFilter) -> bool {
    debug_assert!(!filter.is_null(), "filter must not be null");

    // SAFETY: Caller is to ensure that the pointer `filter` is a valid, non-null pointer to
    // a `NumericFilter`.
    let filter = unsafe { &*filter };

    filter.is_numeric_filter()
}

/// Check if the given value matches the numeric filter.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `filter` must point to a valid `NumericFilter` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericFilter_Match(filter: *const NumericFilter, value: f64) -> bool {
    debug_assert!(!filter.is_null(), "filter must not be null");

    // SAFETY: Caller is to ensure that the pointer `filter` is a valid, non-null pointer to
    // a `NumericFilter`.
    let filter = unsafe { &*filter };

    filter.value_in_range(value)
}

/// Allocate a new intersect result with a given capacity and weight. This result should be freed
/// using [`IndexResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewIntersectResult<'result>(
    cap: usize,
    weight: f64,
) -> *mut RSIndexResult<'result> {
    let result = RSIndexResult::intersect(cap).weight(weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new union result with a given capacity and weight. This result should be freed using
/// [`IndexResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewUnionResult<'result>(cap: usize, weight: f64) -> *mut RSIndexResult<'result> {
    let result = RSIndexResult::union(cap).weight(weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new virtual result with a given weight and field mask. This result should be freed
/// using [`IndexResult_Free`].
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn NewVirtualResult<'result>(
    weight: f64,
    field_mask: t_fieldMask,
) -> *mut RSIndexResult<'result> {
    let result = RSIndexResult::virt().field_mask(field_mask).weight(weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new numeric result. This result should be freed using [`IndexResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewNumericResult<'result>() -> *mut RSIndexResult<'result> {
    let result = RSIndexResult::numeric(0.0);
    Box::into_raw(Box::new(result))
}

/// Allocate a new metric result. This result should be freed using [`IndexResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewMetricResult<'result>() -> *mut RSIndexResult<'result> {
    let result = RSIndexResult::metric();
    Box::into_raw(Box::new(result))
}

/// Allocate a new hybrid result. This result should be freed using [`IndexResult_Free`].
///
/// This constructor is only used by the hydrid reader which will pushed owned copies to it.
/// Therefore, this also returns an owned `RSIndexResult`.
#[unsafe(no_mangle)]
pub extern "C" fn NewHybridResult() -> *mut RSIndexResult<'static> {
    let result = RSIndexResult::hybrid_metric();
    Box::into_raw(Box::new(result.to_owned()))
}

/// Allocate a new token record with a given term and weight. This result should be freed using
/// [`IndexResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTokenRecord<'result>(
    term: *mut RSQueryTerm,
    weight: f64,
) -> *mut RSIndexResult<'result> {
    let result =
        RSIndexResult::term_with_term_ptr(term, RSOffsetVector::empty(), 0, 0, 0).weight(weight);
    Box::into_raw(Box::new(result))
}

/// Free an index result's internal allocations and also free the result itself.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
/// - `result` must have been created using one of these:
///   - [`NewIntersectResult`]
///   - [`NewUnionResult`]
///   - [`NewVirtualResult`]
///   - [`NewNumericResult`]
///   - [`NewMetricResult`]
///   - [`NewHybridResult`]
///   - [`NewTokenRecord`]
///   - [`IndexResult_DeepCopy`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_Free(result: *mut RSIndexResult) {
    debug_assert!(!result.is_null(), "result cannot be NULL");

    // SAFETY: caller is to ensure `result` points to a valid RSIndexResult created by one of the
    // constructors
    let _ = unsafe { Box::from_raw(result) };
}

/// Create a deep copy of the results that is totally thread safe. This is very slow so use it with
/// caution.
///
/// The created copy should be freed using [`IndexResult_Free`].
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_DeepCopy(source: *const RSIndexResult) -> *mut RSIndexResult {
    // SAFETY: caller is to ensure `source` points to a valid RSIndexResult
    let source = unsafe { &*source };

    let copy = source.to_owned();
    let copy = Box::new(copy);

    Box::into_raw(copy)
}

/// Check if the result is an aggregate result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_IsAggregate(result: *const RSIndexResult) -> bool {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &*result };

    result.is_aggregate()
}

/// Get the numeric value of the result if it is a numeric result. If the result is not numeric,
/// this function will return `0.0`.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_NumValue(result: *const RSIndexResult) -> f64 {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &*result };

    result.as_numeric().unwrap_or_default()
}

/// Set the numeric value of the result if it is a numeric result. If the result is not numeric,
/// this function will do nothing.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_SetNumValue(result: *mut RSIndexResult, value: f64) {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &mut *result };

    if let Some(num) = result.as_numeric_mut() {
        *num = value;
    }
}

/// Get the query term from a result if it is a term result. If the result is not a term, then
/// this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_QueryTermRef<'index>(
    result: *const RSIndexResult<'index>,
) -> *mut RSQueryTerm {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &*result };

    result
        .as_term()
        .map_or(ptr::null_mut(), |term| term.query_term())
}

/// Get the term offsets from a result if it is a term result. If the result is not a term, then
/// this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_TermOffsetsRef<'result, 'index>(
    result: *const RSIndexResult<'index>,
) -> Option<&'result RSOffsetVector<'index>> {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result: &'result _ = unsafe { &*result };

    result.as_term().map(|term| match term {
        RSTermRecord::Borrowed { offsets, .. } => offsets,
        RSTermRecord::Owned { offsets, .. } => offsets,
    })
}

/// Get a mutable term offsets from a result if it is a term result. If the result is not a term,
/// then this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_TermOffsetsRefMut<'result>(
    result: *mut RSIndexResult<'static>,
) -> Option<&'result mut RSOffsetVector<'static>> {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result: &'result mut _ = unsafe { &mut *result };

    result.as_term_mut().map(move |term| match term {
        RSTermRecord::Borrowed { offsets, .. } => offsets,
        RSTermRecord::Owned { offsets, .. } => offsets,
    })
}

/// Get the aggregate result reference if the result is an aggregate result. If the result is
/// not an aggregate, this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_AggregateRef<'result, 'index>(
    result: *const RSIndexResult<'index>,
) -> Option<&'result RSAggregateResult<'index>> {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &*result };

    result.as_aggregate()
}

/// Reset the result if it is an aggregate result. This will clear all children and reset the kind mask.
/// This function does not deallocate the children pointers, but rather resets the internal state of the
/// aggregate result. The owner of the children pointers is responsible for managing their lifetime.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_AggregateReset(result: *mut RSIndexResult) {
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result = unsafe { &mut *result };

    if let Some(agg) = result.as_aggregate_mut() {
        agg.reset();
    }
}

/// Get the result at the specified index in the aggregate result. This will return a `NULL` pointer
/// if the index is out of bounds.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
/// - The memory address at `index` should still be valid and not have been deallocated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Get<'result, 'index>(
    agg: *const RSAggregateResult<'index>,
    index: usize,
) -> Option<&'result RSIndexResult<'index>> {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    agg.get(index)
}

/// Get the element count of the aggregate result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_NumChildren(agg: *const RSAggregateResult) -> usize {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    agg.len()
}

/// Get the capacity of the aggregate result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Capacity(agg: *const RSAggregateResult) -> usize {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    agg.capacity()
}

/// Get the kind mask of the aggregate result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_KindMask(agg: *const RSAggregateResult) -> u8 {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    agg.kind_mask().bits()
}

/// Create a new aggregate result with the specified capacity. This function will make the result
/// in Rust memory, but the ownership ends up being transferred to C's memory space. This ownership
/// should return to Rust to free up any heap memory using [`AggregateResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_New(cap: usize) -> RSAggregateResult<'static> {
    RSAggregateResult::with_capacity(cap)
}

/// Take ownership of a `RSAggregateResult` to free any heap memory it owns. This function will not
/// free the individual children pointers, but rather the heap allocations owned by the aggregate
/// result itself (such as the internal vector buffer). The caller is responsible for managing the
/// memory of the children pointers before this call if needed.
///
/// The `agg` parameter should have been created with [`AggregateResult_New`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_Free(agg: RSAggregateResult) {
    match agg {
        RSAggregateResult::Borrowed { .. } => {}
        RSAggregateResult::Owned { records, .. } => {
            for record in records.into_iter() {
                // C still manages this memory so don't free the heap pointers
                std::mem::forget(record);
            }
        }
    }
}

/// Add a child to a result if it is an aggregate result. Note, if `parent` only hold references
/// to results, then it will not take ownership of the `child` and will therefore not free it.
/// Instead, the caller is responsible for managing the memory of the `child` pointer *after* the
/// `parent` has been freed.
///
/// If the `parent` is not an aggregate kind, then this is a no-op.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `parent` must point to a valid `RSIndexResult` and cannot be NULL.
/// - `child` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_AddChild(
    parent: *mut RSIndexResult<'static>,
    child: *mut RSIndexResult<'static>,
) {
    debug_assert!(!parent.is_null(), "parent must not be null");
    debug_assert!(!child.is_null(), "child must not be null");

    // SAFETY: Caller is to ensure that `parent` is a valid, non-null pointer to an `RSIndexResult`
    let parent = unsafe { &mut *parent };

    if parent.is_copy() {
        // SAFETY: Caller is to ensure that `child` is a valid, non-null pointer to an `RSIndexResult`
        let child = unsafe { Box::from_raw(child) };
        parent.push_boxed(child);
    } else {
        // SAFETY: Caller is to ensure that `child` is a valid, non-null pointer to an `RSIndexResult`
        let child = unsafe { &*child };
        parent.push_borrowed(child);
    }
}

/// Create an iterator over the aggregate result. This iterator should be freed
/// using [`AggregateResultIter_Free`].
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Iter(
    agg: *const RSAggregateResult<'static>,
) -> *mut RSAggregateResultIter<'static> {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };
    let iter = agg.iter();
    let iter_boxed = Box::new(iter);

    Box::into_raw(iter_boxed)
}

/// Get the next item in the aggregate result iterator and put it into the provided `value`
/// pointer. This function will return `true` if there is a next item, or `false` if the iterator
/// is exhausted.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `iter` must point to a valid `RSAggregateResultIter` and cannot be NULL.
/// - `value` must point to a valid pointer where the next item will be stored.
/// - All the memory addresses of the `RSAggregateResult` should still be valid and not have
///   been deallocated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResultIter_Next(
    iter: *mut RSAggregateResultIter<'static>,
    value: *mut *mut RSIndexResult,
) -> bool {
    debug_assert!(!iter.is_null(), "iter must not be null");
    debug_assert!(!value.is_null(), "value must not be null");

    // SAFETY: Caller is to ensure that the pointer `iter` is a valid, non-null pointer to
    // an `RSAggregateResultIter`.
    let iter = unsafe { &mut *iter };

    if let Some(next) = iter.next() {
        // SAFETY: Caller is to ensure that the pointer `value` is a valid, non-null pointer
        unsafe {
            *value = next as *const _ as *mut _;
        }
        true
    } else {
        false
    }
}

/// Free the aggregate result iterator. This function will deallocate the memory used by the iterator.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `iter` must point to a valid `RSAggregateResultIter`.
/// - The iterator must have been created using [`AggregateResult_Iter`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResultIter_Free(iter: *mut RSAggregateResultIter<'static>) {
    // Don't free if the pointer is `NULL` - just like the C free function
    if iter.is_null() {
        return;
    }

    // SAFETY: Caller is to ensure `iter` was allocated using `AggregateResult_Iter`
    let _boxed_iter = unsafe { Box::from_raw(iter) };
}

/// Retrieve the offsets array from [`RSOffsetVector`].
///
/// Set the array length into the `len` pointer.
/// The returned array is borrowed from the [`RSOffsetVector`] and should not be modified.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `offsets` must point to a valid [`RSOffsetVector`] and cannot be NULL.
/// - `len` cannot be NULL and must point to an allocated memory big enough to hold an u32.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSOffsetVector_GetData(
    offsets: *const RSOffsetVector,
    len: *mut u32,
) -> *const c_char {
    debug_assert!(!offsets.is_null(), "offsets must not be null");
    debug_assert!(!len.is_null(), "len must not be null");

    // SAFETY: Caller is to ensure `offsets` is non-null and point to a valid RSOffsetVector.
    let offsets = unsafe { &*offsets };

    // SAFETY: Caller is to ensure `len` is non-null and point to a valid u32 memory.
    unsafe { len.write(offsets.len) };
    offsets.data
}

/// Set the offsets array on a [`RSOffsetVector`].
///
/// The [`RSOffsetVector`] will borrow the passed array so it's up to the caller to
/// ensure it stays alive during the [`RSOffsetVector`] lifetime.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `offsets` must point to a valid [`RSOffsetVector`] and cannot be NULL.
/// - `data` must point to an array of `len` offsets.
/// - if `data` is NULL then `len` should be 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSOffsetVector_SetData(
    offsets: *mut RSOffsetVector,
    data: *const c_char,
    len: u32,
) {
    debug_assert!(!offsets.is_null(), "offsets must not be null");
    debug_assert!(
        !data.is_null() || len == 0,
        "data must not be null if len is higher than 0"
    );

    // SAFETY: Caller is to ensure `offsets` is non-null and point to a valid RSOffsetVector.
    let offsets = unsafe { &mut *offsets };

    offsets.data = data as _;
    offsets.len = len;
}

/// Free the data inside an [`RSOffsetVector`]'s offset
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `offsets` must point to a valid [`RSOffsetVector`] and cannot be NULL.
/// - The data pointer of `offsets` had been allocated via the global allocator
///   and points to an array matching the length of `offsets`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSOffsetVector_FreeData(offsets: *mut RSOffsetVector) {
    debug_assert!(!offsets.is_null(), "offsets must not be null");

    // SAFETY: Caller is to ensure `offsets` is non-null and point to a valid RSOffsetVector.
    let offsets = unsafe { &mut *offsets };

    offsets.free_data();
}

/// Copy the data from one offset vector to another.
///
/// Deep copies the data array from `src` to `dest`.
/// It's up to the caller to free the copied array using [`RSOffsetVector_FreeData`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `dest` must point to a valid [`RSOffsetVector`] and cannot be NULL.
/// - `src` must point to a valid [`RSOffsetVector`] and cannot be NULL.
/// - `src` data should point to a valid array of `src.len` offsets.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSOffsetVector_CopyData(
    dest: *mut RSOffsetVector,
    src: *const RSOffsetVector,
) {
    debug_assert!(!dest.is_null(), "offsets must not be null");
    debug_assert!(!src.is_null(), "offsets must not be null");

    // SAFETY: Caller is to ensure `src` is non-null and point to a valid RSOffsetVector.
    let src = unsafe { &*src };
    // SAFETY: Caller is to ensure `dest` is non-null and point to a valid RSOffsetVector.
    let dest = unsafe { &mut *dest };

    *dest = src.to_owned();
}

/// Retrieve the number of offsets in [`RSOffsetVector`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `offsets` must point to a valid [`RSOffsetVector`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSOffsetVector_Len(offsets: *const RSOffsetVector) -> u32 {
    debug_assert!(!offsets.is_null(), "offsets must not be null");

    // SAFETY: Caller is to ensure `offsets` is non-null and point to a valid RSOffsetVector.
    let offsets = unsafe { &*offsets };

    offsets.len
}
