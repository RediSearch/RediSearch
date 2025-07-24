/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use inverted_index::{RSAggregateResult, RSAggregateResultIter, RSIndexResult};

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

/// Get the result at the specified index in the aggregate result. This will return a `NULL` pointer
/// if the index is out of bounds.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
/// - The memory address at `index` should still be valid and not have been deallocated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Get(
    agg: *const RSAggregateResult,
    index: usize,
) -> *const RSIndexResult {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    if let Some(next) = agg.get(index) {
        next
    } else {
        std::ptr::null()
    }
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

/// Get the type mask of the aggregate result.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_TypeMask(agg: *const RSAggregateResult) -> u32 {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &*agg };

    agg.type_mask().bits()
}

/// Reset the aggregate result, clearing all children and resetting the type mask. This function
/// does not deallocate the children pointers, but rather resets the internal state of the
/// aggregate result. The owner of the children pointers is responsible for managing their lifetime.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Reset(agg: *mut RSAggregateResult) {
    debug_assert!(!agg.is_null(), "agg must not be null");

    // SAFETY: Caller is to ensure that the pointer `agg` is a valid, non-null pointer to
    // an `RSAggregateResult`.
    let agg = unsafe { &mut *agg };

    agg.reset();
}

/// Create a new aggregate result with the specified capacity. This function will make the result
/// in Rust memory, but the ownership ends up being transferred to C's memory space. This ownership
/// should return to Rust to free up any heap memory using [`AggregateResult_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_New(cap: usize) -> RSAggregateResult {
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
    drop(agg); // Explicit for clarity - automatically frees LowMemoryThinVec buffer
}

/// Add a child to a result if it is an aggregate result. Note, `parent` will not take ownership of
/// the `child` and will therefore not free it. Instead, the caller is responsible for managing
/// the memory of the `child` pointer *after* the `parent` has been freed.
///
/// If the `parent` is not an aggregate type, then this is a no-op.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `parent` must point to a valid `RSIndexResult` and cannot be NULL.
/// - `child` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_AddChild(
    parent: *mut RSIndexResult,
    child: *mut RSIndexResult,
) {
    debug_assert!(!parent.is_null(), "parent must not be null");
    debug_assert!(!child.is_null(), "child must not be null");

    // SAFETY: Caller is to ensure that `parent` is a valid, non-null pointer to an `RSIndexResult`
    let parent = unsafe { &mut *parent };

    // SAFETY: Caller is to ensure that `child` is a valid, non-null pointer to an `RSIndexResult`
    let child = unsafe { &*child };

    parent.push(child);
}

/// Create an iterator over the aggregate result. This iterator should be freed
/// using [`AggregateResultIter_Free`].
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `agg` must point to a valid `RSAggregateResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AggregateResult_Iter(
    agg: *const RSAggregateResult,
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
