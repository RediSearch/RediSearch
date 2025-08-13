/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use std::{alloc::Layout, ffi::c_char};

use inverted_index::{
    RSAggregateResult, RSAggregateResultIter, RSIndexResult, RSOffsetVector, RSTermRecord,
};

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

    result.as_numeric().map_or(0.0, |num| num.0)
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
        num.0 = value;
    }
}

/// Get the term of the result if it is a term result. If the result is not a term, this function
/// will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_TermRef<'result, 'index, 'aggregate_children>(
    result: *const RSIndexResult<'index, 'aggregate_children>,
) -> Option<&'result RSTermRecord<'index>>
where
    'aggregate_children: 'result,
{
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result: &'result _ = unsafe { &*result };

    result.as_term()
}

/// Get the mutable term of the result if it is a term result. If the result is not a term,
/// this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_TermRefMut<'result, 'index, 'aggregate_children>(
    result: *mut RSIndexResult<'index, 'aggregate_children>,
) -> Option<&'result mut RSTermRecord<'index>>
where
    'aggregate_children: 'result,
{
    debug_assert!(!result.is_null(), "result must not be null");

    // SAFETY: Caller is to ensure that the pointer `result` is a valid, non-null pointer to
    // an `RSIndexResult`.
    let result: &'result mut _ = unsafe { &mut *result };

    result.as_term_mut()
}

/// Get the aggregate result reference if the result is an aggregate result. If the result is
/// not an aggregate, this function will return a `NULL` pointer.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `result` must point to a valid `RSIndexResult` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_AggregateRef<'result, 'index, 'aggregate_children>(
    result: *const RSIndexResult<'index, 'aggregate_children>,
) -> Option<&'result RSAggregateResult<'index, 'aggregate_children>> {
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
pub unsafe extern "C" fn AggregateResult_Get<'result, 'index, 'aggregate_children>(
    agg: *const RSAggregateResult<'index, 'aggregate_children>,
    index: usize,
) -> Option<&'result RSIndexResult<'index, 'aggregate_children>> {
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
pub extern "C" fn AggregateResult_New(cap: usize) -> RSAggregateResult<'static, 'static> {
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
/// If the `parent` is not an aggregate kind, then this is a no-op.
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
    agg: *const RSAggregateResult<'static, 'static>,
) -> *mut RSAggregateResultIter<'static, 'static> {
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
    iter: *mut RSAggregateResultIter<'static, 'static>,
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
pub unsafe extern "C" fn AggregateResultIter_Free(
    iter: *mut RSAggregateResultIter<'static, 'static>,
) {
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

    if offsets.data.is_null() {
        return;
    }

    let layout = Layout::array::<c_char>(offsets.len as usize).unwrap();
    // SAFETY: Caller is to ensure data has been allocated via the global allocator
    // and points to an array matching the length of `offsets`.
    unsafe { std::alloc::dealloc(offsets.data.cast(), layout) };

    offsets.data = std::ptr::null_mut();
    offsets.len = 0;
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

    dest.len = src.len;

    if src.len > 0 {
        debug_assert!(!src.data.is_null(), "src data must not be null");
        let layout = Layout::array::<c_char>(src.len as usize).unwrap();
        // SAFETY: we just checked that len > 0
        dest.data = unsafe { std::alloc::alloc(layout).cast() };
        // SAFETY:
        // - The source buffer and the destination buffer don't overlap because
        //   they belong to distinct non-overlapping allocations.
        // - The destination buffer is valid for writes of `src.len` elements
        //   since it was just allocated with capacity `src.len`.
        // - The source buffer is valid for reads of `src.len` elements as a call invariant.
        unsafe { std::ptr::copy_nonoverlapping(src.data, dest.data, src.len as usize) };
    } else {
        dest.data = std::ptr::null_mut();
    }
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
