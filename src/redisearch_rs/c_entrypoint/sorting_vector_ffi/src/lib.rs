/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

pub use result_processor::sorting_vector::RSSortingVector;
pub use result_processor::sorting_vector::RSValue;

use std::{ffi::c_char, ptr::NonNull};

pub const RS_SORTABLES_MAX: usize = 1024;

pub const RS_SORTABLE_NUM: usize = 1;
pub const RS_SORTABLE_STR: usize = 3;
pub const RS_SORTABLE_NIL: usize = 4;
pub const RS_SORTABLE_RSVAL: usize = 5;

/// Gets a RSValue from the sorting vector at the given index.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Get(
    vec: *const RSSortingVector,
    idx: libc::size_t,
) -> *mut ffi::RSValue {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_ref().unwrap() };
    if idx >= vec.len() {
        return std::ptr::null_mut();
    }

    vec.values[idx].0
}

/// Returns the length of the sorting vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`] or null.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Length(vec: *const RSSortingVector) -> libc::size_t {
    if vec.is_null() {
        return 0;
    }

    // Safety: Caller must ensure 1. --> Deref is safe, we checked for null above
    unsafe { vec.as_ref() }.unwrap().len() as libc::size_t
}

/// Returns the memory size of the sorting vector.
///  
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_GetMemorySize(
    vector: NonNull<RSSortingVector>,
) -> libc::size_t {
    // Safety: Caller must ensure 1. --> Deref is safe
    unsafe { vector.as_ref() }.get_memory_size() as libc::size_t
}

/// Puts a number (double) at the given index in the sorting vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
/// 2. The `idx` must be a valid index within the bounds of the sorting vector.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNum(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    num: f64,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    unsafe { vec.as_mut() }.put_num(idx, num);
}

/// Puts a string at the given index in the sorting vector.
///
/// This function will normalize the string to lowercase and use utf normalization for sorting if `is_normalized` is true.
///
/// Internally it uses `libc` functions to allocate and copy the string, ensuring that string allocation and deallocation
/// all happen on the C side for now.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
/// 2. The `str` pointer must point to a valid C string (null-terminated).
/// 3. The `idx` must be a valid index within the bounds of the sorting vector.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutStr(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    str: *const c_char,
    is_normalized: bool,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    // ffi-side impl ensures to allocate strings from the c allocator, so we can safely use
    if !vec.check(idx) {
        return;
    }

    if is_normalized {
        // Safety: Caller must ensure 2. --> We have a valid C string pointer for duplication
        let dupl = unsafe { libc::strdup(str) };

        // Safety: dupl is a valid pointer to a C string -> we can safely call `libc::strlen`
        let strlen = unsafe { libc::strlen(dupl) };

        // Safety: THe constructor of RString Value with a c str and the right string length
        vec.values[idx] = RSValue(unsafe { ffi::RS_StringVal(dupl, strlen as u32) });
    } else {
        // Safety: Caller must ensure 2. --> We have a valid C string pointer to create a CStr
        let c_str = unsafe { std::ffi::CStr::from_ptr(str) };
        let str_slice = c_str.to_str().unwrap_or("");
        vec.put_string_and_normalize(idx, str_slice);
    }
}

/// Puts a value at the given index in the sorting vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
/// 2. The `val` pointer must point to a valid `RSValue` instance.
/// 3. The `idx` must be a valid index within the bounds of the sorting vector.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutRSVal(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    val: *mut ffi::RSValue,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    unsafe {
        vec.as_mut().put_val(idx, RSValue(val));
    }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn NewSortingVector(len: libc::size_t) -> *mut RSSortingVector {
    if len > RS_SORTABLES_MAX {
        return std::ptr::null_mut();
    }

    let vector = RSSortingVector::new(len);
    Box::into_raw(Box::new(vector))
}

/// Reduces the refcount of every value and frees the memory allocated for an `RSSortingVector`.
/// Called by the C code to deallocate the vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`NewSortingVector`].
/// 2. The pointer must not have been freed before this call to avoid double free.
#[unsafe(no_mangle)]
unsafe extern "C" fn SortingVector_Free(mut vector: NonNull<RSSortingVector>) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vector = unsafe { vector.as_mut() };

    // Safety:
    // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
    // Condition 2 --> Ensures that there is no double free
    let _ = unsafe { Box::from_raw(vector) };
}
