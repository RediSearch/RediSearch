/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::c_char,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use ffi::RS_StringVal;

use value::{RSValueFFI, RSValueTrait};

pub const RS_SORTABLES_MAX: usize = 1024;

pub struct RSSortingVector {
    inner: sorting_vector::RSSortingVector<RSValueFFI>,
}

impl Deref for RSSortingVector {
    type Target = sorting_vector::RSSortingVector<RSValueFFI>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for RSSortingVector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

/// Gets a RSValue from the sorting vector at the given index. If a out of bounds occurs it returns a nullptr.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Get(
    vec: *const RSSortingVector,
    idx: libc::size_t,
) -> *mut ffi::RSValue {
    if vec.is_null() {
        panic!("RSSortingVector_Get called with null pointer");
    }

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { &*vec };
    if idx >= vec.len() {
        return std::ptr::null_mut();
    }

    vec[idx].0.as_ptr()
}

/// Returns the length of the sorting vector. For nullptr it returns 0.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or null.
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
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_GetMemorySize(
    vector: NonNull<RSSortingVector>,
) -> libc::size_t {
    // Safety: Caller must ensure 1. --> Deref is safe
    unsafe { vector.as_ref() }.get_memory_size() as libc::size_t
}

/// Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNum(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    num: f64,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    let _ = vec.try_insert_num(idx, num);
}

/// Puts a string at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// This function will normalize the string to lowercase and use utf normalization for sorting if `is_normalized` is true.
///
/// Internally it uses `libc` functions to allocate and copy the string, ensuring that string allocation and deallocation
/// all happen on the C side for now.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `str` pointer must point to a valid C string (null-terminated).
/// 3. The `str` pointer must be normalized (lowercase and utf normalization).
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutStr(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    str: *const c_char,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };

    // Safety: Caller must ensure 2. --> strlen gets a valid C string pointer
    let len = unsafe { libc::strlen(str) };

    // Safety: RS_StringVal receives a valid C string pointer (1) and length
    let value = unsafe { RS_StringVal(str.cast_mut(), len as u32) };
    // Safety: We assume RS_StringVal never returns a null pointer
    let value = unsafe { NonNull::new_unchecked(value) };
    let value = RSValueFFI(value);

    let _ = vec.try_insert_val(idx, value);
}

/// Puts a value at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `val` pointer must point to a valid `RSValue` instance.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutRSVal(
    mut vec: NonNull<RSSortingVector>,
    idx: libc::size_t,
    val: NonNull<ffi::RSValue>,
) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    let _ = vec.try_insert_val(idx, RSValueFFI(val));
}

/// Puts a null at the given index in the sorting vector.  If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNull(mut vec: NonNull<RSSortingVector>, idx: libc::size_t) {
    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    let _ = vec.try_insert_null(idx);
}

/// Creates a new `RSSortingVector` with the given length. If the length is greater than `RS_SORTABLES_MAX`=`1024`, it returns a null pointer.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_New(len: libc::size_t) -> *mut RSSortingVector {
    if len > RS_SORTABLES_MAX {
        return std::ptr::null_mut();
    }

    let vector = RSSortingVector {
        inner: sorting_vector::RSSortingVector::new(len),
    };
    Box::into_raw(Box::new(vector))
}

/// Reduces the refcount of every `RSValue` and frees the memory allocated for an `RSSortingVector`.
/// Called by the C code to deallocate the vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The pointer must not have been freed before this call to avoid double free.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Free(vector: *mut RSSortingVector) {
    if vector.is_null() {
        return;
    }

    // Safety:
    // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
    // Condition 2 --> Ensures that there is no double free
    let _ = unsafe { Box::from_raw(vector) };
}
