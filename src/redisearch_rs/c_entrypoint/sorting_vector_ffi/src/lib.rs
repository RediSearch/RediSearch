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
    panic,
    ptr::NonNull,
};

use ffi::RSValue_NewStringAlloc;
use value::{RSValueFFI, RSValueTrait as _};

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
    assert!(
        !vec.is_null(),
        "RSSortingVector_Get called with null pointer"
    );

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { &*vec };
    if idx >= vec.len() {
        panic!(
            "RSSortingVector_Get: Index out of bounds: {} >= {}",
            idx,
            vec.len()
        );
    }

    vec[idx].as_ptr()
}

/// Returns the length of the sorting vector. For nullptr it returns 0.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or null.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Length(vec: *const RSSortingVector) -> libc::size_t {
    assert!(
        !vec.is_null(),
        "RSSortingVector_Length called with null pointer",
    );

    // Safety: Caller must ensure 1. --> Deref is safe, we checked for null above
    let vec = unsafe { vec.as_ref() };

    // Safety: We checked that vec is not null, so unwrap is safe
    unsafe { vec.unwrap_unchecked() }.len() as libc::size_t
}

/// Returns the memory size of the sorting vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_GetMemorySize(
    vector: Option<NonNull<RSSortingVector>>,
) -> libc::size_t {
    assert!(
        vector.is_some(),
        "RSSortingVector_GetMemorySize called with null pointer"
    );

    // Safety: We checked for null above, so unwrap is safe
    let vector = unsafe { vector.unwrap_unchecked() };

    // Safety: Caller must ensure 1. --> Deref is safe
    unsafe { vector.as_ref() }.get_memory_size() as libc::size_t
}

/// Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNum(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    num: f64,
) {
    assert!(
        vec.is_some(),
        "RSSortingVector_PutNum called with null pointer"
    );
    // Safety: We checked for null above, so unwrap is safe
    let mut vec = unsafe { vec.unwrap_unchecked() };

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    vec.try_insert_val(idx, RSValueFFI::create_num(num))
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, vec.len());
        });
}

/// Puts a string at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// This function will normalize the string to lowercase and use utf normalization for sorting if `is_normalized` is true.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `str` pointer must point to a valid C string (null-terminated).
/// 3. The `str` pointer must be normalized (lowercase and utf normalization).
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutStr(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    str: *const c_char,
) {
    assert!(
        vec.is_some(),
        "RSSortingVector_PutStr called with null pointer"
    );

    // Safety: We checked for null above, so unwrap is safe
    let mut vec = unsafe { vec.unwrap_unchecked() };

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };

    // Safety: Caller must ensure 2. --> strlen gets a valid C string pointer
    let len = unsafe { libc::strlen(str) };

    // Safety: RSValue_NewStringAlloc receives a valid C string pointer (1) and length
    let value = unsafe { RSValue_NewStringAlloc(str.cast_mut(), len as u32) };

    // Safety: We assume RSValue_NewStringAlloc always returns valid pointers
    let value = unsafe {
        RSValueFFI::from_raw(NonNull::new(value).expect("RSValue_NewStringAlloc returned nullptr"))
    };

    vec.try_insert_val(idx, value).unwrap_or_else(|_| {
        panic!("Index out of bounds: {} >= {}", idx, vec.len());
    });
}

/// Puts a value at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `val` pointer must point to a valid `RSValue` instance.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutRSVal(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    val: Option<NonNull<ffi::RSValue>>,
) {
    assert!(
        vec.is_some(),
        "RSSortingVector_PutRSVal called with null pointer"
    );
    assert!(
        val.is_some(),
        "RSSortingVector_PutRSVal called with null RSValue pointer"
    );

    // Safety: We checked for null above, so unwrap is safe
    let mut vec = unsafe { vec.unwrap_unchecked() };
    // Safety: We checked for null above, so unwrap is safe
    let val = unsafe { val.unwrap_unchecked() };

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    // Safety: Caller must ensure 2. --> pointer is valid
    vec.try_insert_val(idx, unsafe { RSValueFFI::from_raw(val) })
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, vec.len());
        });
}

/// Puts a null at the given index in the sorting vector.  If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNull(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
) {
    assert!(
        vec.is_some(),
        "RSSortingVector_PutNull called with null pointer"
    );
    // Safety: We checked for null above, so unwrap is safe
    let mut vec = unsafe { vec.unwrap_unchecked() };

    // Safety: Caller must ensure 1. --> Deref is safe
    let vec = unsafe { vec.as_mut() };
    vec.try_insert_null(idx).unwrap_or_else(|_| {
        panic!("Index out of bounds: {} >= {}", idx, vec.len());
    });
}

/// Creates a new `RSSortingVector` with the given length. If the length is greater than `RS_SORTABLES_MAX`=`1024`, it returns a null pointer.
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_New(len: libc::size_t) -> *mut RSSortingVector {
    assert!(
        len <= RS_SORTABLES_MAX,
        "RSSortingVector_New called with length greater than RS_SORTABLES_MAX ({RS_SORTABLES_MAX})"
    );

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
    // We allow null in free as this is C standard behavior and used in RediSearch codebase.
    if vector.is_null() {
        return;
    }

    // Safety:
    // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
    // Condition 2 --> Ensures that there is no double free
    drop(unsafe { Box::from_raw(vector) });
}
