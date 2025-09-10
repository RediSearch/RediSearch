/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, panic, ptr::NonNull};

use ffi::RS_StringVal;
use value::{RSValueFFI, RSValueTrait as _};

pub const RS_SORTABLES_MAX: usize = 1024;

/// cbindgen:ignore
pub type RSSortingVector = sorting_vector::RSSortingVector<RSValueFFI>;

/// Gets a RSValue from the sorting vector at the given index. If a out of bounds occurs it returns a nullptr.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Get(
    vec: *const RSSortingVector,
    idx: libc::size_t,
) -> *mut ffi::RSValue {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

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
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`] or null.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Length(vec: *const RSSortingVector) -> libc::size_t {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

    vec.len() as libc::size_t
}

/// Returns the memory size of the sorting vector.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_GetMemorySize(vec: *const RSSortingVector) -> libc::size_t {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

    vec.get_memory_size() as libc::size_t
}

/// Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNum(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    num: f64,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

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
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `str` must be a [valid] pointer to a C string (null-terminated).
/// 3. The `str` pointer must be normalized (lowercase and utf normalization).
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutStr(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    str: *const c_char,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    // Safety: Caller must ensure 2. --> strlen gets a valid C string pointer
    let len = unsafe { libc::strlen(str) };

    // Safety: RS_StringVal receives a valid C string pointer (1) and length
    let value = unsafe { RS_StringVal(str.cast_mut(), len as u32) };

    // Safety: We assume RS_StringVal always returns valid pointers
    let value = unsafe {
        RSValueFFI::from_raw(NonNull::new(value).expect("RS_StringVal returned nullptr"))
    };

    vec.try_insert_val(idx, value).unwrap_or_else(|_| {
        panic!("Index out of bounds: {} >= {}", idx, vec.len());
    });
}

/// Puts a value at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The `val` must be a [valid] pointer must point to a `RSValue`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutRSVal(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    val: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    // Safety: The caller must ensure that the pointer is valid (2.)
    let val = unsafe { RSValueFFI::from_raw(val.expect("val must not be null")) };

    vec.try_insert_val(idx, val).unwrap_or_else(|_| {
        panic!("Index out of bounds: {} >= {}", idx, vec.len());
    });
}

/// Puts a null at the given index in the sorting vector.  If a out of bounds occurs it returns silently.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_PutNull(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

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

    let vec = RSSortingVector::new(len);
    Box::into_raw(Box::new(vec))
}

/// Reduces the refcount of every `RSValue` and frees the memory allocated for an `RSSortingVector`.
/// Called by the C code to deallocate the vector.
///
/// Safety:
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. The pointer must not have been freed before this call to avoid double free.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RSSortingVector_Free(vec: *mut RSSortingVector) {
    // We allow null in free as this is C standard behavior and used in RediSearch codebase.
    if vec.is_null() {
        return;
    }

    // Safety:
    // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
    // Condition 2 --> Ensures that there is no double free
    drop(unsafe { Box::from_raw(vec) });
}
