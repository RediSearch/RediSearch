/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use sorting_vector::RSSortingVector;
use std::slice;
use std::{
    ffi::{CStr, c_char},
    panic,
    ptr::NonNull,
};
use value::RSValueFFI;

pub const RS_SORTABLES_MAX: usize = 1024;

/// Gets a RSValue from the sorting vector at the given index.
///
/// # Panics
///
/// Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_Get(
    vec: *const RSSortingVector,
    idx: size_t,
) -> *mut ffi::RSValue {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

    vec[idx].as_ptr()
}

/// Returns the length of the sorting vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_Length(vec: *const RSSortingVector) -> size_t {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

    vec.len() as size_t
}

/// Returns the memory size of the sorting vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_GetMemorySize(vec: *const RSSortingVector) -> size_t {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.as_ref().expect("vec must not be null") };

    vec.get_memory_size() as size_t
}

/// Puts a number (double) at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// # Panics
///
/// Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_PutNum(
    vec: Option<NonNull<RSSortingVector>>,
    idx: size_t,
    num: f64,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    vec.try_insert_val(idx, RSValueFFI::new_num(num))
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, vec.len());
        });
}

/// Puts a string at the given index in the sorting vector.
///
/// # Panics
///
/// Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. `str` must be a [valid], non-null pointer to a C string (null-terminated).
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_PutStr(
    vec: Option<NonNull<RSSortingVector>>,
    idx: size_t,
    str: *const c_char,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    // Safety: The caller must ensure str points to a valid C string (2.)
    let str = unsafe { CStr::from_ptr(str) };

    // Safety: The caller must ensure str mist be valid (2.)
    let str = unsafe { slice::from_raw_parts(str.as_ptr().cast(), str.count_bytes()) };

    vec.try_insert_string(idx, str.to_vec())
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, vec.len());
        });
}

/// Puts a string at the given index in the sorting vector, the string is normalized before being set.
///
/// # Panics
///
/// - Panics if the provided string is invalid UTF-8
/// - Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. `str` must be a [valid], non-null pointer to a C string (null-terminated).
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_PutStrNormalize(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
    str: *const c_char,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    // Safety: The caller must ensure str points to a valid C string (2.)
    let str = unsafe { CStr::from_ptr(str) };

    let str = str.to_str().expect("value is invalid UTF-8");

    vec.try_insert_string_normalize(idx, str)
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, vec.len());
        });
}

/// Puts a value at the given index in the sorting vector. If a out of bounds occurs it returns silently.
///
/// # Panics
///
/// Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. `vec` must be a [valid], non-null pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. `val` must be a [valid], non-null pointer must point to a `RSValue`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_PutRSVal(
    vec: Option<NonNull<RSSortingVector>>,
    idx: size_t,
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
/// # Panics
///
/// Panics if the `idx` is out of bounds for the vector.
///
/// # Safety
///
/// 1. The pointer must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_PutNull(
    vec: Option<NonNull<RSSortingVector>>,
    idx: libc::size_t,
) {
    // Safety: The caller must ensure that the pointer is valid (1.)
    let vec = unsafe { vec.expect("vec must not be null").as_mut() };

    vec.try_insert_null(idx).unwrap_or_else(|_| {
        panic!("Index out of bounds: {} >= {}", idx, vec.len());
    });
}

/// Creates a new `RSSortingVector` with the given length.
///
/// # Panics
///
/// Panics if `len` is greater than [`RS_SORTABLES_MAX`].
#[unsafe(no_mangle)]
pub extern "C" fn RSSortingVector_New(len: size_t) -> *mut RSSortingVector {
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
/// # Safety
///
/// 1. `vec` must be a [valid] pointer to an [`RSSortingVector`] created by [`RSSortingVector_New`].
/// 2. `vec` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSSortingVector_Free(vec: Option<NonNull<RSSortingVector>>) {
    if let Some(vec) = vec {
        // Safety:
        // Condition 1 --> Ensures this is a valid pointer to an RSSortingVector created by RSSortingVector_New
        // Condition 2 --> Ensures that there is no double free
        drop(unsafe { Box::from_raw(vec.as_ptr()) });
    } else {
        // We allow null in free as this is C standard behavior and used in RediSearch codebase.
    }
}
