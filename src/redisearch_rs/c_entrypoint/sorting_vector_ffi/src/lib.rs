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
use std::ffi::{CStr, c_char};
use std::ptr::NonNull;
use std::slice;
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
    assert!(!vec.is_null(), "vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_from_opaque_ptr(vec as *const ()) };

    values[idx].as_ptr()
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
    assert!(!vec.is_null(), "vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    unsafe { RSSortingVector::len_from_opaque_ptr(vec as *const ()) as size_t }
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
    assert!(!vec.is_null(), "vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_from_opaque_ptr(vec as *const ()) };

    RSSortingVector::compute_memory_size(values) as size_t
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
    let vec = vec.expect("vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_mut_from_raw(vec.as_ptr() as *mut _) };

    let len = values.len();
    let spot = values.get_mut(idx).unwrap_or_else(|| {
        panic!("Index out of bounds: {} >= {}", idx, len);
    });
    *spot = RSValueFFI::new_num(num);
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
    let vec = vec.expect("vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_mut_from_raw(vec.as_ptr() as *mut _) };

    // Safety: The caller must ensure str points to a valid C string (2.)
    let str = unsafe { CStr::from_ptr(str) };

    // Safety: The caller must ensure str must be valid (2.)
    let str = unsafe { slice::from_raw_parts(str.as_ptr().cast(), str.count_bytes()) };

    let len = values.len();
    let spot = values.get_mut(idx).unwrap_or_else(|| {
        panic!("Index out of bounds: {} >= {}", idx, len);
    });
    *spot = RSValueFFI::new_string(str.to_vec());
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
    let vec = vec.expect("vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.) We reconstruct a temporary
    // RSSortingVector to use the normalize method which needs ICU.
    let mut sv = unsafe { RSSortingVector::from_raw_ptr(vec.cast()) };

    // Safety: The caller must ensure str points to a valid C string (2.)
    let str = unsafe { CStr::from_ptr(str) };

    let str = str.to_str().expect("value is invalid UTF-8");

    sv.try_insert_string_normalize(idx, str)
        .unwrap_or_else(|_| {
            panic!("Index out of bounds: {} >= {}", idx, sv.len());
        });

    // Leak the RSSortingVector back into the raw pointer (don't free the allocation)
    sv.into_raw_ptr();
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
    let vec = vec.expect("vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_mut_from_raw(vec.as_ptr() as *mut _) };

    // Safety: The caller must ensure that the pointer is valid (2.)
    let val = unsafe { RSValueFFI::from_raw(val.expect("val must not be null")) };

    let len = values.len();
    let spot = values.get_mut(idx).unwrap_or_else(|| {
        panic!("Index out of bounds: {} >= {}", idx, len);
    });
    *spot = val;
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
    let vec = vec.expect("vec must not be null");
    // SAFETY: The caller ensures the pointer is valid (1.)
    let values = unsafe { RSSortingVector::borrow_values_mut_from_raw(vec.as_ptr() as *mut _) };

    let len = values.len();
    let spot = values.get_mut(idx).unwrap_or_else(|| {
        panic!("Index out of bounds: {} >= {}", idx, len);
    });
    *spot = RSValueFFI::null_static();
}

/// Creates a new `RSSortingVector` with the given length.
///
/// Returns a raw pointer to the backing `SmallThinVec` header allocation. This is a single
/// heap allocation containing `{len, cap, [RSValueFFI; len]}`.
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
    vec.into_raw_ptr().as_ptr() as *mut RSSortingVector
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
        // SAFETY:
        // Condition 1 --> Ensures this is a valid pointer from RSSortingVector_New (into_raw_ptr)
        // Condition 2 --> Ensures that there is no double free
        drop(unsafe { RSSortingVector::from_raw_ptr(vec.cast()) });
    } else {
        // We allow null in free as this is C standard behavior and used in RediSearch codebase.
    }
}
