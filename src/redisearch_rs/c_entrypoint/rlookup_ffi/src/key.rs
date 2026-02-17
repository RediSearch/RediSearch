/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use rlookup::RLookupKey;
use std::ffi::c_char;

/// Get the flags (indicating the type and other attributes) for a `RLookupKey`.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetFlags(key: *const RLookupKey) -> u32 {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.flags.bits()
}

/// Get the index into the array where the value resides.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetDstIdx(key: *const RLookupKey) -> u16 {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.dstidx
}

/// Get the index within the sort vector where the value is located.
///
/// If the source of this value points to a sort vector, then this is the
/// index within the sort vector that the value is located.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetSvIdx(key: *const RLookupKey) -> u16 {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.svidx
}

/// Get the name of the field.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetName(key: *const RLookupKey) -> *const c_char {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.name().as_ptr()
}

/// Get the length of the name field in bytes.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetNameLen(key: *const RLookupKey) -> size_t {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.name().count_bytes()
}

/// Get the path of the field.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupKey_GetPath(key: *const RLookupKey) -> *const c_char {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref().unwrap() };

    key.path().as_ref().unwrap_or(key.name()).as_ptr()
}
