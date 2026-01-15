/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{mem, ptr::NonNull};

pub type SearchResult = search_result::SearchResult<'static>;

/// Overrides the contents of `dst` with those from `src` taking ownership of `src`.
/// Ensures proper cleanup of any existing data in `dst`.
///
/// # Safety
///
/// 1. `dst` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `src` must be a [valid], non-null pointer to a [`SearchResult`].
/// 3. `src` must not be used again.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Override(
    dst: Option<NonNull<SearchResult>>,
    src: Option<NonNull<SearchResult>>,
) {
    // Safety: ensured by caller (1.)
    let dst = unsafe { dst.unwrap().as_mut() };

    // Safety: ensured by caller (2.,3.)
    let _ = mem::replace(dst, unsafe { src.unwrap().read() });
}

/// Clears the [`SearchResult`] pointed to by `res`, removing all values from its [`RLookupRow`][ffi::RLookupRow].
/// This has no effect on the allocated capacity of the lookup row.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Clear(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.clear();
}

/// Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Destroy(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.,2.)
    unsafe { res.unwrap().drop_in_place() };
}

/// Moves the contents the [`SearchResult`] pointed to by `res` into a new heap allocation.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_AllocateMove(
    res: Option<NonNull<SearchResult>>,
) -> *mut SearchResult {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().read() };

    let res = Box::new(res);
    Box::into_raw(res)
}

/// Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_DeallocateDestroy(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.,2.)
    let res = unsafe { Box::from_raw(res.unwrap().as_ptr()) };
    drop(res);
}
