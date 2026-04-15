/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::collect::{CollectCtx, CollectReducer};
use std::ffi::{c_int, c_void};

/// Creates a new [`CollectReducer`] from pre-parsed configuration and returns a
/// pointer to its base [`ffi::Reducer`] with the vtable fully wired.
///
/// The caller is responsible for eventually calling [`collectFree`] on the
/// returned pointer.
///
/// # Safety
///
/// 1. If `field_keys_len > 0`, `field_keys` must point to an array of at least
///    `field_keys_len` [valid] `*const RLookupKey` pointers.
/// 2. If `sort_keys_len > 0`, `sort_keys` must point to an array of at least
///    `sort_keys_len` [valid] `*const RLookupKey` pointers.
/// 3. All [`RLookupKey`][ffi::RLookupKey] pointers must remain valid for the
///    lifetime of the returned reducer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_Create(
    field_keys: *const *const ffi::RLookupKey,
    field_keys_len: usize,
    has_wildcard: bool,
    sort_keys: *const *const ffi::RLookupKey,
    sort_keys_len: usize,
    sort_asc_map: u64,
    has_limit: bool,
    limit_offset: u64,
    limit_count: u64,
) -> *mut ffi::Reducer {
    let field_keys = if !field_keys.is_null() && field_keys_len > 0 {
        // SAFETY: ensured by caller (1.)
        unsafe { std::slice::from_raw_parts(field_keys, field_keys_len) }.to_vec()
    } else {
        Vec::new()
    };

    let sort_keys = if !sort_keys.is_null() && sort_keys_len > 0 {
        // SAFETY: ensured by caller (2.)
        unsafe { std::slice::from_raw_parts(sort_keys, sort_keys_len) }.to_vec()
    } else {
        Vec::new()
    };

    let mut cr = Box::new(CollectReducer::new(
        field_keys,
        has_wildcard,
        sort_keys,
        sort_asc_map,
        has_limit,
        limit_offset,
        limit_count,
    ));

    cr.reducer_mut()
        .set_new_instance(collectNewInstance)
        .set_add(collectAdd)
        .set_finalize(collectFinalize)
        .set_free_instance(collectFreeInstance)
        .set_free(collectFree);

    Box::into_raw(cr).cast()
}

/// Creates a new per-group collect reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_mut().unwrap() };

    let ctx: *mut CollectCtx = r.alloc_instance();
    ctx.cast()
}

/// Frees a per-group collect reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `CollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectFreeInstance(r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { &mut *ctx.cast::<CollectCtx>() };

    collect.free(r);
}

/// Processes the provided [`ffi::RLookupRow`] with the collect reducer
/// instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `CollectCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { &mut *ctx.cast::<CollectCtx>() };

    collect.add(r, srcrow);

    1
}

/// Finalizes the collect reducer instance result into an `RSValue`.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `CollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectFinalize(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { &*ctx.cast::<CollectCtx>() };

    collect.finalize(r).into_raw()
}

/// Frees the provided collect reducer (the global struct, not a per-group
/// instance).
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.)
    let _ = unsafe { Box::from_raw(r.cast::<CollectReducer>()) };
}

// --- Accessors for C/C++ tests ---
//
// These allow the C++ parser tests to inspect CollectReducer config without
// duplicating the struct layout. Unused symbols are stripped by the linker
// in release builds.

/// Returns the number of explicitly listed field keys (excludes the wildcard).
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_GetFieldKeysLen(r: *const ffi::Reducer) -> usize {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.field_keys_len()
}

/// Returns whether the wildcard `*` was specified in the FIELDS clause.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_HasWildcard(r: *const ffi::Reducer) -> bool {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.has_wildcard()
}

/// Returns the number of sort keys.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_GetSortKeysLen(r: *const ffi::Reducer) -> usize {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.sort_keys_len()
}

/// Returns the ASC/DESC bitmask for sort keys.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_GetSortAscMap(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.sort_asc_map()
}

/// Returns whether a LIMIT clause was specified.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_HasLimit(r: *const ffi::Reducer) -> bool {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.has_limit()
}

/// Returns the LIMIT offset value.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_GetLimitOffset(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.limit_offset()
}

/// Returns the LIMIT count value.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a
///    `ffi::Reducer`, originally created by [`CollectReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_GetLimitCount(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CollectReducer>().as_ref().unwrap() };
    r.limit_count()
}
