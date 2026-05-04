/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Remote COLLECT reducer FFI.

use reducers::collect::{RemoteCollectCtx, RemoteCollectReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{c_int, c_void},
    ptr, slice,
};

/// Creates a new [`RemoteCollectReducer`] from pre-parsed configuration and
/// returns a pointer to its base [`ffi::Reducer`] with the vtable fully wired.
///
/// The caller is responsible for eventually calling [`collectRemoteFree`] on
/// the returned pointer.
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
pub unsafe extern "C" fn CollectReducer_CreateRemote(
    field_keys: *const *const ffi::RLookupKey,
    field_keys_len: usize,
    load_all: bool,
    sort_keys: *const *const ffi::RLookupKey,
    sort_keys_len: usize,
    sort_asc_map: u64,
    has_limit: bool,
    limit_offset: u64,
    limit_count: u64,
    include_sort_keys: bool,
) -> *mut ffi::Reducer {
    let field_keys: Box<[&RLookupKey]> = if !field_keys.is_null() && field_keys_len > 0 {
        // SAFETY: ensured by caller (1.)
        Box::from(unsafe {
            slice::from_raw_parts(field_keys.cast::<&RLookupKey>(), field_keys_len)
        })
    } else {
        Box::new([])
    };

    let sort_keys: Box<[&RLookupKey]> = if !sort_keys.is_null() && sort_keys_len > 0 {
        // SAFETY: ensured by caller (2.)
        Box::from(unsafe { slice::from_raw_parts(sort_keys.cast::<&RLookupKey>(), sort_keys_len) })
    } else {
        Box::new([])
    };

    let limit = has_limit.then_some((limit_offset, limit_count));

    let mut cr = Box::new(RemoteCollectReducer::new(
        field_keys,
        load_all,
        sort_keys,
        sort_asc_map,
        limit,
        include_sort_keys,
    ));

    cr.reducer_mut()
        .set_new_instance(collectRemoteNewInstance)
        .set_add(collectRemoteAdd)
        .set_finalize(collectRemoteFinalize)
        .set_free_instance(collectRemoteFreeInstance)
        .set_free(collectRemoteFree);

    Box::into_raw(cr).cast()
}

/// Creates a new per-group shard collect reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ShardCollectReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectRemoteNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// Frees a per-group shard collect reducer instance.
///
/// # Safety
///
/// 1. `ctx` must point to a [valid] `ShardCollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectRemoteFreeInstance(_r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.) — `ctx` points to a valid, initialized
    // `RemoteCollectCtx`. After this call the pointee is logically uninitialized,
    // but the arena memory is freed later when `RemoteCollectReducer` (and its
    // `Bump`) is dropped.
    unsafe { ptr::drop_in_place(ctx.cast::<RemoteCollectCtx>()) }
}

/// Processes the provided [`ffi::RLookupRow`] with the shard collect reducer
/// instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ShardCollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `ShardCollectCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectRemoteAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { ctx.cast::<RemoteCollectCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    collect.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// Finalizes the shard collect reducer instance result into an `RSValue`.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ShardCollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `ShardCollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectRemoteFinalize(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { ctx.cast::<RemoteCollectCtx>().as_mut().unwrap() };

    collect.finalize(r).into_raw() as *mut ffi::RSValue
}

/// Frees the provided shard collect reducer (the global struct, not a
/// per-group instance).
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ShardCollectReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`CollectReducer_CreateRemote`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectRemoteFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<RemoteCollectReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<RemoteCollectReducer>()) });
}

// --- Accessors for C++ parser tests (temporary) ---
//
// These exist solely so `test_cpp_collect.cpp` can inspect `ShardCollectReducer`
// configuration after parsing. The parsing logic lives in C, so we cannot
// yet test it with Rust flow tests.
//
// TODO: migrate the C++ parser tests to Python flow tests
// (`test_groupby_collect.py`), then delete `test_cpp_collect.cpp`, these
// FFI accessors, and the corresponding methods in `reducers/src/collect/shard.rs`.
//
/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_GetFieldKeysLen(r: *const ffi::Reducer) -> usize {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.field_keys_len()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_HasLoadAll(r: *const ffi::Reducer) -> bool {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.load_all()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_GetSortKeysLen(r: *const ffi::Reducer) -> usize {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.sort_keys_len()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_GetSortAscMap(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.sort_asc_map()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_HasLimit(r: *const ffi::Reducer) -> bool {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.has_limit()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_GetLimitOffset(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.limit_offset()
}

/// # Safety
///
/// `r` must point to a valid [`RemoteCollectReducer`] originally created by
/// `CollectReducer_CreateRemote`.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn CollectReducer_GetLimitCount(r: *const ffi::Reducer) -> u64 {
    // SAFETY: ensured by caller.
    let r = unsafe { r.cast::<RemoteCollectReducer>().as_ref().unwrap() };
    r.limit_count()
}
