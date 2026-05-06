/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Local COLLECT reducer FFI.

use reducers::collect::{LocalCollectCtx, LocalCollectReducer};
use rlookup::{RLookupKey, RLookupRow};
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr, slice,
};

/// # Safety
///
/// If `len > 0`, `names` must point to an array of at least `len` valid,
/// NUL-terminated C strings. Each pointer's pointee must remain valid for
/// the duration of this call.
unsafe fn copy_c_names(names: *const *const c_char, len: usize) -> Box<[Box<[u8]>]> {
    if names.is_null() || len == 0 {
        return Box::new([]);
    }
    // SAFETY: caller guarantees the slice of pointers is valid.
    let name_ptrs = unsafe { slice::from_raw_parts(names, len) };
    name_ptrs
        .iter()
        .map(|&p| {
            // SAFETY: caller guarantees each pointer references a valid
            // NUL-terminated C string.
            let bytes = unsafe { CStr::from_ptr(p) }.to_bytes();
            bytes.to_vec().into_boxed_slice()
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// Create a local COLLECT reducer; free it with [`collectLocalFree`].
///
/// # Safety
///
/// 1. `input_key` must be a [valid] pointer to an [`RLookupKey`] that remains
///    alive for the lifetime of the returned reducer.
/// 2. If `field_names_len > 0`, `field_names` must point to an array of at
///    least `field_names_len` valid, NUL-terminated C strings.
/// 3. If `sort_names_len > 0`, `sort_names` must point to an array of at
///    least `sort_names_len` valid, NUL-terminated C strings.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn CollectReducer_CreateLocal(
    input_key: *const ffi::RLookupKey,
    field_names: *const *const c_char,
    field_names_len: usize,
    load_all: bool,
    sort_names: *const *const c_char,
    sort_names_len: usize,
    sort_asc_map: u64,
    has_limit: bool,
    limit_offset: u64,
    limit_count: u64,
) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.); `input_key` points to a valid
    // `RLookupKey` that outlives the returned reducer.
    let input_key: &RLookupKey = unsafe { &*input_key.cast::<RLookupKey>() };

    // SAFETY: ensured by caller (2.)
    let field_names = unsafe { copy_c_names(field_names, field_names_len) };
    // SAFETY: ensured by caller (3.)
    let sort_key_names = unsafe { copy_c_names(sort_names, sort_names_len) };

    let limit = has_limit.then_some((limit_offset, limit_count));

    let mut cr = Box::new(LocalCollectReducer::new(
        input_key,
        field_names,
        load_all,
        sort_key_names,
        sort_asc_map,
        limit,
    ));

    cr.reducer_mut()
        .set_new_instance(collectLocalNewInstance)
        .set_add(collectLocalAdd)
        .set_finalize(collectLocalFinalize)
        .set_free_instance(collectLocalFreeInstance)
        .set_free(collectLocalFree);

    Box::into_raw(cr).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `LocalCollectReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectLocalNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<LocalCollectReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `CoordCollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectLocalFreeInstance(_r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.); `ctx` points to a valid, initialized
    // `LocalCollectCtx`. After this call the pointee is logically uninitialized,
    // but the arena memory is freed later when `LocalCollectReducer` (and its
    // `Bump`) is dropped.
    unsafe { ptr::drop_in_place(ctx.cast::<LocalCollectCtx>()) }
}

/// # Safety
///
/// 1. `r` must point to a [valid] `LocalCollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `CoordCollectCtx` masquerading as a void pointer.
/// 3. `srcrow` must point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectLocalAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<LocalCollectReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { ctx.cast::<LocalCollectCtx>().as_mut().unwrap() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { srcrow.cast::<RLookupRow>().as_ref().unwrap() };

    collect.add(r, srcrow);

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `r` must point to a [valid] `LocalCollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` must point to a [valid] `CoordCollectCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectLocalFinalize(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<LocalCollectReducer>().as_ref().unwrap() };
    // SAFETY: ensured by caller (2.)
    let collect = unsafe { ctx.cast::<LocalCollectCtx>().as_mut().unwrap() };

    collect.finalize(r).into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `r` must point to a [valid] `CoordCollectReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`CollectReducer_CreateLocal`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectLocalFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<LocalCollectReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<LocalCollectReducer>()) });
}
