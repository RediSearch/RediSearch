/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::{
    ReducerOptions,
    collect::{CollectCtx, CollectReducer},
};
use rlookup::RLookupRow;
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Creates a new counter reducer instance
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

/// Frees a counter reducer instance
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CollectCtx` masquerading as a void pointer.
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

/// Process the provided `RLookupRow` with the collect reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CollectCtx` masquerading as a void pointer.
/// 3. `srcrow` mut point to a [valid] `ffi::RLookupRow`.
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
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { &*srcrow.cast::<RLookupRow>() };

    collect.add(r, srcrow);

    1
}

/// Finalize the collect reducer instance result into an `RSValue`.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CollectReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CollectCtx` masquerading as a void pointer.
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

/// Frees the provided counter reducer.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn collectFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.)
    let _ = unsafe { Box::from_raw(r.cast::<CollectReducer>()) };
}
