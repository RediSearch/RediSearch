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
    count::{CounterCtx, CounterReducer},
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
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CounterReducer>().as_mut().unwrap() };

    let ctx: *mut CounterCtx = r.alloc_instance();
    ctx.cast()
}

/// Frees a counter reducer instance
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CounterCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterFreeInstance(r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CounterReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let counter = unsafe { &mut *ctx.cast::<CounterCtx>() };

    counter.free(r);
}

/// Process the provided `RLookupRow` with the counter reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CounterCtx` masquerading as a void pointer.
/// 3. `srcrow` mut point to a [valid] `ffi::RLookupRow`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterAdd(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
    srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CounterReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let count = unsafe { &mut *ctx.cast::<CounterCtx>() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { &*srcrow.cast::<RLookupRow>() };

    count.add(r, srcrow);

    1
}

/// Finalize the counter reducer instance result into an `RSValue`.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `CounterCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterFinalize(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CounterReducer>().as_mut().unwrap() };
    // SAFETY: ensured by caller (2.)
    let count = unsafe { &*ctx.cast::<CounterCtx>() };

    count
        .finalize(r)
        .into_raw()
        .cast::<ffi::RSValue>()
        .cast_mut()
}

/// Constructor for the counter reducer.
///
/// # Safety
///
/// 1. `options` must point to a [valid] `ffi::ReducerOptions`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RDCRCount_New(options: *const ffi::ReducerOptions) -> *mut ffi::Reducer {
    // SAFETY: ensured by caller (1.)
    let options = unsafe { ReducerOptions::from_raw_mut(options.cast_mut()) };

    if let Some(mut counter_reducer) = CounterReducer::new(options) {
        counter_reducer
            .reducer_mut()
            .set_new_instance(counterNewInstance)
            .set_free_instance(counterFreeInstance)
            .set_add(counterAdd)
            .set_finalize(counterFinalize)
            .set_free(counterFree);

        Box::into_raw(Box::new(counter_reducer)).cast()
    } else {
        ptr::null_mut()
    }
}

/// Frees the provided counter reducer.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `CounterReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.)
    let _ = unsafe { Box::from_raw(r.cast::<CounterReducer>()) };
}
