/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! COUNT reducer FFI.

use reducers::count::{CountCtx, CountReducer};
use std::{
    ffi::{c_int, c_void},
    ptr,
};

/// Create a COUNT reducer; free it with [`countFree`].
#[unsafe(no_mangle)]
pub extern "C" fn CountReducer_Create() -> *mut ffi::Reducer {
    let mut r = Box::new(CountReducer::new());

    r.reducer_mut()
        .set_new_instance(countNewInstance)
        .set_add(countAdd)
        .set_finalize(countFinalize)
        .set_free(countFree);

    Box::into_raw(r).cast()
}

/// # Safety
///
/// 1. `r` must point to a [valid] `CountReducer` masquerading as a `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn countNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { r.cast::<CountReducer>().as_mut().unwrap() };

    ptr::from_mut(r.alloc_instance()).cast::<c_void>()
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `CountCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn countAdd(
    _r: *mut ffi::Reducer,
    ctx: *mut c_void,
    _srcrow: *const ffi::RLookupRow,
) -> c_int {
    // SAFETY: ensured by caller (1.)
    let count = unsafe { ctx.cast::<CountCtx>().as_mut().unwrap() };

    count.add();

    1 // C reducer->Add convention: always returns 1
}

/// # Safety
///
/// 1. `ctx` must point to a [valid] `CountCtx` masquerading as a void pointer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn countFinalize(
    _r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let count = unsafe { ctx.cast::<CountCtx>().as_ref().unwrap() };

    count.finalize().into_raw() as *mut ffi::RSValue
}

/// # Safety
///
/// 1. `r` must point to a [valid] `CountReducer` masquerading as a `ffi::Reducer`,
///    originally created by [`CountReducer_Create`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn countFree(r: *mut ffi::Reducer) {
    // SAFETY: ensured by caller (1.); `r` originates from `Box::into_raw` of a
    // `Box<CountReducer>` and is still owned by C, so we can reclaim it here.
    drop(unsafe { Box::from_raw(r.cast::<CountReducer>()) });
}
