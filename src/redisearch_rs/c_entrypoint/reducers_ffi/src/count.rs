/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::{Reducer, count::Counter};
use rlookup::RLookupRow;
use std::ffi::{c_int, c_void};

/// Creates a new counter reducer instance
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ffi::Reducer`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterNewInstance(r: *mut ffi::Reducer) -> *mut c_void {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { Reducer::from_raw(r) };

    Box::into_raw(Box::new(Counter::new(r))).cast()
}

/// Frees a counter reducer instance
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `Counter`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterFreeInstance(r: *mut ffi::Reducer, ctx: *mut c_void) {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { Reducer::from_raw(r) };
    // SAFETY: ensured by caller (2.)
    let counter = unsafe { Box::from_raw(ctx.cast::<Counter>()) };

    counter.free(r);
}

/// Process the provided `RLookupRow` with the counter reducer instance.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `Counter`.
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
    let r = unsafe { Reducer::from_raw(r) };
    // SAFETY: ensured by caller (2.)
    let count = unsafe { &mut *ctx.cast::<Counter>() };
    // SAFETY: ensured by caller (3.)
    let srcrow = unsafe { &*srcrow.cast::<RLookupRow>() };

    count.add(r, srcrow);

    1
}

/// Finalize the counter reducer instance result into an `RSValue`.
///
/// # Safety
///
/// 1. `r` must point to a [valid] `ffi::Reducer`.
/// 2. `ctx` mut point to a [valid] `Counter`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn counterFinalize(
    r: *mut ffi::Reducer,
    ctx: *mut c_void,
) -> *mut ffi::RSValue {
    // SAFETY: ensured by caller (1.)
    let r = unsafe { Reducer::from_raw(r) };
    // SAFETY: ensured by caller (2.)
    let count = unsafe { &*ctx.cast::<Counter>() };

    count.finalize(r).into_raw()
}
