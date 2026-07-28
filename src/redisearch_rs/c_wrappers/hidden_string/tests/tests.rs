/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use hidden_string::{HiddenString, OwnedHiddenString};
use pretty_assertions::assert_eq;

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn owned_wrapper_borrows_buffer_and_frees_on_drop() {
    let bytes = c"Ab#123!";
    let hidden = OwnedHiddenString::new(bytes);

    // Deref to `HiddenString` exposes the same secret value as the backing buffer.
    assert_eq!(hidden.secret_value(), c"Ab#123!");

    // Dropping `hidden` frees the `ffi::HiddenString` wrapper (no manual
    // `HiddenString_Free`); the borrowed `bytes` buffer is left untouched.
    drop(hidden);
    assert_eq!(bytes, c"Ab#123!");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn get_secret_value() {
    let input = c"Ab#123!";
    let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
    let sut = unsafe { HiddenString::from_raw(ffi_hs) };

    assert_eq!(sut.secret_value(), input);

    unsafe { ffi::HiddenString_Free(ffi_hs, false) };
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn debug_output() {
    let input = c"Ab#123!";
    let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
    let hs = unsafe { HiddenString::from_raw(ffi_hs) };

    assert_eq!(format!("{hs:?}"), "HiddenString(****)");

    unsafe { ffi::HiddenString_Free(ffi_hs, false) };
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn pointer_output() {
    let input = c"Ab#123!";
    let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
    let hs = unsafe { HiddenString::from_raw(ffi_hs) };

    assert!(format!("{hs:p}").starts_with("0x"));

    unsafe { ffi::HiddenString_Free(ffi_hs, false) };
}
