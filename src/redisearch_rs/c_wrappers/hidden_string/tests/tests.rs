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
fn secret_value_truncates_at_an_interior_nul() {
    // A name holding a NUL of its own. The trailing NUL is the terminator
    // every hidden string carries; `len` excludes it, as the C side reports it.
    let input: Vec<u8> = b"v\0hidden\0".to_vec();
    let len = input.len() - 1;
    // SAFETY: `input` is a live allocation of `len + 1` bytes terminated by a
    // NUL, and is neither moved nor mutated while borrowed below.
    // `takeOwnership = false`, so the wrapper borrows it rather than adopting
    // it — `input` stays the owner and frees it on drop.
    let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr().cast(), len, false) };
    // SAFETY: `ffi_hs` is a valid `HiddenString` just returned by
    // `NewHiddenString`, and `input` outlives every borrow taken from it.
    let sut = unsafe { HiddenString::from_raw(ffi_hs) };

    assert_eq!(sut.secret_value(), c"v");

    // SAFETY: `ffi_hs` has not been freed yet and no borrow of it is live.
    // The `false` matches the `takeOwnership` passed to `NewHiddenString`, so
    // this frees only the wrapper and leaves `input`'s buffer to `input`.
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
