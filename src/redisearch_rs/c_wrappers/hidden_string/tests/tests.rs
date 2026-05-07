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

use hidden_string::HiddenStringRef;
use pretty_assertions::assert_eq;

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn get_secret_value() {
    let input = c"Ab#123!";
    let ffi_hs = unsafe { ffi::NewHiddenString(input.as_ptr(), input.count_bytes(), false) };
    let sut = unsafe { HiddenStringRef::from_raw(ffi_hs) };

    let actual = sut.into_secret_value();

    assert_eq!(actual, input);

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
    let hs = unsafe { HiddenStringRef::from_raw(ffi_hs) };

    let actual = format!("{hs:?}");

    assert_eq!(actual, "HiddenStringRef(****)");

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
    let hs = unsafe { HiddenStringRef::from_raw(ffi_hs) };

    let actual = format!("{hs:p}");

    assert!(actual.starts_with("0x"));

    unsafe { ffi::HiddenString_Free(ffi_hs, false) };
}
