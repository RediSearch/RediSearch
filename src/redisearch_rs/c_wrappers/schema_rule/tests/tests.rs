/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::undocumented_unsafe_blocks)]

extern crate redisearch_rs;

redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{mem, ptr};

use pretty_assertions::assert_eq;
use schema_rule::SchemaRule;

/// Create a C array from a fixed-size Rust array using the C `array_new_sz` function.
fn rs_array<const N: usize, T: Copy>(fields: [T; N]) -> *mut T {
    let arr = unsafe {
        let size_t_u16 = const { size_of::<T>() as u16 };
        let len_u32 = const { N as u32 };

        ffi::array_new_sz(size_t_u16, 0, len_u32).cast::<T>()
    };

    unsafe {
        let elements = std::slice::from_raw_parts_mut(arr, fields.len());
        elements.copy_from_slice(&fields);
    }

    arr
}

/// Test filter_fields and filter_fields_index together since their lengths are coupled.
#[test]
#[cfg_attr(miri, ignore = "miri does not support FFI functions")]
fn fields_and_indices() {
    let mut schema_rule = unsafe { mem::zeroed::<ffi::SchemaRule>() };

    schema_rule.filter_fields = rs_array([c"aaa", c"bbb"].map(|cstr| cstr.as_ptr().cast_mut()));
    schema_rule.filter_fields_index = rs_array([10, 20]);

    let sut = unsafe { SchemaRule::from_raw(ptr::from_ref(&schema_rule)) };

    let mut ff = sut.filter_fields();
    let ffi = sut.filter_fields_index();

    assert_eq!(ff.len(), 2);
    assert_eq!(ff.next().unwrap(), c"aaa");
    assert_eq!(ff.next().unwrap(), c"bbb");
    assert_eq!(ffi, [10, 20]);

    unsafe {
        ffi::array_free(schema_rule.filter_fields.cast());
        ffi::array_free(schema_rule.filter_fields_index.cast());
    }
}
