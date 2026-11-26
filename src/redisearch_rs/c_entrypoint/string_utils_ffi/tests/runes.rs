/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The C rune entry points' buffer handling, which the pure
//! [`string_utils::runes`] tests cannot reach.

use std::{mem::MaybeUninit, ptr};

use string_utils::runes::MAX_RUNE_STR_LEN;
use string_utils_ffi::runes::{runesToStr, strToRunes, strToSingleCodepointFoldedRunes};

// Keep the Rust FFI exports needed by the linked C bundle from being stripped.
extern crate redisearch_rs;
// Mock or stub the C symbols the bundle needs but the line above does not provide.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Copy out a buffer an entry point returned, `len` items plus its
/// terminator, and free it with `RedisModule_Free` as the C caller would.
///
/// # Safety
///
/// 1. `ptr` must come from one of the entry points under test, which reported
///    `len` items, and must not be used afterwards.
unsafe fn take_buffer<T: Copy>(ptr: *mut T, len: usize) -> Vec<T> {
    // SAFETY: see safety point 1 above; the entry points write `len` items and
    // a terminator.
    let items = unsafe { std::slice::from_raw_parts(ptr, len + 1) }.to_vec();
    // SAFETY: `RedisModule_Free` is set by the mock before any test runs.
    let free = unsafe { redis_module::RedisModule_Free }.expect("Redis allocator not available");
    // SAFETY: see safety point 1 above; the buffer came from `RedisModule_Alloc`.
    unsafe { free(ptr.cast()) };
    items
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn runes_to_str_returns_a_nul_terminated_copy() {
    let mut len = usize::MAX;
    // SAFETY: the runes are a live array of the given length, and `len` is a
    // valid `size_t`.
    let encoded = unsafe { runesToStr([0x41, 0x7FF].as_ptr(), 2, &mut len) };
    assert_eq!(len, 3);
    // SAFETY: `encoded` was just returned with length `len`.
    let encoded = unsafe { take_buffer(encoded.cast::<u8>(), len) };
    assert_eq!(encoded, b"A\xDF\xBF\0");
}

#[test]
fn runes_to_str_rejects_too_many_runes() {
    let runes = vec![u16::from(b'a'); MAX_RUNE_STR_LEN + 1];
    let mut len = usize::MAX;
    // SAFETY: `runes` is a live array of the given length, and `len` is a
    // valid `size_t`.
    let encoded = unsafe { runesToStr(runes.as_ptr(), runes.len(), &mut len) };
    assert!(encoded.is_null());
    assert_eq!(len, 0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn folded_runes_are_zero_terminated_and_the_length_is_optional() {
    let input = b"AbC";
    // SAFETY: `input` is live for its length, and a NULL length is allowed.
    let folded = unsafe {
        strToSingleCodepointFoldedRunes(input.as_ptr().cast(), input.len(), ptr::null_mut())
    };
    // SAFETY: `folded` was just returned, holding one rune per input byte.
    let folded = unsafe { take_buffer(folded, input.len()) };
    assert_eq!(folded, b"abc\0".map(u16::from));
}

#[test]
fn str_to_runes_writes_into_uninitialized_storage_and_counts_past_it() {
    let input = b"abcd";
    let mut out = [MaybeUninit::<u16>::uninit(); 2];
    // SAFETY: `input` is live for its length, and `out` holds `out.len()`
    // writable runes apart from it.
    let count = unsafe {
        strToRunes(
            input.as_ptr().cast(),
            input.len(),
            out.as_mut_ptr().cast(),
            out.len(),
        )
    };
    assert_eq!(count, input.len());
    // SAFETY: `strToRunes` wrote every slot up to `outcap`.
    let out = out.map(|slot| unsafe { slot.assume_init() });
    assert_eq!(out, [u16::from(b'a'), u16::from(b'b')]);
}
