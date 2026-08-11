/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`LoweredPattern`], the wildcard pattern both the
//! terms trie and the suffix trie walk with.
//!
//! The rune conversion these go through comes from the linked static library,
//! so the tests need the same C linkage as the trie tests.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::c_char;

use c_trie::LoweredPattern;

/// Convert an ASCII/UTF-8 string to the trie's rune (`u16`) key.
fn to_runes(s: &str) -> Vec<ffi::rune> {
    // A UTF-8 string decodes to at most as many runes as bytes; the extra slot
    // gives the conversion room for a trailing rune.
    let mut buf = vec![0 as ffi::rune; s.len() + 1];
    // SAFETY: `s` is valid UTF-8 of `s.len()` bytes, so the decode stays within
    // the slice, and `buf` has room for `s.len() + 1` runes.
    let n = unsafe { ffi::strToRunesN(s.as_ptr().cast::<c_char>(), s.len(), buf.as_mut_ptr()) };
    buf.truncate(n);
    buf
}

/// Build a wildcard pattern from an ASCII/UTF-8 string. Every pattern here is
/// lowercase already, so no case folding is needed on the way in.
fn pattern(s: &str) -> LoweredPattern {
    LoweredPattern::new(&to_runes(s)).expect("pattern is short enough to convert")
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_length_excludes_the_sentinel() {
    let p = pattern("he?l*o");
    assert_eq!(p.len(), 6);
    assert!(!p.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_from_no_runes_is_empty() {
    // The sentinel is still appended, but it does not count as content.
    let p = LoweredPattern::new(&[]).expect("an empty pattern converts");
    assert_eq!(p.len(), 0);
    assert!(p.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_length_is_in_runes_not_bytes() {
    // A multibyte pattern separates rune count from source-byte count, which
    // every ASCII pattern leaves equal. `len()` must count runes — it is what
    // the walks take as the pattern length.
    let p = pattern("hé*");
    assert_eq!(p.len(), 3, "three runes: h, é, *");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_declines_a_pattern_longer_than_max_rune_str_len() {
    // Term insertion declines anything over `MAX_RUNE_STR_LEN` runes, so such a
    // pattern can name no stored term and there is nothing to walk with.
    let runes = vec![ffi::rune::from(b'a'); ffi::MAX_RUNE_STR_LEN as usize + 1];
    assert!(LoweredPattern::new(&runes).is_none());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_declines_a_pattern_holding_a_zero_rune() {
    // An interior zero collides with the sentinel layout the type guarantees —
    // a consumer scanning for the zero would see only `a`. Declined rather than
    // built.
    assert!(LoweredPattern::new(&[ffi::rune::from(b'a'), 0, ffi::rune::from(b'b')]).is_none());
}
