/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The case mappings behind [`lower_runes`] and [`fold`], checked on every
//! codepoint against the vendored libnu tables the C tokenizer and fuzzy
//! matcher still use. Terms lowercased by either side must meet in the trie,
//! so a toolchain whose Unicode version differs from libnu's fails here.

// These tests call libnu, so they cannot run under miri.
#![cfg(not(miri))]

use std::ffi::{CStr, c_char};

use string_utils::{
    libnu::{nu_codepoints, nu_utf8_encode},
    runes::{fold, lower_runes},
};

unsafe extern "C" {
    fn nu_tolower(codepoint: u32) -> *const c_char;
    fn nu_tofold(codepoint: u32) -> *const c_char;
}

/// Every value a four-byte sequence can decode to.
const CODEPOINTS: std::ops::Range<u32> = 1..0x20_0000;

/// The codepoints libnu maps `codepoint` to, or `codepoint` alone when it has no
/// mapping.
fn libnu_mapping(map: unsafe extern "C" fn(u32) -> *const c_char, codepoint: u32) -> Vec<u32> {
    // SAFETY: the lookup only reads libnu's static tables.
    let mapped = unsafe { map(codepoint) };
    if mapped.is_null() {
        return vec![codepoint];
    }
    // SAFETY: a non-null mapping is a NUL-terminated UTF-8 string in those tables.
    let bytes = unsafe { CStr::from_ptr(mapped) }.to_bytes();
    nu_codepoints(bytes).collect()
}

#[test]
fn fold_matches_libnu_on_every_codepoint() {
    for codepoint in CODEPOINTS {
        assert_eq!(
            fold(codepoint),
            libnu_mapping(nu_tofold, codepoint)[0],
            "{codepoint:#x}"
        );
    }
}

#[test]
fn lowercasing_matches_libnu_on_every_codepoint() {
    let mut encoded = Vec::new();
    for codepoint in CODEPOINTS {
        encoded.clear();
        nu_utf8_encode(codepoint, &mut encoded);
        let expected: Vec<u16> = libnu_mapping(nu_tolower, codepoint)
            .into_iter()
            .map(|c| c as u16)
            .collect();
        assert_eq!(lower_runes(&encoded).unwrap(), expected, "{codepoint:#x}");
    }
}
