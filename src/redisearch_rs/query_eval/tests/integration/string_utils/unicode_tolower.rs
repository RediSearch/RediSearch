/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Verify that Rust's [`str::to_lowercase`] matches the FFI
//! `unicode_tolower_fn` for BMP codepoints where the two Unicode
//! versions agree.

#[cfg(not(miri))]
mod ffi_comparison {
    use super::super::LIBNU_DIVERGENT;
    use proptest::prelude::*;
    use std::alloc::{Layout, dealloc};

    /// Call `unicode_tolower_fn` via FFI and return the lowercased string.
    fn c_unicode_tolower(s: &str) -> String {
        if s.is_empty() {
            return String::new();
        }
        let mut buf = s.as_bytes().to_vec();
        buf.push(0); // null terminator for safety
        let mut len = s.len();
        // SAFETY: `buf` is a valid mutable buffer of at least `len` bytes.
        // `unicode_tolower_fn` either writes in-place (returns NULL) or
        // returns a new buffer via the global allocator. `len` is updated
        // to the output length.
        let ret = unsafe { ffi::unicode_tolower_fn(buf.as_mut_ptr().cast(), &mut len) };
        if ret.is_null() {
            // Result written in-place into `buf`.
            String::from_utf8(buf[..len].to_vec())
                .expect("unicode_tolower in-place result must be valid UTF-8")
        } else {
            // Result in newly allocated buffer.
            // SAFETY: `ret` is a valid pointer to `len` bytes allocated
            // via `std::alloc::alloc`.
            let result = unsafe { std::slice::from_raw_parts(ret.cast::<u8>(), len) }.to_vec();
            // SAFETY: `ret` was allocated with `Layout::from_size_align_unchecked(len + 1, 1)`
            // by `unicode_tolower_fn`.
            unsafe {
                dealloc(ret.cast(), Layout::from_size_align_unchecked(len + 1, 1));
            }
            String::from_utf8(result).expect("unicode_tolower allocated result must be valid UTF-8")
        }
    }

    fn assert_tolower_matches_c(s: &str) {
        let c_result = c_unicode_tolower(s);
        let rust_result = s.to_lowercase();
        assert_eq!(
            rust_result, c_result,
            "mismatch for input {:?}: rust={:?}, c={:?}",
            s, rust_result, c_result,
        );
    }

    #[test]
    fn ffi_ascii() {
        assert_tolower_matches_c("HELLO");
    }

    #[test]
    fn ffi_empty() {
        assert_tolower_matches_c("");
    }

    #[test]
    fn ffi_already_lower() {
        assert_tolower_matches_c("hello");
    }

    #[test]
    fn ffi_mixed() {
        assert_tolower_matches_c("Hello World 123!");
    }

    #[test]
    fn ffi_unicode() {
        assert_tolower_matches_c("Straße");
    }

    /// Generate BMP strings excluding codepoints where libnu and Rust
    /// disagree on lowercasing.
    fn bmp_tolower_safe() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            (0x0001u32..=0xD7FFu32).prop_filter_map("valid non-divergent BMP char", |cp| {
                if LIBNU_DIVERGENT.contains(&cp) {
                    return None;
                }
                char::from_u32(cp)
            }),
            1..100,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #[test]
        fn ffi_matches_rust(s in bmp_tolower_safe()) {
            assert_tolower_matches_c(&s);
        }
    }
}
