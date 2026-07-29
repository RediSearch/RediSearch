/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Verify that [`string_utils::unicode::tolower`] and
//! [`string_utils::unicode::tolower_cow`] match the FFI `unicode_tolower_fn`
//! for all BMP codepoints, and that the `Cow` variant borrows when possible.

#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use std::ffi::c_void;

    /// Call C `unicode_tolower_fn` and return the lowercased string.
    fn c_unicode_tolower(s: &str) -> String {
        if s.is_empty() {
            return String::new();
        }
        let mut buf = s.as_bytes().to_vec();
        buf.push(0); // guard against off-by-one reads in C
        let mut len = s.len();
        // SAFETY: `buf` is a valid mutable buffer of at least `len` bytes.
        // `unicode_tolower_fn` either writes in-place (returns NULL) or
        // returns a new `rm_malloc`'d buffer. `len` is updated to the
        // output length.
        let ret = unsafe { ffi::unicode_tolower_fn(buf.as_mut_ptr().cast(), &mut len) };
        if ret.is_null() {
            // Result written in-place into `buf`.
            String::from_utf8(buf[..len].to_vec())
                .expect("C unicode_tolower in-place result must be valid UTF-8")
        } else {
            // Result in newly allocated buffer.
            // SAFETY: `ret` is a valid `rm_malloc`'d buffer of `len` bytes.
            let result = unsafe { std::slice::from_raw_parts(ret.cast::<u8>(), len) }.to_vec();
            // SAFETY: `RedisModule_Free` is a `static mut` populated during the
            // test's allocator setup and not mutated while the test runs, so
            // reading it here is sound.
            let rm_free = unsafe { ffi::RedisModule_Free.expect("Redis allocator not available") };
            // SAFETY: `ret` was allocated by `rm_malloc` (via `unicode_tolower_fn`).
            unsafe {
                rm_free(ret.cast::<c_void>());
            }
            String::from_utf8(result)
                .expect("C unicode_tolower allocated result must be valid UTF-8")
        }
    }

    /// Assert that both the owned [`string_utils::unicode::tolower`] and the
    /// borrowing [`string_utils::unicode::tolower_cow`] produce the same bytes
    /// as the C oracle. Sharing the oracle here means every case below — and the
    /// BMP proptest — exercises both Rust functions.
    fn assert_tolower_matches_c(s: &str) {
        let c_result = c_unicode_tolower(s);

        let rust_result = string_utils::unicode::tolower(s);
        assert_eq!(
            rust_result, c_result,
            "unicode_tolower mismatch for {s:?}: rust={rust_result:?}, c={c_result:?}",
        );

        let cow_result = string_utils::unicode::tolower_cow(s);
        assert_eq!(
            cow_result.as_ref(),
            c_result.as_str(),
            "unicode_tolower_cow mismatch for {s:?}: cow={cow_result:?}, c={c_result:?}",
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

    #[test]
    fn ffi_greek_sigma() {
        assert_tolower_matches_c("ΣΩΚΡΆΤΗΣ");
    }

    /// Generate BMP strings for exhaustive comparison.
    fn bmp_string() -> impl Strategy<Value = String> {
        proptest::collection::vec(
            (0x0001u32..=0xD7FFu32).prop_filter_map("valid BMP char", |cp| char::from_u32(cp)),
            1..100,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>())
    }

    proptest! {
        #[test]
        fn ffi_matches_rust(s in bmp_string()) {
            assert_tolower_matches_c(&s);
        }
    }
}

/// The `Cow` variant must borrow when nothing changes and allocate only when a
/// fold is required — the optimization the equivalence checks can't observe.
mod cow_borrow_contract {
    use std::borrow::Cow;
    use string_utils::unicode;

    #[test]
    fn borrows_when_already_lowercase() {
        assert!(matches!(unicode::tolower_cow(""), Cow::Borrowed(_)));
        assert!(matches!(unicode::tolower_cow("hello"), Cow::Borrowed(_)));
        // ß is already lowercase; the non-ASCII path must borrow it too.
        assert!(matches!(unicode::tolower_cow("straße"), Cow::Borrowed(_)));
    }

    #[test]
    fn owns_when_fold_changes_input() {
        assert!(matches!(unicode::tolower_cow("Hello"), Cow::Owned(_)));
        assert!(matches!(unicode::tolower_cow("Σ"), Cow::Owned(_)));
    }
}
