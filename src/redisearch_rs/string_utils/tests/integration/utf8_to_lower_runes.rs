/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use string_utils::runes::{MAX_RUNE_STR_LEN, runes_to_utf8, utf8_to_lower_runes};

#[test]
fn ascii_lowercases() {
    let expected: Vec<u16> = "hello".encode_utf16().collect();
    assert_eq!(utf8_to_lower_runes(b"HeLLo").unwrap(), expected);
}

#[test]
fn empty_yields_empty() {
    assert_eq!(utf8_to_lower_runes(b"").unwrap(), Vec::<u16>::new());
}

#[test]
fn bmp_non_ascii_lowercases() {
    // `Ä` (U+00C4) folds to `ä` (U+00E4), `Ω` (U+03A9) to `ω` (U+03C9).
    assert_eq!(
        utf8_to_lower_runes("ÄΩ".as_bytes()).unwrap(),
        vec![0x00E4, 0x03C9]
    );
}

#[test]
fn malformed_sequence_decodes_leniently() {
    // `[0xC3, b'(']` is not valid UTF-8, but the trie's keys were built with a
    // decoder that masks continuation bytes unconditionally: it yields
    // (0xC3 & 0x1F) << 6 | (0x28 & 0x3F) = 0x00E8, which is already lowercase.
    // A validating decode would substitute U+FFFD here and look up a key that
    // was never stored.
    assert_eq!(utf8_to_lower_runes(&[0xC3, b'(']).unwrap(), vec![0x00E8]);
}

#[test]
fn surrogate_bytes_are_preserved() {
    // WTF-8 bytes for a lone surrogate, as emitted for a non-BMP codepoint that
    // was truncated to a rune at index time. The surrogate has no case mapping
    // and must survive the fold to resolve the key it was stored under.
    let bytes = runes_to_utf8(&[0xD800]).unwrap();
    assert_eq!(utf8_to_lower_runes(&bytes).unwrap(), vec![0xD800]);
}

#[test]
fn out_of_range_lead_byte_is_consumed() {
    // A `0xF8..=0xFF` lead is a 4-byte start for this decoder, not a malformed
    // byte: 0xF8 0x81 0x82 0x83 -> (0x81 & 0x3F) << 12 | (0x82 & 0x3F) << 6
    // | (0x83 & 0x3F) = 0x1083, which is already lowercase.
    assert_eq!(
        utf8_to_lower_runes(&[0xF8, 0x81, 0x82, 0x83]).unwrap(),
        vec![0x1083]
    );
}

#[test]
fn folds_before_truncating_to_a_rune() {
    // `𐐀` (U+10400, DESERET CAPITAL LETTER LONG I) folds to U+10428, whose low
    // 16 bits are 0x0428. Truncating to a rune first would instead fold U+0400
    // to U+0450 and yield 0x0450 — a different key.
    assert_eq!(utf8_to_lower_runes("𐐀".as_bytes()).unwrap(), vec![0x0428]);
}

#[test]
fn stops_at_first_nul_codepoint() {
    // A term is stored under the runes up to its first NUL codepoint, so the key
    // stops there too; the `B` after the literal NUL is not part of the key.
    assert_eq!(utf8_to_lower_runes(b"a\0B").unwrap(), vec![u16::from(b'a')]);
}

#[test]
fn stops_at_decoded_nul_before_end() {
    // The stop is on the decoded codepoint, so a sequence that decodes to `0`
    // without being a literal NUL byte — the overlong encoding `[0xC0, 0x80]` —
    // ends the key just the same, and the trailing bytes are not decoded.
    assert_eq!(
        utf8_to_lower_runes(&[b'a', 0xC0, 0x80, b'b', b'c']).unwrap(),
        vec![u16::from(b'a')]
    );
}

#[test]
fn truncated_trailing_sequence_zero_fills() {
    // A lead byte whose continuation bytes run past the end still yields a rune,
    // its missing bytes taken as zero — the terminator byte the C decoder reads
    // there. Dropping the sequence would leave an empty key, which as a prefix
    // matches every term instead of the one it names.
    // `[0xE2, 0x82]` -> (0xE2 & 0x0F) << 12 | (0x82 & 0x3F) << 6 = 0x2080, which
    // has no case mapping.
    assert_eq!(
        utf8_to_lower_runes(&[b'a', 0xE2, 0x82]).unwrap(),
        vec![u16::from(b'a'), 0x2080]
    );
    // `[0xC3]` -> (0xC3 & 0x1F) << 6 = 0x00C0 (`À`), which folds to 0x00E0 (`à`).
    assert_eq!(utf8_to_lower_runes(&[0xC3]).unwrap(), vec![0x00E0]);
}

#[test]
fn at_and_over_the_maximum_length() {
    let at_max = vec![b'a'; MAX_RUNE_STR_LEN];
    assert_eq!(
        utf8_to_lower_runes(&at_max).unwrap().len(),
        MAX_RUNE_STR_LEN
    );

    let over_max = vec![b'a'; MAX_RUNE_STR_LEN + 1];
    let err = utf8_to_lower_runes(&over_max).expect_err("exceeds the maximum rune length");
    assert_eq!(err.len, MAX_RUNE_STR_LEN + 1);
}

#[test]
fn length_is_measured_after_folding() {
    // `İ` (U+0130) folds to two codepoints, so a string that fits the limit
    // before folding can exceed it after.
    let input = "İ".repeat(MAX_RUNE_STR_LEN / 2 + 1);
    let err = utf8_to_lower_runes(input.as_bytes()).expect_err("folded form exceeds the maximum");
    assert!(err.len > MAX_RUNE_STR_LEN);
}

/// Compare against the real C `strToLowerRunes`, which is the routine this
/// reimplements, over binary-safe input including sequences a validating UTF-8
/// decoder would reject.
#[cfg(not(miri))]
mod ffi_comparison {
    use proptest::prelude::*;
    use std::ffi::c_void;
    use string_utils::runes::utf8_to_lower_runes;

    /// Call C `strToLowerRunes` via FFI and return the resulting rune slice, or
    /// `None` when it returns NULL (the folded form exceeds `MAX_RUNE_STR_LEN`).
    ///
    /// The token is handed over NUL-terminated with three spare terminator
    /// bytes, exactly as the query layer stores it. C classifies a sequence's
    /// width from its lead byte and reads that many bytes without
    /// bounds-checking, so a truncated trailing sequence reads terminators; the
    /// spares keep even a four-byte lead inside the allocation while `len` still
    /// describes the token itself. That is what [`utf8_to_lower_runes`]
    /// reproduces by taking a missing continuation byte as zero, so truncated
    /// input can be compared directly rather than padded away.
    ///
    /// One constraint remains on `bytes`: no sequence in it may decode to
    /// codepoint `0`. This conversion stops at the first such codepoint, matching
    /// how a term is indexed, while the C routine in this branch's base does not
    /// yet; a separate change reconciles them. It is a property of the *decoded*
    /// text, not of the bytes: `[0xA0, 0x40]` holds no NUL byte and still decodes
    /// to `0`. Until that change is in the base, such input is excluded from the
    /// comparison; afterwards the two agree and the exclusion should go away.
    fn c_str_to_lower_runes(bytes: &[u8]) -> Option<Vec<u16>> {
        let mut unicode_len: usize = 0;
        let mut buf = bytes.to_vec();
        buf.extend_from_slice(&[0, 0, 0]);
        // SAFETY: `buf` holds `bytes` followed by three NUL bytes, so a sequence
        // starting anywhere within the first `bytes.len()` bytes — at most four
        // bytes wide — stays inside the allocation. `strToLowerRunes` returns a
        // freshly `rm_calloc`'d array of `unicode_len` runes, or NULL.
        let ptr =
            unsafe { ffi::strToLowerRunes(buf.as_ptr().cast(), bytes.len(), &mut unicode_len) };
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is a valid `rm_calloc`'d array of `unicode_len` runes.
        let runes = unsafe { std::slice::from_raw_parts(ptr, unicode_len) }.to_vec();
        // SAFETY: `RedisModule_Free` is a `static mut` set by the test harness and
        // not mutated concurrently.
        let rm_free = unsafe { ffi::RedisModule_Free.expect("Redis allocator not available") };
        // SAFETY: `ptr` was allocated by `rm_calloc` (backed by RedisModule_Alloc).
        unsafe { rm_free(ptr.cast::<c_void>()) };
        Some(runes)
    }

    /// Whether `bytes` decodes to a `0` codepoint anywhere — the input excluded
    /// from the oracle comparison, see [`c_str_to_lower_runes`].
    ///
    /// Detected with an independent lenient decode (the same lead-byte ranges the
    /// conversion uses), because [`utf8_to_lower_runes`] now stops at the first
    /// such codepoint and so no longer surfaces it. The test is on the decoded
    /// codepoint, before truncation: a `0` *rune* produced by truncating a
    /// non-zero codepoint (`U+10000`) is not a `0` *codepoint*, and the C routine
    /// is fine with it. Delete this together with the exclusions once the base
    /// carries the reconciling change.
    fn decodes_a_nul(bytes: &[u8]) -> bool {
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            let width = if b < 0x80 {
                1
            } else if b < 0xE0 {
                2
            } else if b < 0xF0 {
                3
            } else {
                4
            };
            // A continuation byte past the end is taken as zero, matching the
            // conversion and the terminator the C decoder reads there.
            let cont = |k: usize| u32::from(bytes.get(i + k).copied().unwrap_or(0)) & 0x3F;
            let cp = match width {
                1 => u32::from(b),
                2 => (u32::from(b) & 0x1F) << 6 | cont(1),
                3 => (u32::from(b) & 0x0F) << 12 | cont(1) << 6 | cont(2),
                _ => (u32::from(b) & 0x07) << 18 | cont(1) << 12 | cont(2) << 6 | cont(3),
            };
            if cp == 0 {
                return true;
            }
            i += width;
        }
        false
    }

    fn assert_matches_c(bytes: &[u8]) {
        assert!(
            !decodes_a_nul(bytes),
            "input decoding to a NUL codepoint is excluded from the oracle comparison",
        );
        let c_runes =
            c_str_to_lower_runes(bytes).expect("FFI strToLowerRunes returned NULL unexpectedly");
        let rust_runes = utf8_to_lower_runes(bytes).expect("within the maximum rune length");
        assert_eq!(
            rust_runes, c_runes,
            "mismatch for bytes {bytes:?}: rust={rust_runes:?}, c={c_runes:?}",
        );
    }

    #[test]
    fn ffi_ascii() {
        assert_matches_c(b"ABC");
    }

    #[test]
    fn ffi_empty() {
        assert_matches_c(b"");
    }

    #[test]
    fn ffi_bmp_unicode() {
        assert_matches_c("Héllo Wörld".as_bytes());
    }

    #[test]
    fn ffi_malformed_sequence() {
        assert_matches_c(&[0xC3, b'(']);
    }

    #[test]
    fn ffi_surrogate_bytes() {
        assert_matches_c(&[0xED, 0xA0, 0x80]);
    }

    #[test]
    fn ffi_non_bmp() {
        assert_matches_c("𐐀😀".as_bytes());
    }

    #[test]
    fn ffi_truncated_trailing_sequence() {
        // The token ends on a lead byte whose continuation is missing; C reads
        // the terminator there, and the conversion supplies the same zero.
        assert_matches_c(&[0xC3]);
        assert_matches_c(&[b'a', b'b', 0xC3]);
        assert_matches_c(&[b'a', 0xE2, 0x82]);
    }

    proptest! {
        /// Arbitrary binary input — malformed and truncated sequences included —
        /// folds to the same runes as the C conversion. Input decoding to a `0`
        /// codepoint is excluded (see [`c_str_to_lower_runes`]); the NUL byte
        /// itself is kept out of the generator so the rejection stays rare rather
        /// than a quarter of all cases. Once the base carries the reconciling
        /// change, drop the `prop_assume!` and cover them.
        #[test]
        fn matches_c(bytes in proptest::collection::vec(1u8..=u8::MAX, 0..64)) {
            prop_assume!(!decodes_a_nul(&bytes));
            assert_matches_c(&bytes);
        }
    }
}
