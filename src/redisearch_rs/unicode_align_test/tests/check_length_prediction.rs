/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Self-consistency sweeps for libnu's length-prediction APIs.
//!
//! # Why
//!
//! RediSearch's production text path relies on libnu predicting exact
//! lengths before allocating an output buffer:
//!
//! - `unicode_tolower()` at `src/util/strconv.h:121` calls
//!   `nu_strtransformnlen(s, in_len, nu_utf8_read, nu_tolower, nu_casemap_read)`
//!   to size the codepoint buffer, then runs the manual decode-and-lower
//!   loop into that buffer. The two values are debug-asserted equal at
//!   `src/util/strconv.h:172`:
//!
//!   ```c
//!   RS_LOG_ASSERT_FMT(i == u_len, "i (%u) should be equal to u_len (%zd)", i, u_len);
//!   ```
//!
//!   If they diverge the assertion misses in release builds and the
//!   buffer is the wrong size — a heap overflow or under-fill.
//!
//! - Same site then calls `nu_bytenlen(u_buffer, i, nu_utf8_write)` to size
//!   the *re-encode* buffer and writes via `nu_writenstr`. A mismatch
//!   between the prediction and the actual encode emits past the buffer
//!   or leaves it short.
//!
//! - `runesToStr()` at `src/trie/rune_util.c:54` uses `nu_bytelen` for the
//!   same purpose against a NUL-terminated codepoint array.
//!
//! - `strToRunes()` / `strToSingleCodepointFoldedRunes()` use `nu_strlen`
//!   to size the decode buffer.
//!
//! Promoting the runtime debug-assert to a deterministic sweep catches a
//! prediction bug on every codepoint, in release builds, before it ever
//! reaches user data. The sweep cost is small (~1M codepoints, parallel).
//!
//! # What is tested
//!
//! For every Unicode scalar `cp ∈ (0..=0x10FFFF) \ surrogates \ {NUL}`,
//! exercising both the single-codepoint and the embedded-in-string shape:
//!
//! 1. **`nu_strtransformnlen` (lowercase)** vs the manual transform loop
//!    (`nu_utf8_read` → `nu_tolower` → `nu_casemap_read`). This is the
//!    invariant the production `RS_LOG_ASSERT_FMT` checks.
//! 2. **`nu_strlen`** vs Rust's `char` count, stopping at NUL.
//! 3. **`nu_strnlen`** vs the same count, but bounded by byte length
//!    without requiring a NUL terminator.
//! 4. **`nu_bytelen` / `nu_bytenlen`** vs the actual byte count produced
//!    by `nu_writenstr`. Same shape as the `nu_strtransformnlen` check on
//!    the encode side.
//!
//! NUL (U+0000) is excluded because every length predictor and the
//! production loops treat 0 as a string/buffer terminator — the API has
//! no way to represent "I literally want to count NUL." This matches the
//! `unicode_tolower` precondition (`if (*inout_len == 0) return NULL;`).

use unicode_align_test::checks::{
    actual_encoded_byte_count, lower_libnu, predict_bytelen, predict_bytenlen, predict_lower_len,
    predict_strlen, predict_strnlen, write_with_libnu,
};
use rayon::prelude::*;

/// Iterate every Unicode scalar except surrogates and NUL.
///
/// NUL is excluded for the reason described in the module docstring:
/// every libnu length predictor stops at the first 0, so feeding it a
/// solitary NUL would observe terminator behaviour, not prediction
/// behaviour.
fn iter_scalars() -> impl ParallelIterator<Item = u32> {
    (1u32..=0x10FFFF)
        .into_par_iter()
        .filter(|cp| !(0xD800..=0xDFFF).contains(cp))
        .filter(|cp| char::from_u32(*cp).is_some())
}

/// Compute the lowercase codepoint count the way the production loop does.
///
/// `lower_libnu` walks the input one codepoint at a time, calling
/// `nu_tolower` and then iterating the mapping (the same shape as
/// `while (1) { map = nu_casemap_read(map, &mu); ... }` at
/// `src/util/strconv.h:160` and `src/trie/rune_util.c:97`). The
/// codepoint count of its output is exactly what `nu_strtransformnlen`
/// is supposed to predict.
fn manual_lower_len(s: &str) -> usize {
    lower_libnu(s).chars().count()
}

#[test]
fn nu_strtransformnlen_matches_manual_loop_single_codepoint() {
    let offenders: Vec<String> = iter_scalars()
        .filter_map(|cp| {
            let ch = char::from_u32(cp).expect("scalar was pre-filtered");
            let mut buf = [0u8; 4];
            let s = ch.encode_utf8(&mut buf);
            let predicted = predict_lower_len(s.as_bytes());
            let actual = manual_lower_len(s);
            (predicted != actual as isize)
                .then(|| format!("U+{cp:04X}: predicted={predicted} actual={actual}"))
        })
        .collect();

    report_offenders("nu_strtransformnlen vs manual loop (single cp)", &offenders);
    assert!(
        offenders.is_empty(),
        "nu_strtransformnlen disagreed with the manual decode-and-lower loop \
         for {} codepoint(s); the RS_LOG_ASSERT_FMT at src/util/strconv.h:172 \
         would fire in debug builds and the lowercase buffer would be the wrong \
         size in release builds",
        offenders.len(),
    );
}

#[test]
fn nu_strtransformnlen_matches_manual_loop_strings() {
    // A handful of multi-codepoint inputs that mix passthrough, single-cp
    // mappings, and multi-cp expansions (ß → ss, ﬃ → ffi).
    let cases: &[&str] = &[
        "",
        "ABC",
        "abc",
        "Hello, World!",
        "ß",
        "Straße",
        "ﬃ",
        "ﬁnal",
        "Σ",
        "ΣΣΣ",
        "İstanbul",
        "Καλημέρα",
        "АБВ",
        "𐐀𐐁𐐂",
        "🦀 crab",
    ];
    let mut offenders = Vec::new();
    for &input in cases {
        let predicted = predict_lower_len(input.as_bytes());
        let actual = manual_lower_len(input);
        if predicted != actual as isize {
            offenders.push(format!("{input:?}: predicted={predicted} actual={actual}",));
        }
    }
    report_offenders("nu_strtransformnlen vs manual loop (strings)", &offenders);
    assert!(offenders.is_empty(), "see report above");
}

#[test]
fn nu_strlen_matches_char_count() {
    let offenders: Vec<String> = iter_scalars()
        .filter_map(|cp| {
            let ch = char::from_u32(cp).expect("scalar was pre-filtered");
            let mut nul_terminated = ch.to_string().into_bytes();
            nul_terminated.push(0);
            let predicted = predict_strlen(&nul_terminated);
            let actual = 1isize; // single-codepoint input, by construction
            (predicted != actual)
                .then(|| format!("U+{cp:04X}: predicted={predicted} actual={actual}"))
        })
        .collect();
    report_offenders("nu_strlen vs Rust char count (single cp)", &offenders);
    assert!(offenders.is_empty(), "see report above");
}

#[test]
fn nu_strnlen_matches_char_count() {
    let offenders: Vec<String> = iter_scalars()
        .filter_map(|cp| {
            let ch = char::from_u32(cp).expect("scalar was pre-filtered");
            let mut buf = [0u8; 4];
            let s = ch.encode_utf8(&mut buf);
            let predicted = predict_strnlen(s.as_bytes());
            let actual = 1isize;
            (predicted != actual)
                .then(|| format!("U+{cp:04X}: predicted={predicted} actual={actual}"))
        })
        .collect();
    report_offenders("nu_strnlen vs Rust char count (single cp)", &offenders);
    assert!(offenders.is_empty(), "see report above");
}

#[test]
fn nu_bytelen_matches_actual_encoded_bytes() {
    let offenders: Vec<String> = iter_scalars()
        .filter_map(|cp| {
            // Build a NUL-terminated 2-codepoint array `[cp, 0]`. `nu_bytelen`
            // walks until it hits 0, so it sees exactly `cp`.
            let unicode = [cp, 0];
            let predicted = predict_bytelen(&unicode);
            let actual = actual_encoded_byte_count(&unicode);
            (predicted != actual as isize)
                .then(|| format!("U+{cp:04X}: predicted={predicted} actual={actual}"))
        })
        .collect();
    report_offenders("nu_bytelen vs sum of nu_utf8_write byte counts", &offenders);
    assert!(
        offenders.is_empty(),
        "nu_bytelen disagreed with the actual nu_utf8_write byte count for {} \
         codepoint(s); a re-encode buffer sized via nu_bytelen would be too \
         large (waste) or too small (heap overflow) for these codepoints",
        offenders.len(),
    );
}

#[test]
fn nu_bytenlen_matches_actual_encoded_bytes() {
    let offenders: Vec<String> = iter_scalars()
        .filter_map(|cp| {
            let unicode = [cp];
            let predicted = predict_bytenlen(&unicode);
            let actual = actual_encoded_byte_count(&unicode);
            (predicted != actual as isize)
                .then(|| format!("U+{cp:04X}: predicted={predicted} actual={actual}"))
        })
        .collect();
    report_offenders(
        "nu_bytenlen vs sum of nu_utf8_write byte counts",
        &offenders,
    );
    assert!(offenders.is_empty(), "see report above");
}

#[test]
fn nu_writenstr_writes_exactly_predicted_bytes_on_multi_codepoint_arrays() {
    // Exercise the multi-codepoint path: arrays mixing 1/2/3/4-byte UTF-8
    // codepoints, plus boundary cases. The assertion is that `nu_writenstr`
    // emits exactly as many bytes as the independently-computed
    // `actual_encoded_byte_count`, and that `nu_bytenlen` predicts the same
    // value — the three-way agreement is the production safety property.
    let cases: &[&[u32]] = &[
        &[],
        &[b'a' as u32],
        &[b'a' as u32, b'b' as u32, b'c' as u32],
        &[0x00E9, 0x00DF, 0x2603, 0x1F980], // é ß ☃ 🦀
        &[0x1F980, 0x2603, 0x00DF, 0x00E9], // reverse order
        &[0x10FFFF],
    ];
    let mut offenders = Vec::new();
    for case in cases {
        let predicted = predict_bytenlen(case);
        let actual = actual_encoded_byte_count(case);
        let written = write_with_libnu(case).len();
        if predicted != actual as isize || written != actual {
            offenders.push(format!(
                "{case:X?}: predicted={predicted} actual={actual} written={written}",
            ));
        }
    }
    assert!(
        offenders.is_empty(),
        "nu_bytenlen / nu_writenstr / per-cp sum disagreed on multi-codepoint inputs:\n  {}",
        offenders.join("\n  "),
    );
}

fn report_offenders(label: &str, offenders: &[String]) {
    const PREVIEW: usize = 32;
    println!("{}: {} mismatches", label, offenders.len());
    for o in offenders.iter().take(PREVIEW) {
        println!("  {o}");
    }
    if offenders.len() > PREVIEW {
        println!("  ... ({} more)", offenders.len() - PREVIEW);
    }
}
