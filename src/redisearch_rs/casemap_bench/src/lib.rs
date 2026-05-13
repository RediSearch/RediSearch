/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side case-mapping perf comparison between libnu (RediSearch's
//! C-side fold/lower path) and `icu_casemap` (Rust-side ICU4X case mapper).
//!
//! Exposes four mappers that the criterion harness in `benches/casemap.rs`
//! exercises against five fixed corpora:
//!
//! - [`fold_libnu`] — per-codepoint `nu_tofold` lookup (same tables that drive
//!   `runeFold` in `src/trie/rune_util.c`).
//! - [`fold_icu`] — `CaseMapper::fold_string` (matches
//!   `try_insert_string_normalize` in `src/redisearch_rs/sorting_vector/`).
//! - [`lower_libnu`] — per-codepoint `nu_tolower` lookup (matches
//!   `unicode_tolower` in `src/util/strconv.h`, the production
//!   text-normalisation hot path).
//! - [`lower_icu`] — `CaseMapper::lowercase_to_string` at the root locale
//!   ("und": spec-default, context-sensitive but not language-specific).
//!
//! The libnu wrappers re-encode the multi-codepoint mapping bytes through
//! Rust's native UTF-8 handling rather than through libnu's `nu_utf8_write`,
//! so the bench isolates *table-lookup* cost from encoder cost. Treat the
//! libnu numbers as a lower bound for the C-side fold/lower path, not a
//! faithful reproduction of the C call site.

use std::ffi::{CStr, c_char};

use icu_casemap::CaseMapper;
use icu_locale_core::LanguageIdentifier;

unsafe extern "C" {
    /// Return the case-folded mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no folding mapping exists (the codepoint folds
    /// to itself).
    fn nu_tofold(codepoint: u32) -> *const c_char;

    /// Return the lowercase mapping for `codepoint` as a null-terminated UTF-8
    /// string. Returns NULL if no lowercase mapping exists. Unconditional — no
    /// context sensitivity (cf. `_nu_tolower`, which handles Greek final
    /// sigma but is not called from RediSearch).
    fn nu_tolower(codepoint: u32) -> *const c_char;
}

/// Fold `s` using libnu's per-codepoint `nu_tofold`.
pub fn fold_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tofold` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { nu_tofold(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Fold `s` using ICU4X's full Unicode case folding.
///
/// Mirrors `try_insert_string_normalize()` at
/// `src/redisearch_rs/sorting_vector/src/lib.rs`.
pub fn fold_icu(s: &str) -> String {
    CaseMapper::new().fold_string(s).into_owned()
}

/// Lowercase `s` using libnu's per-codepoint `nu_tolower`.
///
/// Mirrors the production text-normalisation path in `unicode_tolower()` at
/// `src/util/strconv.h` and `strToLowerRunes()` at `src/trie/rune_util.c`.
pub fn lower_libnu(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let cp = ch as u32;
        // SAFETY: `nu_tolower` is a pure function; any u32 input is accepted
        // and the returned pointer is either NULL or points to a static,
        // null-terminated UTF-8 buffer owned by libnu's data tables.
        let encoded = unsafe { nu_tolower(cp) };
        if encoded.is_null() {
            out.push(ch);
            continue;
        }
        // SAFETY: `encoded` is non-NULL and points to a null-terminated
        // UTF-8 byte sequence inside libnu's static data.
        let cstr = unsafe { CStr::from_ptr(encoded) };
        match cstr.to_str() {
            Ok(mapped) => out.push_str(mapped),
            Err(_) => out.push('\u{FFFD}'),
        }
    }
    out
}

/// Lowercase `s` using ICU4X (`CaseMapper::lowercase_to_string`) at the root
/// locale ("und": context-sensitive but not language-specific).
pub fn lower_icu(s: &str) -> String {
    CaseMapper::new()
        .lowercase_to_string(s, &LanguageIdentifier::UNKNOWN)
        .into_owned()
}

// -- Corpora ----------------------------------------------------------------
//
// Each corpus is a short, fixed string designed to exercise a particular cost
// regime in the four mappers above. The ordering matches the criterion
// grouping: cheapest to most expensive expected libnu cost (more miss → more
// multi-codepoint hits).

/// All-ASCII, already lowercase. Maximum miss rate in libnu's MPH (no
/// fold/lower mapping for any codepoint). Stresses the per-codepoint loop
/// overhead in libnu and the whole-string allocation path in ICU.
pub const ASCII_LOWER: &str =
    "the quick brown fox jumps over the lazy dog and then sleeps in the warm sun";

/// ASCII letters with capitals. Every `A`..`Z` hits a 1:1 mapping in both
/// libraries.
pub const ASCII_MIXED: &str =
    "The Quick Brown Fox Jumps Over The Lazy Dog And Then Sleeps In The Warm Sun";

/// Western European with accents and ligatures (German ß, fi/fl ligatures,
/// é, ü, etc.). Multi-codepoint output for ß and ligatures.
pub const LATIN_MIXED: &str =
    "Straße in München mit café français — naïve fiancée, ﬁsh ﬂour, weiß und schön";

/// Greek text including capital Σ at word boundaries. Forces libnu's only
/// context-sensitive branch (Σ → ς) on the underscore-prefixed
/// `_nu_tolower` — but `nu_tolower` (used by RediSearch) skips it. ICU at
/// root locale applies it.
pub const GREEK_MIXED: &str = "ΟΔΥΣΣΕΥΣ καὶ ΑΧΙΛΛΕΥΣ ἐν τῇ Τροίᾳ· ΣΟΦΟΣ ΛΟΓΟΣ, τέλος.";

/// Everything stitched together at realistic proportions (mostly ASCII,
/// sprinkled with the rest).
pub const PATHOLOGICAL: &str = concat!(
    "The Quick Brown Fox Jumps Over The Lazy Dog. ",
    "Straße in München mit café français. ",
    "ΟΔΥΣΣΕΥΣ καὶ ΑΧΙΛΛΕΥΣ. ",
    "ﬁsh ﬂour weiß. ",
    "the quick brown fox jumps over the lazy dog again and again."
);

/// All corpora in one slice, labeled. Order is from cheapest to most expensive
/// expected libnu cost (more miss → more multi-codepoint hits).
pub const ALL: &[(&str, &str)] = &[
    ("ascii_lower", ASCII_LOWER),
    ("ascii_mixed", ASCII_MIXED),
    ("latin_mixed", LATIN_MIXED),
    ("greek_mixed", GREEK_MIXED),
    ("pathological", PATHOLOGICAL),
];
