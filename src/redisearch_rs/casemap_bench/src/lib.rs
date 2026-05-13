/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Corpus fixtures for `casemap_bench`.
//!
//! Each corpus is a short, fixed string designed to exercise a particular
//! cost regime in [`casemap_compare`]'s `fold_libnu` / `fold_icu` /
//! `lower_libnu` / `lower_icu` mappers:
//!
//! - [`ASCII_LOWER`]: all-ASCII, already lowercase. Maximum miss rate in
//!   libnu's MPH (no fold/lower mapping for any codepoint). Stresses the
//!   per-codepoint loop overhead in libnu and the whole-string allocation
//!   path in ICU.
//! - [`ASCII_MIXED`]: ASCII letters with capitals. Every `A`..`Z` hits a
//!   1:1 mapping in both libraries.
//! - [`LATIN_MIXED`]: Western European with accents and ligatures
//!   (German ß, fi/fl ligatures, é, ü, etc.). Multi-codepoint output for
//!   ß and ligatures.
//! - [`GREEK_MIXED`]: Greek text including capital Σ at word boundaries.
//!   Forces libnu's only context-sensitive branch (Σ → ς) on the
//!   underscore-prefixed `_nu_tolower` — but `nu_tolower` (used by
//!   RediSearch) skips it. ICU at root locale applies it.
//! - [`PATHOLOGICAL`]: everything stitched together at realistic
//!   proportions (mostly ASCII, sprinkled with the rest).

pub const ASCII_LOWER: &str =
    "the quick brown fox jumps over the lazy dog and then sleeps in the warm sun";

pub const ASCII_MIXED: &str =
    "The Quick Brown Fox Jumps Over The Lazy Dog And Then Sleeps In The Warm Sun";

pub const LATIN_MIXED: &str =
    "Straße in München mit café français — naïve fiancée, ﬁsh ﬂour, weiß und schön";

pub const GREEK_MIXED: &str = "ΟΔΥΣΣΕΥΣ καὶ ΑΧΙΛΛΕΥΣ ἐν τῇ Τροίᾳ· ΣΟΦΟΣ ΛΟΓΟΣ, τέλος.";

pub const PATHOLOGICAL: &str = concat!(
    "The Quick Brown Fox Jumps Over The Lazy Dog. ",
    "Straße in München mit café français. ",
    "ΟΔΥΣΣΕΥΣ καὶ ΑΧΙΛΛΕΥΣ. ",
    "ﬁsh ﬂour weiß. ",
    "the quick brown fox jumps over the lazy dog again and again."
);

/// All corpora in one slice, labeled. Order is from cheapest to most
/// expensive expected libnu cost (more miss → more multi-codepoint hits).
pub const ALL: &[(&str, &str)] = &[
    ("ascii_lower", ASCII_LOWER),
    ("ascii_mixed", ASCII_MIXED),
    ("latin_mixed", LATIN_MIXED),
    ("greek_mixed", GREEK_MIXED),
    ("pathological", PATHOLOGICAL),
];
