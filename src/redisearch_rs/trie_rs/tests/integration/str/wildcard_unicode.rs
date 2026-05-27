/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard iteration over multi-byte UTF-8 keys.
//!
//! `StrTrieMap` / `TermDictionary` accept `&str` for both keys and
//! patterns, but the underlying [`rqe_wildcard::WildcardPattern`] matcher
//! operates on raw bytes: `?` consumes exactly one byte and the
//! `expected_length` short-circuit counts bytes. For ASCII inputs this is
//! identical to a codepoint-aware matcher; for multi-byte codepoints it is
//! not.
//!
//! These tests pin the **Unicode-intended** semantics (`?` = one
//! codepoint, `*` = zero or more codepoints). They fail under the current
//! byte-level matcher — see each test body for the actual byte-level
//! result alongside the asserted Unicode-intended result.

use trie_rs::str::term_dict::TermDictionary;

/// Score is irrelevant for these tests — only the yielded term set matters.
const UNUSED_SCORE: f32 = 1.0;

fn collect_sorted(dict: &TermDictionary, pattern: &str) -> Vec<String> {
    let mut keys: Vec<String> = dict.wildcard_iter(pattern).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

#[test]
fn question_must_match_one_codepoint_not_one_byte() {
    let mut d = TermDictionary::new();
    d.replace_term("é", UNUSED_SCORE, 1); // 2 bytes, 1 char
    d.replace_term("ab", UNUSED_SCORE, 1); // 2 bytes, 2 chars

    // Unicode-intent: `??` matches strings of exactly 2 codepoints.
    //   Only "ab" qualifies.
    // Byte-level (current): `?` `?` consumes 0xC3 then 0xA9, so the matcher
    //   accepts "é" — atom boundary lands in the middle of the codepoint.
    //   Actual current output: ["ab", "é"].
    assert_eq!(
        collect_sorted(&d, "??"),
        vec!["ab".to_string()],
        "`??` must match exactly two codepoints, not two bytes"
    );
}

#[test]
fn question_should_match_multibyte_char_before_literal() {
    let mut d = TermDictionary::new();
    d.replace_term("aé", UNUSED_SCORE, 1); // 3 bytes, 2 chars
    d.replace_term("éé", UNUSED_SCORE, 1); // 4 bytes, 2 chars

    // Unicode-intent: `?é` matches any single codepoint followed by `é`.
    //   Both "aé" and "éé" qualify.
    // Byte-level (current): pattern bytes are `? \xC3 \xA9`, so
    //   `expected_length` = 3. "aé" (3 bytes) matches; "éé" (4 bytes) is
    //   rejected by the length cap before the matcher even runs.
    //   Actual current output: ["aé"].
    assert_eq!(
        collect_sorted(&d, "?é"),
        vec!["aé".to_string(), "éé".to_string()],
        "`?é` must match any single codepoint followed by literal `é`"
    );
}

#[test]
fn fixed_length_pattern_must_yield_uniform_codepoint_count() {
    let mut d = TermDictionary::new();
    for t in ["a", "é", "ab", "ée", "abc"] {
        d.replace_term(t, UNUSED_SCORE, 1);
    }

    // `?` — one codepoint. Unicode-intent: ["a", "é"].
    //   Byte-level (current): ["a"]  (é is 2 bytes, length-capped out).
    assert_eq!(
        collect_sorted(&d, "?"),
        vec!["a".to_string(), "é".to_string()],
        "`?` must yield every one-codepoint term"
    );

    // `??` — two codepoints. Unicode-intent: ["ab", "ée"].
    //   Byte-level (current): ["ab", "é"]  (mixes 2-char "ab" and 1-char "é").
    assert_eq!(
        collect_sorted(&d, "??"),
        vec!["ab".to_string(), "ée".to_string()],
        "`??` must yield every two-codepoint term, never a one-codepoint term"
    );

    // `???` — three codepoints. Unicode-intent: ["abc"].
    //   Byte-level (current): ["abc", "ée"]  (ée is 2 chars but 3 bytes).
    assert_eq!(
        collect_sorted(&d, "???"),
        vec!["abc".to_string()],
        "`???` must yield every three-codepoint term, never a two-codepoint term"
    );
}
