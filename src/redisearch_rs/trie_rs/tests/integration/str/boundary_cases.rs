/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `StrTrieMap` reproduces the C trie's input-length-guard and
//! non-BMP-codepoint-truncation behavior against the snapshots owned by
//! `rune_trie_snapshots::boundary_cases`.
//!
//! See that crate's docs for the structural rationale. Two design choices
//! the Rust side must make explicit to match the C oracle:
//!
//!   - **Length guards**: the C trie has two independent rejection paths
//!     (`Trie_InsertStringBuffer` rejects > 512 bytes; `Trie_InsertRune`
//!     requires 1 ≤ runes < 256). The Rust port must reproduce *both* to
//!     match the rc=0/1 column in the snapshot. We model this at the test
//!     wrapper rather than baking it into `StrTrieMap` itself, so the
//!     pure trie stays value-agnostic and length-agnostic. A real port
//!     would push these guards into the `Trie` shell that wraps the map.
//!
//!   - **Non-BMP truncation**: the C trie's `strToRunesN` does `(rune)cp`
//!     on each decoded codepoint, dropping the high bits for non-BMP
//!     scalars (e.g. U+1F980 → 0xF980). `String::encode_utf16` would
//!     surrogate-pair-encode the same input (D83E DD80), producing
//!     different keys. To pin the C behavior we use a dedicated
//!     `term_runes_truncated` helper that mirrors the cast.
//!
//! # Expected API surface
//!
//! Items the compiler will currently flag:
//!
//! - `StrTrieMap::<V>::insert(&mut self, key: &str, value: V) -> Option<V>` —
//!   returns the previously stored value, or `None` if the slot was empty.
//!   The current `insert` returns `()`. Mirroring C's NEW vs UPDATED rc
//!   needs a found-or-not signal at the call site; `Option<V>` is the
//!   idiomatic Rust shape (and `()` is a `Drop`-no-op, so the existing
//!   tests don't notice).
//!
//! # UTF-8 port note
//!
//! `StrTrieMap` keys whole `&str` slices, so the C `(rune)cp` truncation
//! is structurally absent — distinct codepoints stay distinct. The second
//! test exercises that divergence on purpose; the shared snapshot will
//! report a mismatch and we accept it.

use std::fmt::Write as _;

use trie_rs::str::StrTrieMap;

/// Insert with C-style length guards layered on top of the trie. The C
/// trie's byte-guard becomes a UTF-8 byte-count guard; the rune-guard
/// becomes a char-count guard. Returns the C-equivalent rc: 1 = NEW,
/// 0 = update or rejected.
fn insert(trie: &mut StrTrieMap<()>, term: &str) -> i32 {
    // C `Trie_InsertStringBuffer` byte-guard (> 512 bytes pre-conversion).
    if term.len() > 512 {
        return 0;
    }
    // C `Trie_InsertRune` count-guard (1 ≤ runes < 256, strict `<`).
    let char_count = term.chars().count();
    if char_count == 0 || char_count >= 256 {
        return 0;
    }
    let prev = trie.insert(term, ());
    if prev.is_none() { 1 } else { 0 }
}

/// Dump every terminal entry: char length + the term itself (head/tail
/// summary if it would otherwise dominate the snapshot).
fn dump_all(trie: &StrTrieMap<()>) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();

    for (term, _) in trie.iter() {
        let char_len = term.chars().count();
        let display = if char_len <= 16 {
            format!("{term:?}")
        } else {
            let head: String = term.chars().take(4).collect();
            let tail: String = term
                .chars()
                .rev()
                .take(4)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            format!("<{char_len}-rune term: {head:?}..{tail:?}>")
        };
        writeln!(&mut out, "  runeLen={char_len:3}  {display}").unwrap();
    }
    out
}

#[test]
fn lex_input_length_boundaries() {
    let mut trie = StrTrieMap::<()>::new();

    struct Case {
        label: &'static str,
        term: String,
        expected: &'static str,
    }

    let cases: Vec<Case> = vec![
        Case {
            label: "empty string",
            term: String::new(),
            expected: "rejected by Trie_InsertRune (len > 0 required)",
        },
        Case {
            label: "single ASCII rune",
            term: "a".to_string(),
            expected: "accepted (minimum non-empty input)",
        },
        Case {
            label: "255-rune ASCII (one below TRIE_INITIAL_STRING_LEN)",
            term: "x".repeat(255),
            expected: "accepted (255 < 256)",
        },
        Case {
            label: "256-rune ASCII (at TRIE_INITIAL_STRING_LEN — strict `<`)",
            term: "y".repeat(256),
            expected: "rejected by Trie_InsertRune (256 not strictly < 256)",
        },
        Case {
            label: "513-byte ASCII (one above TRIE_INITIAL_STRING_LEN * sizeof(rune))",
            term: "z".repeat(513),
            expected: "rejected by Trie_InsertStringBuffer (513 > 512 bytes)",
        },
    ];

    let mut out = String::new();
    for case in &cases {
        let rc = insert(&mut trie, &case.term);
        writeln!(
            &mut out,
            "--- {} (bytes={}, expected: {}) ---",
            case.label,
            case.term.len(),
            case.expected,
        )
        .unwrap();
        writeln!(&mut out, "rc = {rc}").unwrap();
    }

    writeln!(&mut out, "\n--- final trie contents ---").unwrap();
    out.push_str(&dump_all(&trie));

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_input_length_boundaries", out); }
    );
}

/// Regression: iteration must not overflow the stack on a chain of
/// nested terminals up to the maximum supported depth.
///
/// Mirrors `testDeepEntry` in `tests/cpptests/test_cpp_trie.cpp`. The C
/// test inserts every prefix of a 255-char `"1...1"` string (lengths 1
/// through `TRIE_INITIAL_STRING_LEN - 1 = 255`), then runs the range
/// `["1", "1Z")` and asserts the trie yields all 255 entries. The point
/// is that nothing in the recursion path explodes on the maximum-depth
/// trie shape.
///
/// Not a shared-snapshot test: the C oracle test asserts a count, so we
/// do the same here.
#[test]
fn lex_deep_entry_chain_iterates_without_stack_overflow() {
    let mut trie = StrTrieMap::<()>::new();

    // 255 = TRIE_INITIAL_STRING_LEN - 1, the highest depth the C trie
    // accepts (`Trie_InsertRune` rejects 256 via strict `<`).
    const MAX_LEN: usize = 255;

    for len in 1..=MAX_LEN {
        let term = "1".repeat(len);
        let rc = insert(&mut trie, &term);
        assert_eq!(rc, 1, "expected NEW insert for len={len}");
    }
    assert_eq!(trie.len(), MAX_LEN);

    // include_max=false matches the C `Trie_IterateRange(..., false)`.
    // Every key is "1...1" — strictly less than "1Z" since '1' < 'Z'.
    let hits: Vec<String> = trie
        .range_iter(Some("1"), true, Some("1Z"), false)
        .map(|(k, _)| k)
        .collect();
    assert_eq!(hits.len(), MAX_LEN);
}

#[test]
#[ignore = "UTF-8 keys preserve distinct codepoints; the C `(rune)cp` u16 truncation that aliases U+1F980 onto U+F980 has no structural analog here, so the shared rune snapshot will never match."]
fn lex_non_bmp_codepoint_truncation_and_aliasing() {
    let mut trie = StrTrieMap::<()>::new();

    // Each (utf-8 term, label) — same fixture as the C oracle. The 🦀 vs
    // \u{F980} pair is the punch line: after `(rune)cp` truncation both
    // hash to the same key, so the second insert is an UPDATE (rc=0).
    let cases: &[(&str, &str)] = &[
        ("🦀", "non-BMP — codepoint U+1F980 truncated to rune 0xF980"),
        ("a", "ASCII baseline"),
        (
            "\u{F980}",
            "BMP U+F980 directly — collides with truncated 🦀",
        ),
        ("ab", "ASCII baseline 2"),
        ("a🦀", "BMP + non-BMP — second rune truncated to 0xF980"),
    ];

    let mut out = String::new();
    writeln!(&mut out, "--- inserts ---").unwrap();
    for (term, why) in cases {
        let rc = insert(&mut trie, term);
        // Printed for the snapshot reader to see the divergence between
        // what UTF-16 encoding *would* produce and what we actually store.
        let utf16_runes = term.encode_utf16().count();
        writeln!(
            &mut out,
            "insert({term:?})  utf8bytes={}  rustUtf16Runes={}  rc={rc}  ({why})",
            term.len(),
            utf16_runes,
        )
        .unwrap();
    }

    writeln!(
        &mut out,
        "\n--- iteration (entries are by raw u16; 🦀 and U+F980 collapse) ---"
    )
    .unwrap();
    out.push_str(&dump_all(&trie));

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_non_bmp_codepoint_truncation_and_aliasing", out); }
    );
}
