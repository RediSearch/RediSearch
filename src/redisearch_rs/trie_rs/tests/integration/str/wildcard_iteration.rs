/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert the Rust `StrTrieMap` reproduces the C rune-trie's wildcard
//! iteration (`Trie_IterateWildcard`) against the snapshots owned by
//! `rune_trie_snapshots::wildcard_iteration`.
//!
//! Drives [`StrTrieMap::wildcard_iter`]. Pattern syntax:
//!
//!   - `*` matches zero or more runes.
//!   - `?` matches exactly one rune.
//!   - Every other rune is literal — **including `\\`**: the matcher itself
//!     has no escape handling. `\*` means "literal backslash, then
//!     star-wildcard".
//!
//! Callers must pass a non-empty pattern. The C side reads `str[nstr - 1]`
//! at `src/trie/trie_node.c:1204` to decide its `prefix` shortcut flag —
//! `nstr=0` is UB on the C side and undefined-by-contract for the Rust
//! port.

use std::fmt::Write as _;

use trie_rs::str::StrTrieMap;

struct TermEntry {
    num_docs: usize,
}

fn build_fixture() -> StrTrieMap<TermEntry> {
    let mut trie = StrTrieMap::<TermEntry>::new();
    // Same insertion order as `rune_trie_snapshots::wildcard_iteration::build_fixture`.
    for term in ["cat", "car", "cab", "bat", "rat", "category", "concat"] {
        trie.insert(term, TermEntry { num_docs: 1 });
    }
    trie
}

fn dump_wildcard(trie: &StrTrieMap<TermEntry>, label: &str, pattern: &str, out: &mut String) {
    assert!(
        !pattern.is_empty(),
        "wildcard pattern must be non-empty (str[-1] read)"
    );
    writeln!(out, "query: {label}  pattern={pattern:?}").unwrap();

    let mut matches = 0usize;
    for (term, entry) in trie.wildcard_iter(pattern) {
        writeln!(out, "  {term:10}  numDocs={}", entry.num_docs).unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }
}

fn header(trie: &StrTrieMap<TermEntry>) -> String {
    format!("size: {}\n\n", trie.len())
}

fn run(queries: &[(&str, &str)]) -> String {
    let trie = build_fixture();
    let mut out = header(&trie);
    for (i, (label, pattern)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        dump_wildcard(&trie, label, pattern, &mut out);
    }
    out
}

fn assert_against_shared_snapshot(name: &str, dump: String) {
    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!(name, dump); }
    );
}

#[test]
fn lex_wildcard_star_only() {
    let dump = run(&[("*", "*")]);
    assert_against_shared_snapshot("lex_wildcard_star_only", dump);
}

#[test]
fn lex_wildcard_leading_star() {
    let dump = run(&[("*at", "*at")]);
    assert_against_shared_snapshot("lex_wildcard_leading_star", dump);
}

#[test]
fn lex_wildcard_trailing_star() {
    let dump = run(&[("ca*", "ca*")]);
    assert_against_shared_snapshot("lex_wildcard_trailing_star", dump);
}

#[test]
fn lex_wildcard_middle_star() {
    let dump = run(&[("c*t", "c*t")]);
    assert_against_shared_snapshot("lex_wildcard_middle_star", dump);
}

#[test]
fn lex_wildcard_question_mark() {
    let dump = run(&[("?at", "?at")]);
    assert_against_shared_snapshot("lex_wildcard_question_mark", dump);
}

#[test]
fn lex_wildcard_multiple_questions() {
    let dump = run(&[("??t", "??t")]);
    assert_against_shared_snapshot("lex_wildcard_multiple_questions", dump);
}

#[test]
fn lex_wildcard_escape() {
    let dump = run(&[("\\*", "\\*")]);
    assert_against_shared_snapshot("lex_wildcard_escape", dump);
}

#[test]
fn lex_wildcard_no_match() {
    let dump = run(&[("xyz", "xyz")]);
    assert_against_shared_snapshot("lex_wildcard_no_match", dump);
}

// NOTE: no `lex_wildcard_empty_pattern` — the C side reads `str[nstr-1]`
// which is UB on `nstr=0`. The Rust port should refuse empty input by
// contract; we don't snapshot UB.
