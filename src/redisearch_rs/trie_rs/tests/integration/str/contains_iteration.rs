/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert the Rust `StrTrieMap` reproduces the C rune-trie's "contains"
//! iteration (`Trie_IterateContains`) against the snapshots owned by
//! `rune_trie_snapshots::contains_iteration`.
//!
//! The C `Trie_IterateContains(t, str, len, prefix, suffix, ...)` four-way
//! `(prefix, suffix)` switch is split across four Rust entry points:
//!
//!   - `(true, false)`  → [`StrTrieMap::prefixed_iter`]
//!   - `(false, true)`  → [`StrTrieMap::suffixed_iter`]
//!   - `(true, true)`   → [`StrTrieMap::contains_iter`]
//!   - `(false, false)` → [`StrTrieMap::get`] (exact lookup is structurally
//!     a single-element result, not an iteration; the C `(false, false)`
//!     case is also unreachable from the query parser — see
//!     `src/query.c:735` + `src/query_parser/v2/parser.y:618-626`).
//!
//! EMPTY-PATTERN CONTRACT: on a zero-length pattern, every iterator MUST
//! yield zero matches — NOT "every term". This mirrors
//! `TrieNode_Get(root, str, 0, ...)` returning NULL (loop never enters,
//! falls through to `return NULL` at `src/trie/trie_node.c:411`), which
//! short-circuits the C contains walk before it touches the subtree.
//!
//! `TermDictionary` carries a `score` per entry; this test never reads it
//! (the snapshot column is `numDocs={n}` only, to match the C rune oracle).
//! [`UNUSED_SCORE`] makes the dead field's value explicit at the call site.

use std::fmt::Write as _;

use trie_rs::term_dict::TermDictionary;

/// Sentinel score value carried into every seed entry. The dump format
/// for this suite is `numDocs`-only, so the score is never observed.
const UNUSED_SCORE: f32 = 1.0;

fn build_fixture() -> TermDictionary {
    let mut trie = TermDictionary::new();
    // Same insertion order as `rune_trie_snapshots::contains_iteration::build_fixture`.
    for term in ["cat", "catalog", "category", "concat", "scat", "scatter"] {
        trie.replace_term(term, UNUSED_SCORE, 1);
    }
    trie
}

fn emit_match(out: &mut String, matches: &mut usize, term: &str, num_docs: usize) {
    writeln!(out, "  {term:10}  numDocs={num_docs}").unwrap();
    *matches += 1;
}

fn dump_contains(
    trie: &TermDictionary,
    label: &str,
    pattern: &str,
    prefix: bool,
    suffix: bool,
    out: &mut String,
) {
    writeln!(
        out,
        "query: {label}  str={pattern:?}  prefix={prefix}  suffix={suffix}"
    )
    .unwrap();

    let mut matches = 0usize;
    match (prefix, suffix) {
        (true, false) => {
            for (term, entry) in trie.prefixed_iter(pattern) {
                emit_match(out, &mut matches, &term, entry.num_docs);
            }
        }
        (false, true) => {
            for (term, entry) in trie.suffixed_iter(pattern) {
                emit_match(out, &mut matches, &term, entry.num_docs);
            }
        }
        (true, true) => {
            for (term, entry) in trie.contains_iter(pattern) {
                emit_match(out, &mut matches, &term, entry.num_docs);
            }
        }
        (false, false) => {
            // Exact lookup: the C `(false, false)` mode is unreachable from
            // the parser; the test harness reroutes through `get()` so the
            // snapshot can still cross-check the C `Trie_IterateContains`
            // shape. Empty patterns yield no match (mirrors `TrieNode_Get`
            // returning NULL for `len == 0`).
            if !pattern.is_empty()
                && let Some(entry) = trie.get(pattern)
            {
                emit_match(out, &mut matches, pattern, entry.num_docs);
            }
        }
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }
}

fn header(trie: &TermDictionary) -> String {
    format!("size: {}\n\n", trie.len())
}

fn run(queries: &[(&str, &str, bool, bool)]) -> String {
    let trie = build_fixture();
    let mut out = header(&trie);
    for (i, (label, pattern, prefix, suffix)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        dump_contains(&trie, label, pattern, *prefix, *suffix, &mut out);
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
fn lex_contains_pure_prefix() {
    let dump = run(&[("starts-with \"cat\"", "cat", true, false)]);
    assert_against_shared_snapshot("lex_contains_pure_prefix", dump);
}

#[test]
fn lex_contains_pure_suffix() {
    let dump = run(&[("ends-with \"cat\"", "cat", false, true)]);
    assert_against_shared_snapshot("lex_contains_pure_suffix", dump);
}

#[test]
fn lex_contains_substring() {
    let dump = run(&[("substring \"cat\"", "cat", true, true)]);
    assert_against_shared_snapshot("lex_contains_substring", dump);
}

#[test]
fn lex_contains_exact() {
    let dump = run(&[
        ("exact \"cat\"", "cat", false, false),
        ("exact \"catalo\"", "catalo", false, false),
        ("exact \"nope\"", "nope", false, false),
    ]);
    assert_against_shared_snapshot("lex_contains_exact", dump);
}

#[test]
fn lex_contains_no_match() {
    let dump = run(&[
        ("starts-with \"zzz\"", "zzz", true, false),
        ("ends-with \"zzz\"", "zzz", false, true),
        ("substring \"zzz\"", "zzz", true, true),
        ("exact \"zzz\"", "zzz", false, false),
    ]);
    assert_against_shared_snapshot("lex_contains_no_match", dump);
}

#[test]
fn lex_contains_empty_pattern() {
    // C contract: empty-prefix yields ZERO matches (see module doc above).
    // A naive Rust port that treats `&[]` as "match everything" is wrong
    // and the snapshot will catch it.
    let dump = run(&[("empty-prefix", "", true, false)]);
    assert_against_shared_snapshot("lex_contains_empty_pattern", dump);
}
