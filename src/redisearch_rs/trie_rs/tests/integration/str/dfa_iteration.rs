/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `TermDictionary` reproduces the C rune-trie's DFA-filtered
//! iteration (`Trie_Iterate`) against the snapshots owned by
//! `rune_trie_snapshots::dfa_iteration`.
//!
//! Assumed `TermDictionary` API (does NOT exist yet — porting target):
//!
//! ```ignore
//! impl TermDictionary {
//!     /// Iterate every terminal whose key lies within Levenshtein edit
//!     /// distance `max_dist` of `prefix`. With `prefix_mode = true`, any
//!     /// suffix beneath a matched-prefix node is also yielded.
//!     ///
//!     /// `distance` in the yielded triple is the C DFA's "min cost along
//!     /// the matched path" (`MIN(state->distance, minDist)` —
//!     /// `src/trie/levenshtein.c:258-278`), NOT a Levenshtein-from-yielded-
//!     /// term-to-prefix recompute. Reproducing that exactly is the whole
//!     /// point of asserting against the C snapshots.
//!     pub fn iterate_dfa<'tm, 'p>(
//!         &'tm self,
//!         prefix: &'p str,
//!         max_dist: u32,
//!         prefix_mode: bool,
//!     ) -> impl Iterator<Item = (String, &'tm TermEntry, u32)> + 'tm;
//! }
//! ```
//!
//! Each scenario builds the same 10-term fixture as
//! `rune_trie_snapshots::dfa_iteration`, runs the same query batch, and
//! asserts against the same `.snap` files. The shared snapshot path is set
//! via `insta::Settings::snapshot_path`.

use std::fmt::Write as _;

use trie_rs::term_dict::TermDictionary;

fn build_fixture() -> TermDictionary {
    let mut trie = TermDictionary::new();
    // Same insertion order as `rune_trie_snapshots::dfa_iteration::build_fixture`.
    // Order doesn't affect the iterator's lex traversal, but keeping it
    // identical makes any structural divergence (e.g. node-split bugs that
    // depend on insertion order) easier to bisect by tweaking just this list.
    for term in [
        "apple", "apply", "appl", "ape", "banana", "band", "bandana", "b", "cat", "category",
    ] {
        trie.replace_term(term, 1.0, 1);
    }
    trie
}

/// Run a single DFA-filtered query and append its results to `out`.
/// Format mirrors `rune_trie_snapshots::dfa_iteration::dump_filtered` exactly
/// (including the column alignment + the `<no matches>` placeholder) so the
/// `.snap` bytes match.
fn dump_filtered(
    trie: &TermDictionary,
    prefix: &str,
    max_dist: u32,
    prefix_mode: bool,
    out: &mut String,
) {
    // Render the prefix-mode bool as the C-style 0/1 the C scenario emits, so
    // the header lines are byte-identical.
    let prefix_mode_c = prefix_mode as u8;
    writeln!(
        out,
        "query: prefix={prefix:?}, maxDist={max_dist}, prefixMode={prefix_mode_c}"
    )
    .unwrap();

    let mut matches = 0usize;
    for (term, entry, dist) in trie.iterate_dfa(prefix, max_dist, prefix_mode) {
        writeln!(
            out,
            "  {term:10}  dist={dist}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }
}

/// Header line: only the fixture size, matching the C scenario's `header()`.
fn header(trie: &TermDictionary) -> String {
    format!("size: {}\n\n", trie.len())
}

/// Build the fixture once per test, dump each query block separated by a
/// blank line, return the rendered string ready to assert.
fn run(queries: &[(&str, u32, bool)]) -> String {
    let trie = build_fixture();
    let mut out = header(&trie);
    for (i, (prefix, max_dist, prefix_mode)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        dump_filtered(&trie, prefix, *max_dist, *prefix_mode, &mut out);
    }
    out
}

/// Centralized `insta::Settings` for all five scenarios — points the snapshot
/// lookup at the C scenario's snapshots dir, so both crates share the `.snap`.
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
fn lex_iterate_pure_prefix() {
    let dump = run(&[
        ("", 0, true),
        ("ap", 0, true),
        ("ban", 0, true),
        ("z", 0, true),
        ("appl", 0, true),
    ]);
    assert_against_shared_snapshot("lex_iterate_pure_prefix", dump);
}

#[test]
fn lex_iterate_exact_match() {
    let dump = run(&[("apple", 0, false), ("appl", 0, false), ("apples", 0, false)]);
    assert_against_shared_snapshot("lex_iterate_exact_match", dump);
}

#[test]
fn lex_iterate_fuzzy_distance_1() {
    let dump = run(&[
        ("appel", 1, false),
        ("banan", 1, false),
        ("aple", 1, false),
        ("bandz", 1, false),
        ("ca", 1, false),
    ]);
    assert_against_shared_snapshot("lex_iterate_fuzzy_distance_1", dump);
}

#[test]
fn lex_iterate_fuzzy_distance_2() {
    let dump = run(&[("aplez", 2, false), ("bnan", 2, false)]);
    assert_against_shared_snapshot("lex_iterate_fuzzy_distance_2", dump);
}

#[test]
fn lex_iterate_fuzzy_prefix() {
    let dump = run(&[("apl", 1, true), ("bnd", 1, true), ("cta", 1, true)]);
    assert_against_shared_snapshot("lex_iterate_fuzzy_prefix", dump);
}
