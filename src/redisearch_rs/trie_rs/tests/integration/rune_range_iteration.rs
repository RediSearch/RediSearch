/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert the Rust `RuneTrieMap` reproduces the C rune-trie's lex-range
//! iteration (`Trie_IterateRange`) against the snapshots owned by
//! `rune_trie_snapshots::range_iteration`.
//!
//! Assumed `RuneTrieMap` API (does NOT exist yet — porting target):
//!
//! ```ignore
//! impl<Data> RuneTrieMap<Data> {
//!     /// Yield every terminal whose key lies within the lex range
//!     /// `[min, max]` (with inclusivity flags), in lex order. `None` on
//!     /// either side disables that bound — matches the C `(NULL, -1)`
//!     /// sentinel used by `Trie_IterateRange`.
//!     pub fn iterate_range(
//!         &self,
//!         min: Option<&[Rune]>,
//!         include_min: bool,
//!         max: Option<&[Rune]>,
//!         include_max: bool,
//!     ) -> impl Iterator<Item = (Vec<Rune>, &Data)>;
//! }
//! ```
//!
//! Callback-stop semantics are mirrored via `.take(n)` on the iterator —
//! that's the most natural Rust shape for "halt after N hits". The C side
//! halts after exactly 3 callback fires by returning non-zero from the
//! callback; the iterator port just truncates. If the user prefers a
//! callback-bearing `try_for_each` adapter, that's a redesign call.

use std::fmt::Write as _;

use trie_rs::rune::{Rune, RuneTrieMap};

/// Per-term payload. The dump format only needs `num_docs` — no score column.
struct TermEntry {
    num_docs: usize,
}

fn term_runes(s: &str) -> Vec<Rune> {
    s.encode_utf16().collect()
}

fn build_fixture() -> RuneTrieMap<TermEntry> {
    let mut trie = RuneTrieMap::<TermEntry>::new();
    // Same insertion order as `rune_trie_snapshots::range_iteration::build_fixture`.
    for term in [
        "apple", "apricot", "banana", "band", "bandana", "cherry", "date",
    ] {
        trie.insert(&term_runes(term), TermEntry { num_docs: 1 });
    }
    trie
}

/// Run one range query and append a labeled block. `min`/`max` of `None` is
/// the "unbounded on that side" sentinel — same role as the C `(NULL, -1)`
/// pair.
fn dump_range(
    trie: &RuneTrieMap<TermEntry>,
    label: &str,
    min: Option<&str>,
    include_min: bool,
    max: Option<&str>,
    include_max: bool,
    out: &mut String,
) {
    writeln!(
        out,
        "query: {label}  min={:?} (inc={include_min})  max={:?} (inc={include_max})",
        min.unwrap_or("<none>"),
        max.unwrap_or("<none>"),
    )
    .unwrap();

    let min_buf = min.map(term_runes);
    let max_buf = max.map(term_runes);

    let iter = trie.iterate_range(
        min_buf.as_deref(),
        include_min,
        max_buf.as_deref(),
        include_max,
    );

    let mut matches = 0usize;
    for (k, entry) in iter {
        let term = String::from_utf16(&k).expect("trie runes are valid BMP UTF-16");
        writeln!(out, "  {term:10}  numDocs={}", entry.num_docs).unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }
}

fn header(trie: &RuneTrieMap<TermEntry>) -> String {
    format!("size: {}\n\n", trie.len())
}

fn run(queries: &[(&str, Option<&str>, bool, Option<&str>, bool)]) -> String {
    let trie = build_fixture();
    let mut out = header(&trie);
    for (i, (label, min, inc_min, max, inc_max)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        dump_range(&trie, label, *min, *inc_min, *max, *inc_max, &mut out);
    }
    out
}

fn assert_against_shared_snapshot(name: &str, dump: String) {
    insta::with_settings!(
        {
            snapshot_path => "../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!(name, dump); }
    );
}

#[test]
fn lex_range_inclusive_both_ends() {
    let dump = run(&[(
        "[apple, cherry] inclusive",
        Some("apple"),
        true,
        Some("cherry"),
        true,
    )]);
    assert_against_shared_snapshot("lex_range_inclusive_both_ends", dump);
}

#[test]
fn lex_range_exclusive_both_ends() {
    let dump = run(&[(
        "(apple, cherry) exclusive",
        Some("apple"),
        false,
        Some("cherry"),
        false,
    )]);
    assert_against_shared_snapshot("lex_range_exclusive_both_ends", dump);
}

#[test]
fn lex_range_mixed_inclusivity() {
    let dump = run(&[
        (
            "[apple, cherry)",
            Some("apple"),
            true,
            Some("cherry"),
            false,
        ),
        (
            "(apple, cherry]",
            Some("apple"),
            false,
            Some("cherry"),
            true,
        ),
    ]);
    assert_against_shared_snapshot("lex_range_mixed_inclusivity", dump);
}

#[test]
fn lex_range_one_sided_min_only() {
    let dump = run(&[("[b, +inf)", Some("b"), true, None, true)]);
    assert_against_shared_snapshot("lex_range_one_sided_min_only", dump);
}

#[test]
fn lex_range_one_sided_max_only() {
    let dump = run(&[("(-inf, b]", None, true, Some("b"), true)]);
    assert_against_shared_snapshot("lex_range_one_sided_max_only", dump);
}

#[test]
fn lex_range_unbounded() {
    let dump = run(&[("(-inf, +inf)", None, true, None, true)]);
    assert_against_shared_snapshot("lex_range_unbounded", dump);
}

#[test]
fn lex_range_empty() {
    let dump = run(&[("[ze, zz]", Some("ze"), true, Some("zz"), true)]);
    assert_against_shared_snapshot("lex_range_empty", dump);
}

#[test]
fn lex_range_min_equals_max() {
    let dump = run(&[
        ("[band, band]", Some("band"), true, Some("band"), true),
        ("(band, band)", Some("band"), false, Some("band"), false),
        ("[nope, nope]", Some("nope"), true, Some("nope"), true),
    ]);
    assert_against_shared_snapshot("lex_range_min_equals_max", dump);
}

#[test]
fn lex_range_min_greater_than_max() {
    let dump = run(&[(
        "[cherry, apple] inverted",
        Some("cherry"),
        true,
        Some("apple"),
        true,
    )]);
    assert_against_shared_snapshot("lex_range_min_greater_than_max", dump);
}

#[test]
fn lex_range_callback_stops_early() {
    // C side halts on the 3rd non-zero callback return. Rust-side equivalent
    // is `.take(3)` on the iterator — same observable: exactly 3 records.
    let trie = build_fixture();
    let mut out = header(&trie);
    writeln!(
        &mut out,
        "query: (-inf, +inf) stop after 3rd  min={:?} (inc={})  max={:?} (inc={})",
        "<none>", true, "<none>", true,
    )
    .unwrap();

    let mut matches = 0usize;
    for (k, entry) in trie.iterate_range(None, true, None, true).take(3) {
        let term = String::from_utf16(&k).expect("trie runes are valid BMP UTF-16");
        writeln!(&mut out, "  {term:10}  numDocs={}", entry.num_docs).unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(&mut out, "  <no matches>").unwrap();
    }

    assert_against_shared_snapshot("lex_range_callback_stops_early", out);
}
