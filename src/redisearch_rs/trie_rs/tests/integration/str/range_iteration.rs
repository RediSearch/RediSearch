/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert the Rust `StrTrieMap` reproduces the C rune-trie's lex-range
//! iteration (`Trie_IterateRange`) against the snapshots owned by
//! `rune_trie_snapshots::range_iteration`.
//!
//! Drives [`StrTrieMap::range_iter`]. `None` on either side disables that
//! bound — matches the C `(NULL, -1)` sentinel used by `Trie_IterateRange`.
//!
//! Callback-stop semantics are mirrored via `.take(n)` on the iterator —
//! that's the most natural Rust shape for "halt after N hits". The C side
//! halts after exactly 3 callback fires by returning non-zero from the
//! callback; the iterator port just truncates. If the user prefers a
//! callback-bearing `try_for_each` adapter, that's a redesign call.
//!
//! `TermDictionary` carries a `score` per entry; this test never reads it
//! (the snapshot column is `numDocs={n}` only, to match the C rune oracle).
//! [`UNUSED_SCORE`] makes the dead field's value explicit at the call site.

use std::fmt::Write as _;

use trie_rs::str::term_dict::TermDictionary;

/// Sentinel score value carried into every seed entry. The dump format
/// for this suite is `numDocs`-only, so the score is never observed.
const UNUSED_SCORE: f32 = 1.0;

fn build_fixture() -> TermDictionary {
    let mut trie = TermDictionary::new();
    // Same insertion order as `rune_trie_snapshots::range_iteration::build_fixture`.
    for term in [
        "apple", "apricot", "banana", "band", "bandana", "cherry", "date",
    ] {
        trie.replace_term(term, UNUSED_SCORE, 1);
    }
    trie
}

/// Run one range query and append a labeled block. `min`/`max` of `None` is
/// the "unbounded on that side" sentinel — same role as the C `(NULL, -1)`
/// pair.
fn dump_range(
    trie: &TermDictionary,
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

    let iter = trie.range_iter(min, include_min, max, include_max);

    let mut matches = 0usize;
    for (term, entry) in iter {
        writeln!(out, "  {term:10}  numDocs={}", entry.num_docs).unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }
}

fn header(trie: &TermDictionary) -> String {
    format!("size: {}\n\n", trie.len())
}

type Query<'a> = (&'a str, Option<&'a str>, bool, Option<&'a str>, bool);

fn run(queries: &[Query<'_>]) -> String {
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
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
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

/// Regression: range iteration must not double-emit a subtree when the
/// boundary value is a proper prefix of a collapsed child label.
///
/// Mirrors `testRangeBoundaryPrefix` in `tests/cpptests/test_cpp_trie.cpp`.
/// The fixture uses textual terms (rather than the numeric `0..1000` in
/// the C `testBasicRange`) so that root children like `"ban"` have
/// multi-character labels — only those expose the bug, because
/// `rsb_gt`/`rsb_lt` treat e.g. `"b" < "ban"` and would include the
/// boundary child in the for-loop alongside the dedicated boundary
/// recursion above it.
///
/// Not a shared-snapshot test: this asserts a structural invariant
/// (no duplicate emission), not a byte-exact match against the C oracle.
#[test]
fn lex_range_boundary_prefix_no_double_emission() {
    let mut trie = TermDictionary::new();
    for term in ["apple", "banana", "band", "bandana", "cherry", "date"] {
        trie.replace_term(term, UNUSED_SCORE, 1);
    }

    // Min-only: [b, +inf). "b" is a proper prefix of the collapsed "ban"
    // label. Pre-fix C trie would fire the "ban" subtree once via the
    // boundary recursion and again via the for-loop.
    let min_only: Vec<String> = trie
        .range_iter(Some("b"), true, None, true)
        .map(|(k, _)| k)
        .collect();
    let min_only_unique: std::collections::HashSet<&String> = min_only.iter().collect();
    assert_eq!(
        min_only.len(),
        min_only_unique.len(),
        "min-only range emitted a duplicate: {min_only:?}",
    );
    let mut min_only_sorted = min_only;
    min_only_sorted.sort();
    assert_eq!(
        min_only_sorted,
        vec!["banana", "band", "bandana", "cherry", "date"],
    );

    // Max-only: (-inf, banb]. "ban" is a proper prefix of "banb", so
    // `rsb_lt("banb")` would include the "ban" subtree alongside the
    // boundary recursion. Only entries strictly less than "banb" should
    // surface — "band"/"bandana" are lex-greater than "banb".
    let max_only: Vec<String> = trie
        .range_iter(None, true, Some("banb"), true)
        .map(|(k, _)| k)
        .collect();
    let max_only_unique: std::collections::HashSet<&String> = max_only.iter().collect();
    assert_eq!(
        max_only.len(),
        max_only_unique.len(),
        "max-only range emitted a duplicate: {max_only:?}",
    );
    let mut max_only_sorted = max_only;
    max_only_sorted.sort();
    assert_eq!(max_only_sorted, vec!["apple", "banana"]);
}

#[test]
fn lex_range_callback_stops_early() {
    // C side halts on the 3rd non-zero callback return. Rust-side equivalent
    // is `.take(3)` on the iterator — same observable: exactly 3 records.
    let trie = build_fixture();
    let mut out = header(&trie);
    writeln!(
        &mut out,
        "query: (-inf, +inf) stop after 3rd  min={:?} (inc=true)  max={:?} (inc=true)",
        "<none>", "<none>",
    )
    .unwrap();

    let mut matches = 0usize;
    for (term, entry) in trie.range_iter(None, true, None, true).take(3) {
        writeln!(&mut out, "  {term:10}  numDocs={}", entry.num_docs).unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(&mut out, "  <no matches>").unwrap();
    }

    assert_against_shared_snapshot("lex_range_callback_stops_early", out);
}
