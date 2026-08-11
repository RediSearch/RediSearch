/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Case-folding contract for [`TermDictionary`].
//!
//! Every key and pattern is lowercased on entry — see the module doc on
//! [`TermDictionary`] for the rationale (it mirrors the C terms-trie's
//! `runeBufFill` pre-fold). These tests pin the contract at every entry
//! point: insert/lookup, remove, decrement, and all five iteration paths
//! (prefixed, suffixed, contains, range, wildcard, fuzzy).
//!
//! The underlying [`StrTrieMap`](trie_rs::str_trie_map::StrTrieMap) stays
//! byte-exact; tests in sibling files exercise its raw byte semantics
//! and must continue to pass.

use term_dictionary::{DecrResult, TermDictionary, TermEntry};

fn seed(dict: &mut TermDictionary, terms: &[&str]) {
    for term in terms {
        dict.insert(
            term,
            TermEntry {
                score: 1.0,
                num_docs: 1,
            },
        );
    }
}

fn collect_prefixed(dict: &TermDictionary, prefix: &str) -> Vec<String> {
    let mut keys: Vec<String> = dict.prefixed_iter(prefix).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

fn collect_suffixed(dict: &TermDictionary, suffix: &str) -> Vec<String> {
    let mut keys: Vec<String> = dict.suffixed_iter(suffix).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

fn collect_contains(dict: &TermDictionary, target: &str) -> Vec<String> {
    let mut keys: Vec<String> = dict.contains_iter(target).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

fn collect_range(
    dict: &TermDictionary,
    min: Option<&str>,
    include_min: bool,
    max: Option<&str>,
    include_max: bool,
) -> Vec<String> {
    let mut keys: Vec<String> = dict
        .range_iter(min, include_min, max, include_max)
        .map(|(k, _)| k)
        .collect();
    keys.sort();
    keys
}

fn collect_wildcard(dict: &TermDictionary, pattern: &str) -> Vec<String> {
    let mut keys: Vec<String> = dict.wildcard_iter(pattern).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

fn collect_fuzzy(dict: &TermDictionary, pattern: &str, max_dist: u32) -> Vec<String> {
    let mut keys: Vec<String> = dict.fuzzy_iter(pattern, max_dist).map(|(k, _)| k).collect();
    keys.sort();
    keys
}

#[test]
fn insert_uppercase_then_get_lowercase() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "Foo",
        TermEntry {
            score: 2.0,
            num_docs: 3,
        },
    );
    let entry = dict
        .get("foo")
        .expect("get(\"foo\") should find folded key");
    assert_eq!(entry.score, 2.0);
    assert_eq!(entry.num_docs, 3);
}

#[test]
fn insert_lowercase_then_get_uppercase() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "foo",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    assert!(
        dict.get("FOO").is_some(),
        "get(\"FOO\") should fold to \"foo\""
    );
    assert!(
        dict.get("Foo").is_some(),
        "get(\"Foo\") should fold to \"foo\""
    );
}

#[test]
fn iteration_yields_folded_keys() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "Hello",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    let keys: Vec<String> = dict.iter().map(|(k, _)| k).collect();
    assert_eq!(
        keys,
        vec!["hello".to_string()],
        "iteration must yield the folded key, not the raw input"
    );
}

#[test]
fn add_term_collapses_case_variants() {
    let mut dict = TermDictionary::new();
    dict.add_term("Foo", 1.0, 1);
    dict.add_term("FOO", 1.0, 1);
    dict.add_term("foo", 1.0, 1);
    assert_eq!(dict.len(), 1, "case variants must collapse to one entry");
    let entry = dict.get("foo").expect("folded entry");
    assert_eq!(entry.score, 3.0);
    assert_eq!(entry.num_docs, 3);
}

#[test]
fn replace_term_overwrites_case_variant() {
    let mut dict = TermDictionary::new();
    dict.add_term("Foo", 5.0, 2);
    dict.replace_term("FOO", 1.0, 0);
    let entry = dict.get("foo").expect("folded entry");
    assert_eq!(entry.score, 1.0, "replace must overwrite score");
    assert_eq!(entry.num_docs, 2, "replace still accumulates num_docs (+0)");
}

#[test]
fn remove_folds_case() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "Foo",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    assert!(dict.remove("FOO").is_some(), "remove must fold case");
    assert_eq!(dict.len(), 0);
}

#[test]
fn decrement_num_docs_folds_case() {
    let mut dict = TermDictionary::new();
    dict.add_term("Foo", 1.0, 3);
    assert!(matches!(
        dict.decrement_num_docs("FOO", 1),
        DecrResult::Updated
    ));
    assert_eq!(dict.get("foo").unwrap().num_docs, 2);
    assert!(matches!(
        dict.decrement_num_docs("foo", 10),
        DecrResult::Deleted
    ));
    assert!(dict.get("foo").is_none());
}

#[test]
fn prefixed_iter_folds_pattern() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["foobar", "foobaz", "qux"]);
    assert_eq!(
        collect_prefixed(&dict, "FOO"),
        vec!["foobar".to_string(), "foobaz".to_string()]
    );
}

#[test]
fn suffixed_iter_folds_pattern() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["foobar", "carbar", "baz"]);
    assert_eq!(
        collect_suffixed(&dict, "BAR"),
        vec!["carbar".to_string(), "foobar".to_string()]
    );
}

#[test]
fn contains_iter_folds_pattern() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["xfooy", "afoob", "qux"]);
    assert_eq!(
        collect_contains(&dict, "FOO"),
        vec!["afoob".to_string(), "xfooy".to_string()]
    );
}

#[test]
fn range_iter_folds_bounds() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["alpha", "beta", "delta", "gamma"]);
    assert_eq!(
        collect_range(&dict, Some("A"), true, Some("C"), false),
        vec!["alpha".to_string(), "beta".to_string()],
        "uppercase bounds must fold to lowercase before traversal"
    );
}

#[test]
fn range_iter_none_bounds_unchanged() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["alpha", "beta"]);
    assert_eq!(
        collect_range(&dict, None, true, None, true),
        vec!["alpha".to_string(), "beta".to_string()]
    );
}

#[test]
fn wildcard_iter_folds_pattern() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["foo", "fob", "qux"]);
    assert_eq!(
        collect_wildcard(&dict, "F?O"),
        vec!["foo".to_string()],
        "uppercase wildcard literals must fold (the metacharacters \\? \\* are ASCII)"
    );
}

#[test]
fn fuzzy_iter_folds_pattern() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["foo", "bar"]);
    assert_eq!(
        collect_fuzzy(&dict, "FOO", 0),
        vec!["foo".to_string()],
        "fuzzy pattern must fold before the automaton is built"
    );
}

#[test]
fn fold_preserves_sharp_s() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "Straße",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    // `char::to_lowercase` keeps `ß` as `ß` (matching C `unicode_tolower`),
    // so the stored key is "straße". It must NOT be default-folded to
    // "strasse", which would split the term from its C-indexed form.
    assert!(dict.get("straße").is_some());
    assert!(dict.get("STRAßE").is_some(), "ASCII letters still fold");
    assert!(dict.get("strasse").is_none(), "ß must not expand to ss");
    let keys: Vec<String> = dict.iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["straße".to_string()]);
}

#[test]
fn fold_lowercases_uppercase_sigma() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "ΟΔΥΣΣΕΥΣ",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    // Per-char lowering maps every uppercase Σ to σ (no context-sensitive
    // final-sigma rule at the codepoint level), so the all-caps input folds
    // to "οδυσσευσ".
    assert!(dict.get("οδυσσευσ").is_some());
}

#[test]
fn fuzzy_iter_round_trips_sharp_s_through_fold() {
    let mut dict = TermDictionary::new();
    dict.insert(
        "Straße",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    // ß is preserved by the fold, so the stored key is "straße". An exact
    // fuzzy match over the same input round-trips at distance 0.
    assert_eq!(
        collect_fuzzy(&dict, "Straße", 0),
        vec!["straße".to_string()]
    );
    // A single leading-letter substitution (ASCII, so byte/codepoint
    // counting agree) is one edit away — and outside a zero budget.
    assert_eq!(
        collect_fuzzy(&dict, "Xtraße", 1),
        vec!["straße".to_string()]
    );
    assert!(collect_fuzzy(&dict, "Xtraße", 0).is_empty());
}

#[test]
fn fuzzy_iter_handles_multibyte_lowercase_expansion() {
    // `İ` folds to `i` + combining dot above (two codepoints) — pin the
    // round-trip so a regression to per-char folding fails.
    let mut dict = TermDictionary::new();
    dict.insert(
        "İstanbul",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    let hits = collect_fuzzy(&dict, "İstanbul", 0);
    assert_eq!(
        hits.len(),
        1,
        "fuzzy matching must round-trip the multi-codepoint fold"
    );
}
