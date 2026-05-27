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
//! (prefixed, suffixed, contains, range, wildcard, dfa).
//!
//! The underlying [`StrTrieMap`](trie_rs::str::StrTrieMap) stays
//! byte-exact; tests in sibling files exercise its raw byte semantics
//! and must continue to pass.

use trie_rs::str::term_dict::{DecrResult, TermDictionary, TermEntry};

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

fn collect_dfa(dict: &TermDictionary, prefix: &str, max_dist: u32, prefix_mode: bool) -> Vec<String> {
    let mut keys: Vec<String> = dict
        .iterate_dfa(prefix, max_dist, prefix_mode)
        .map(|(k, _, _)| k)
        .collect();
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
    let entry = dict.get("foo").expect("get(\"foo\") should find folded key");
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
    assert!(dict.get("FOO").is_some(), "get(\"FOO\") should fold to \"foo\"");
    assert!(dict.get("Foo").is_some(), "get(\"Foo\") should fold to \"foo\"");
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
fn dfa_iter_folds_prefix() {
    let mut dict = TermDictionary::new();
    seed(&mut dict, &["foo", "bar"]);
    assert_eq!(
        collect_dfa(&dict, "FOO", 0, false),
        vec!["foo".to_string()],
        "DFA prefix must fold before the automaton is built"
    );
}

#[test]
fn fold_expands_sharp_s_to_ss() {
    // ICU `fold_string` expands `ß` to `ss`. Plain `str::to_lowercase`
    // would leave `ß` unchanged. Pins the divergence so a future revert
    // to stdlib lowercasing breaks loudly.
    let mut dict = TermDictionary::new();
    dict.insert(
        "Straße",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    assert!(
        dict.get("strasse").is_some(),
        "fold must rewrite ß to ss on insert"
    );
    assert!(
        dict.get("STRASSE").is_some(),
        "fold must rewrite ß to ss on lookup too"
    );
    let keys: Vec<String> = dict.iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["strasse".to_string()]);
}

#[test]
fn fold_collapses_final_sigma_to_lowercase_sigma() {
    // ICU `fold_string` maps final-position sigma `ς` to `σ`. Plain
    // `str::to_lowercase` would leave the final sigma form alone.
    let mut dict = TermDictionary::new();
    dict.insert(
        "ΟΔΥΣΣΕΥΣ",
        TermEntry {
            score: 1.0,
            num_docs: 1,
        },
    );
    assert!(
        dict.get("οδυσσευσ").is_some(),
        "fold must use medial sigma σ, never final sigma ς"
    );
}

#[test]
fn ascii_lowercase_input_does_not_allocate() {
    // Smoke check the fast-path branch: we don't observe the Cow directly
    // from outside the module, but exercising every entry point with a
    // pre-lowercased input ensures no panic-on-fold path fires for that
    // case. Allocation behavior is verified indirectly via the perf-
    // sensitive tokenizer flow in production (`src/tokenize.c` always
    // pre-lowercases before reaching `sp->terms`).
    let mut dict = TermDictionary::new();
    dict.add_term("foo", 1.0, 1);
    dict.replace_term("foo", 2.0, 0);
    let _ = dict.get("foo");
    let _ = dict.contains_iter("foo").count();
    let _ = dict.prefixed_iter("foo").count();
    let _ = dict.suffixed_iter("foo").count();
    let _ = dict.wildcard_iter("foo").count();
    let _ = dict.range_iter(Some("foo"), true, Some("foo"), true).count();
    let _ = dict.iterate_dfa("foo", 0, false).count();
    let _ = dict.remove("foo");
}
