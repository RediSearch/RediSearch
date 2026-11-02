/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str_trie_map::StrTrieMap;

fn collect(trie: &StrTrieMap<i32>, needle: &str) -> Vec<(String, i32)> {
    trie.case_insensitive_iter(needle)
        .map(|(k, v)| (k, *v))
        .collect()
}

#[test]
fn matches_all_case_variants_of_the_needle() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);
    trie.insert("Hello", 2);
    trie.insert("HELLO", 3);
    trie.insert("hell", 4);
    trie.insert("hellos", 5);

    let expected = vec![
        ("HELLO".to_string(), 3),
        ("Hello".to_string(), 2),
        ("hello".to_string(), 1),
    ];
    assert_eq!(collect(&trie, "hello"), expected);
    assert_eq!(collect(&trie, "HeLlO"), expected);
}

#[test]
fn no_match_yields_nothing() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello", 1);

    assert!(collect(&trie, "help").is_empty());
    assert!(collect(&trie, "hell").is_empty());
    assert!(collect(&trie, "helloo").is_empty());
    assert!(collect(&trie, "").is_empty());
    assert!(collect(&StrTrieMap::new(), "hello").is_empty());
}

#[test]
fn multibyte_keys_match_case_insensitively() {
    let mut trie = StrTrieMap::new();
    trie.insert("über", 1);
    trie.insert("Über", 2);
    trie.insert("uber", 3);

    assert_eq!(
        collect(&trie, "ÜBER"),
        vec![("Über".to_string(), 2), ("über".to_string(), 1)],
    );
}

#[test]
fn node_splits_inside_a_codepoint_are_handled() {
    // 'è' (C3 A8) and 'é' (C3 A9) share their first byte, so inserting both
    // splits a trie node in the middle of the codepoint. The automaton must
    // carry the partial codepoint across the edge boundary.
    let mut trie = StrTrieMap::new();
    trie.insert("cafè", 1);
    trie.insert("café", 2);

    assert_eq!(collect(&trie, "CAFÉ"), vec![("café".to_string(), 2)]);
    assert_eq!(collect(&trie, "CAFÈ"), vec![("cafè".to_string(), 1)]);
}

#[test]
fn one_to_many_folding_matches_expanded_needle() {
    // 'İ' folds to "i" + U+0307; a needle already in folded form must match
    // the stored uppercase key.
    let mut trie = StrTrieMap::new();
    trie.insert("İstanbul", 1);

    assert_eq!(
        collect(&trie, "i\u{307}stanbul"),
        vec![("İstanbul".to_string(), 1)],
    );
    assert!(collect(&trie, "istanbul").is_empty());
}

mod property_based {
    #![cfg(not(miri))]

    use proptest::{arbitrary, collection, proptest, sample, strategy::Strategy};
    use std::collections::BTreeMap;
    use trie_rs::str_trie_map::StrTrieMap;

    /// A small, case-rich alphabet, so that random keys collide on prefixes
    /// (forcing node splits, including mid-codepoint ones) and on folded
    /// forms. It spans all UTF-8 widths and the interesting folding shapes.
    const ALPHABET: &[char] = &[
        'a', 'A', 'i', 'I', 'é', 'É', 'è', 'ß', 'İ', 'ı', 'σ', 'Σ', 'ς', '\u{307}', '𐐀', '𐐨',
    ];

    proptest! {
        /// The iterator yields exactly the entries whose folded key equals
        /// the folded needle, in lexicographical key order — the same result
        /// as filtering a BTreeMap with the fold oracle.
        #[test]
        fn agrees_with_fold_oracle(entries in entries(), needle in key()) {
            let mut trie = StrTrieMap::new();
            for (key, value) in &entries {
                trie.insert(key, *value);
            }

            let actual: Vec<(String, i32)> =
                trie.case_insensitive_iter(&needle).map(|(k, v)| (k, *v)).collect();
            let folded_needle = fold(&needle);
            let expected = entries
                .iter()
                .filter(|(key, _)| fold(key) == folded_needle)
                .map(|(key, value)| (key.clone(), *value))
                .collect::<Vec<_>>();

            assert_eq!(actual, expected);
        }

        /// Every stored key is found when the needle is a fold-preserving
        /// case perturbation of it (uppercasing a random subset of its
        /// codepoints, skipping those whose uppercase folds differently,
        /// like 'ß' → "SS").
        #[test]
        fn finds_key_under_case_perturbation(
            entries in collection::btree_map(key(), arbitrary::any::<i32>(), 1..16),
            index: sample::Index,
            case_flips: u32,
        ) {
            let mut trie = StrTrieMap::new();
            for (key, value) in &entries {
                trie.insert(key, *value);
            }

            let (key, value) = entries.iter().nth(index.index(entries.len())).unwrap();
            let needle: String = key
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    let upper: String = c.to_uppercase().collect();
                    if case_flips >> i & 1 == 1 && fold(&upper) == fold(&c.to_string()) {
                        upper
                    } else {
                        c.to_string()
                    }
                })
                .collect();

            let results: Vec<_> =
                trie.case_insensitive_iter(&needle).map(|(k, v)| (k, *v)).collect();
            assert!(
                results.contains(&(key.clone(), *value)),
                "key {key:?} not found via needle {needle:?}: {results:?}",
            );
        }
    }

    /// Per-codepoint case folding — the matching model of
    /// [`StrTrieMap::case_insensitive_iter`], reimplemented independently.
    fn fold(s: &str) -> String {
        s.chars().flat_map(char::to_lowercase).collect()
    }

    fn key() -> impl Strategy<Value = String> {
        collection::vec(sample::select(ALPHABET), 0..6)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn entries() -> impl Strategy<Value = BTreeMap<String, i32>> {
        collection::btree_map(key(), arbitrary::any::<i32>(), 0..16)
    }
}
