/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::str::StrTrieMap;

#[test]
fn iter_round_trips_multi_byte_utf8_keys() {
    let mut trie = StrTrieMap::new();
    trie.insert("ascii", 1);
    trie.insert("café", 2);
    trie.insert("東京", 3);
    trie.insert("🦀", 4);
    trie.insert("مرحبا", 5);
    trie.insert("שלום", 6);

    let entries: Vec<(String, i32)> = trie.iter().map(|(k, v)| (k, *v)).collect();
    let mut sorted = entries.clone();
    sorted.sort();
    assert_eq!(entries, sorted, "iter must yield lexicographic byte order");
    assert_eq!(entries.len(), 6);
    assert!(entries.contains(&("ascii".to_string(), 1)));
    assert!(entries.contains(&("café".to_string(), 2)));
    assert!(entries.contains(&("東京".to_string(), 3)));
    assert!(entries.contains(&("🦀".to_string(), 4)));
    assert!(entries.contains(&("مرحبا".to_string(), 5)));
    assert!(entries.contains(&("שלום".to_string(), 6)));
}

#[test]
fn prefixed_iter_matches_on_multi_byte_codepoint_boundary() {
    let mut trie = StrTrieMap::new();
    trie.insert("café", 1);
    trie.insert("cafés", 2);
    trie.insert("cafe", 3);

    let mut hits: Vec<(String, i32)> = trie.prefixed_iter("café").map(|(k, v)| (k, *v)).collect();
    hits.sort();
    assert_eq!(
        hits,
        vec![("café".to_string(), 1), ("cafés".to_string(), 2)],
    );
}

#[test]
fn suffixed_iter_matches_on_multi_byte_suffix() {
    let mut trie = StrTrieMap::new();
    trie.insert("hello🦀", 1);
    trie.insert("world🦀", 2);
    trie.insert("plain", 3);

    let mut hits: Vec<(String, i32)> = trie.suffixed_iter("🦀").map(|(k, v)| (k, *v)).collect();
    hits.sort();
    assert_eq!(
        hits,
        vec![("hello🦀".to_string(), 1), ("world🦀".to_string(), 2)],
    );
}

#[test]
fn contains_iter_matches_on_multi_byte_target() {
    let mut trie = StrTrieMap::new();
    trie.insert("préfecture", 1);
    trie.insert("café", 2);
    trie.insert("plain", 3);

    let mut hits: Vec<(String, i32)> = trie.contains_iter("é").map(|(k, v)| (k, *v)).collect();
    hits.sort();
    assert_eq!(
        hits,
        vec![("café".to_string(), 2), ("préfecture".to_string(), 1)],
    );
}

/// `StrTrieMap` matches at the codepoint level, not the grapheme-cluster level.
/// A ZWJ-joined emoji ("bear + ZWJ + snowflake + VS16") is four scalars and
/// thirteen UTF-8 bytes; the trie keeps and yields the byte sequence verbatim
/// but `prefixed_iter` will happily match on any leading prefix of those
/// bytes — codepoint boundaries respected, cluster boundaries not.
#[test]
fn zwj_grapheme_clusters_are_treated_as_codepoint_sequences() {
    let polar_bear = "\u{1F43B}\u{200D}\u{2744}\u{FE0F}";
    let bear = "\u{1F43B}";
    assert_eq!(polar_bear.chars().count(), 4);
    assert_eq!(polar_bear.len(), 13);

    let mut trie = StrTrieMap::new();
    trie.insert(polar_bear, 1);
    trie.insert(bear, 2);

    let round_trip: Vec<(String, i32)> = trie.iter().map(|(k, v)| (k, *v)).collect();
    assert!(round_trip.contains(&(polar_bear.to_string(), 1)));
    assert!(round_trip.contains(&(bear.to_string(), 2)));

    let mut hits: Vec<(String, i32)> = trie.prefixed_iter(bear).map(|(k, v)| (k, *v)).collect();
    hits.sort();
    assert_eq!(
        hits,
        vec![(bear.to_string(), 2), (polar_bear.to_string(), 1)],
        "prefix matching is codepoint-level: the leading BEAR scalar matches \
         both the standalone bear and the ZWJ sequence, with no \
         grapheme-cluster awareness",
    );
}
