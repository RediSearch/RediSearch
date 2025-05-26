/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use lending_iterator::LendingIterator;
use trie_rs::{TrieMap, iter::ContainsLendingIter};

/// Return all the keys that contain the given target.
fn contains<Data: Clone>(trie: &TrieMap<Data>, target: &[u8]) -> Vec<Vec<u8>> {
    let lending_keys = {
        let mut keys = Vec::new();
        let mut iter: ContainsLendingIter<_> = trie.contains_iter(target).into();
        while let Some((key, _)) = LendingIterator::next(&mut iter) {
            keys.push(key.to_owned());
        }
        keys
    };
    let iter_keys = trie.contains_iter(target).map(|(k, _)| k).collect();
    assert_eq!(
        iter_keys, lending_keys,
        "Lending and non-lending iterator don't agree on the result set"
    );
    iter_keys
}

#[test]
fn empty_is_always_contained() {
    let mut trie = TrieMap::new();
    trie.insert(b"", b"".to_vec());
    trie.insert(b"apple", b"apple".into());

    assert_eq!(contains(&trie, b""), vec!["".as_bytes(), b"apple"]);
}

#[test]
fn non_empty_contains() {
    let mut trie = TrieMap::new();
    trie.insert(b"apple", b"apple".to_vec());
    trie.insert(b"ban", b"ban".into());
    trie.insert(b"banana", b"banana".into());
    trie.insert(b"apricot", b"apricot".into());

    // No entry contains the target.
    assert!(contains(&trie, b"coat").is_empty());

    assert_eq!(contains(&trie, b"appl"), vec![b"apple"]);
    assert_eq!(contains(&trie, b"ap"), vec!["apple".as_bytes(), b"apricot"]);
    assert_eq!(contains(&trie, b"an"), vec!["ban".as_bytes(), b"banana"]);

    // If the target is stored as a term in the trie,
    // it is returned as it contains itself.
    assert_eq!(contains(&trie, b"ban"), vec!["ban".as_bytes(), b"banana"]);
}

mod property_based {
    #![cfg(not(miri))]

    use std::collections::BTreeMap;
    use trie_rs::TrieMap;

    fn is_subslice(needle: &[u8], haystack: &[u8]) -> bool {
        if needle.len() > haystack.len() {
            return false;
        }
        if needle.len() == 0 {
            return true;
        }
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    proptest::proptest! {
        #[test]
        /// Test whether [`trie_rs::iter::ContainsIter`] yields the same entries as a filtered BTreeMap iterator.
        /// In particular, entries are yielded in the same order.
        fn test_contains_iter(entries: BTreeMap<Vec<u8>, i32>, target: Vec<u8>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<u8>, i32)> = trie.contains_iter(&target).map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<u8>, i32)> = entries.iter().filter_map(|(k, v)| {
                if is_subslice(&target, k) {
                    Some((k.clone(), *v))
                } else {
                    None
                }
            }).collect();

            assert_eq!(trie_entries, btree_entries);
        }
    }
}
