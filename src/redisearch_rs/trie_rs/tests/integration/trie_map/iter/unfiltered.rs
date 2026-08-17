/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod property_based {
    #![cfg(not(miri))]

    use std::collections::BTreeMap;
    use trie_rs::TrieMap;

    proptest::proptest! {
        #[test]
        /// Test whether [`trie_rs::iter::Iter`] yields the same entries as the BTreeMap entries iterator.
        /// In particular, entries are yielded in the same order.
        fn test_iter(entries: BTreeMap<Vec<u8>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<u8>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<u8>, i32)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();

            assert_eq!(trie_entries, btree_entries);
        }

        #[test]
        /// Verify that [`trie_rs::iter::Iter`] and [`trie_rs::iter::LendingIter`] yield the same entries, in the same order.
        fn test_lending_iter(entries: BTreeMap<Vec<u8>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<u8>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();

            let mut lending_entries = Vec::new();
            let mut lending_iter = trie.lending_iter();
            while let Some((key, value)) = lending_iterator::LendingIterator::next(&mut lending_iter) {
                lending_entries.push((key.to_owned(), *value));
            }

            assert_eq!(trie_entries, lending_entries);
        }
    }
}
