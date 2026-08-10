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
        /// Test whether the [`trie_rs::iter::Values`] iterator yields the same results as the BTreeMap values iterator.
        /// In particular, entries are yielded in the same order.
        fn test_values(entries: BTreeMap<Vec<u8>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }

            let trie_values: Vec<i32> = trie.values().copied().collect();
            let trie_into_values: Vec<i32> = trie.into_values().collect();
            let btree_values: Vec<i32> = entries.values().copied().collect();

            assert_eq!(trie_values, btree_values);
            assert_eq!(trie_values, trie_into_values);
        }
    }
}
