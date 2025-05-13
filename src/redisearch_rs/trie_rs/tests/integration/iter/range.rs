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

    use std::collections::{BTreeMap, BTreeSet};
    use trie_rs::TrieMap;
    use trie_rs::iter::{RangeBoundary, RangeFilter};

    proptest::proptest! {
        #[test]
        /// Test whether the [`trie_rs::iter::RangeIter`] iterator yields the same results as
        /// a filtered BTreeMap iterator.
        /// In particular, entries must be yielded in the same order.
        fn test_range_iter(keys: BTreeSet<u16>, min: Option<u16>, min_included: bool, max: Option<u16>, max_included: bool) {
            let mut trie = TrieMap::new();
            for (value, key) in keys.iter().copied().enumerate() {
                trie.insert(&key.to_be_bytes(), value);
            }
            let mut btree: BTreeMap<[u8; 2], usize> = BTreeMap::new();
            for (value, key) in keys.iter().copied().enumerate() {
                btree.insert(key.to_be_bytes(), value);
            }

            let min_bytes = min.map(|m| m.to_be_bytes().to_vec());
            let max_bytes = max.map(|m| m.to_be_bytes().to_vec());
            let filter = RangeFilter {
                min: min_bytes.as_ref().map(|value| RangeBoundary {
                    value,
                    is_included: min_included,
                }),
                max: max_bytes.as_ref().map(|value| RangeBoundary {
                    value,
                    is_included: max_included,
                }),
            };

            let trie_keys: Vec<u16> = trie.range_iter(filter).map(|(k, _)| u16::from_be_bytes([k[0], k[1]])).collect();
            let btree_keys: Vec<u16> = btree.keys().filter_map(|&k| {
                if let Some(min) = &filter.min {
                    if k.as_slice() < min.value || (k.as_slice() == min.value && !min.is_included) {
                        return None;
                    }
                }

                if let Some(max) = &filter.max {
                    if k.as_slice() > max.value || (k.as_slice() == max.value && !max.is_included) {
                        return None;
                    }
                }

                Some(u16::from_be_bytes(k))
            }).collect();

            assert_eq!(trie_keys, btree_keys);
        }
    }
}
