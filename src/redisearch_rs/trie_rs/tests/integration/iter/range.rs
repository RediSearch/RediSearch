/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::{
    TrieMap,
    iter::{RangeBoundary, RangeFilter},
};

fn in_range<Data>(t: &TrieMap<Data>, filter: RangeFilter) -> Vec<Vec<u8>> {
    t.range_iter(filter).map(|(k, _)| k).collect()
}

#[test]
fn empty_trie_does_not_return_entries() {
    let trie = TrieMap::<u64>::new();

    assert!(in_range(&trie, RangeFilter::all()).is_empty());
}

#[test]
fn range() {
    let mut trie = TrieMap::new();
    trie.insert(b"apple", 0);
    trie.insert(b"ban", 1);
    trie.insert(b"banana", 2);
    trie.insert(b"apricot", 3);

    // If the minimum is greater than the maximum, nothing is returned.
    assert!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::included("ap".as_bytes())),
                max: Some(RangeBoundary::included("a".as_bytes()))
            }
        )
        .is_empty()
    );

    // If the minimum is equal to the maximum, the matching key is
    // returned (if any).
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::included("apple".as_bytes())),
                max: Some(RangeBoundary::included("apple".as_bytes()))
            }
        ),
        vec!["apple".as_bytes()]
    );

    // If the minimum and maximum share a prefix, we visit directly
    // that subtree.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::included("apac".as_bytes())),
                max: Some(RangeBoundary::included("april".as_bytes()))
            }
        ),
        vec!["apple".as_bytes(), b"apricot"]
    );

    // If the minimum and maximum share a prefix, but there is nothing
    // with that prefix, we get nothing back.
    assert!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::included("biker".as_bytes())),
                max: Some(RangeBoundary::included("bis".as_bytes()))
            }
        )
        .is_empty(),
    );

    // If the minimum is lower than all terms stored in the trie, we get
    // all the keys back.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                // Exactly equal to the key attached to the prefix node
                // that's a parent of `apple` and `apricot`
                min: Some(RangeBoundary::included("ap".as_bytes())),
                max: None
            }
        ),
        vec!["apple".as_bytes(), b"apricot", b"ban", b"banana"]
    );
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                // A prefix of the key attached to the prefix node
                // that's a parent of `apple` and `apricot`
                min: Some(RangeBoundary::included("a".as_bytes())),
                max: None
            }
        ),
        vec!["apple".as_bytes(), b"apricot", b"ban", b"banana"]
    );

    // If the minimum matches a key in the trie, and it is included,
    // that key is in the result set.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::included("apple".as_bytes())),
                max: None
            }
        ),
        vec!["apple".as_bytes(), b"apricot", b"ban", b"banana"]
    );

    // But if the minimum is excluded, that key is not returned.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: Some(RangeBoundary::excluded("apple".as_bytes())),
                max: None
            }
        ),
        vec!["apricot".as_bytes(), b"ban", b"banana"]
    );

    // If the maximum is greater than all terms stored in the trie, we get
    // all the keys back.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: None,
                max: Some(RangeBoundary::included("bas".as_bytes()))
            }
        ),
        vec!["apple".as_bytes(), b"apricot", b"ban", b"banana"]
    );

    // If only some keys are smaller than the maximum, those are returned.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: None,
                max: Some(RangeBoundary::included("ba".as_bytes()))
            }
        ),
        vec!["apple".as_bytes(), b"apricot"]
    );

    // If the maximum matches a key in the trie, and it is included,
    // that key is in the result set.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: None,
                max: Some(RangeBoundary::included("ban".as_bytes()))
            }
        ),
        vec!["apple".as_bytes(), b"apricot", b"ban"]
    );

    // But if the maximum is excluded, that key is not returned.
    assert_eq!(
        in_range(
            &trie,
            RangeFilter {
                min: None,
                max: Some(RangeBoundary::excluded("ban".as_bytes()))
            }
        ),
        vec!["apple".as_bytes(), b"apricot"]
    );
}

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

        #[test]
        /// Test whether the [`trie_rs::iter::RangeIter`] iterator yields the same results as
        /// [`trie_rs::iter::Iter`] if the filter has no minimum and no maximum.
        /// In particular, entries must be yielded in the same order.
        fn test_range_iter_without_bounds(keys: BTreeSet<u16>) {
            let mut trie = TrieMap::new();
            for (value, key) in keys.iter().copied().enumerate() {
                trie.insert(&key.to_be_bytes(), value);
            }

            let range_keys = super::in_range(&trie, RangeFilter::all());
            let iter_keys: Vec<_> = trie.iter().map(|(k, _)| k).collect();
            assert_eq!(iter_keys, range_keys);
        }
    }
}
