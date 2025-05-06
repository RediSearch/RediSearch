/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::RefCell, ffi::c_char, rc::Rc};

use crate::{c_chars_vec, utils::ToCCharVec as _};
use trie_rs::{
    TrieMap,
    iter::filter::{FilterOutcome, TraversalFilter},
};

// Assert that all variations of prefixed iterators return the expected entries.
macro_rules! assert_prefixed_iterators {
    ($trie:ident, $prefix:expr, $entries:expr) => {{
        // Standard iterator
        let trie_entries: Vec<(Vec<c_char>, i32)> = $trie
            .prefixed_iter($prefix)
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        assert_eq!(trie_entries, $entries, "Standard iterator failed");

        // Lending iterator
        let mut lending_entries = Vec::new();
        let mut lending_iter = $trie.prefixed_lending_iter($prefix);
        while let Some((key, value)) = lending_iterator::LendingIterator::next(&mut lending_iter) {
            lending_entries.push((key.to_owned(), *value));
        }
        assert_eq!(lending_entries, $entries, "Lending iterator failed");

        // Verify the values iterator
        let expected_values: Vec<i32> = $entries.iter().map(|(_, v)| *v).collect();
        let trie_values: Vec<i32> = $trie.prefixed_values($prefix).copied().collect();
        assert_eq!(trie_values, expected_values, "Values iterator failed");
    }};
}

#[test]
fn prefix_constraint_is_honored() {
    let mut trie = TrieMap::new();
    trie.insert(&"".c_chars(), 0);
    trie.insert(&"apple".c_chars(), 1);
    trie.insert(&"ban".c_chars(), 2);
    trie.insert(&"banana".c_chars(), 3);
    trie.insert(&"apricot".c_chars(), 4);

    // Prefix search works when there is a node matching the prefix.
    // `ap` isn't stored in the trie as a key, but there is node
    // with `ap` as a label since `ap` is the shared prefix between
    // `apricot` and `apple`.
    assert_prefixed_iterators!(
        trie,
        &"ap".c_chars(),
        vec![("apple".c_chars(), 1), ("apricot".c_chars(), 4)]
    );

    // Prefix search works even when there isn't a node matching the prefix.
    assert_prefixed_iterators!(
        trie,
        &"a".c_chars(),
        vec![("apple".c_chars(), 1), ("apricot".c_chars(), 4)]
    );

    // If the prefix matches an entry, it should be included in the results.
    assert_prefixed_iterators!(
        trie,
        &"ban".c_chars(),
        vec![("ban".c_chars(), 2), ("banana".c_chars(), 3)]
    );

    // If the prefix is empty, all entries should be included in the results,
    // ordered lexicographically by key.
    assert_prefixed_iterators!(
        trie,
        &"".c_chars(),
        vec![
            ("".c_chars(), 0),
            ("apple".c_chars(), 1),
            ("apricot".c_chars(), 4),
            ("ban".c_chars(), 2),
            ("banana".c_chars(), 3),
        ]
    );

    // If there is no entry matching the prefix, an empty iterator should be returned.
    let expected: Vec<(Vec<c_char>, i32)> = vec![];
    assert_prefixed_iterators!(trie, &"xyz".c_chars(), expected);
}

/// A wrapper around a traversal filter that records the keys visited during the traversal.
#[derive(Clone)]
pub struct SpyFilter<T> {
    visited_keys: Rc<RefCell<Vec<Vec<c_char>>>>,
    inner: T,
}

impl<T> SpyFilter<T> {
    pub fn visited_keys(&self) -> Vec<Vec<c_char>> {
        self.visited_keys.borrow().clone()
    }

    pub fn reset(&mut self) {
        self.visited_keys.borrow_mut().clear();
    }
}

impl<T: TraversalFilter> TraversalFilter for SpyFilter<T> {
    fn filter(&self, key: &[c_char]) -> FilterOutcome {
        self.visited_keys.borrow_mut().push(key.to_vec());
        self.inner.filter(key)
    }
}

macro_rules! assert_traversal {
    ($trie:ident, $filter:ident, $expected_entries:expr, $expected_visited_keys:expr) => {{
        // Collect entries using the normal iterator with the traversal filter
        let normal_entries: Vec<Vec<c_char>> = $trie
            .iter()
            .traversal_filter($filter.clone())
            .map(|(k, _)| k.clone())
            .collect();

        assert_eq!(
            normal_entries, $expected_entries,
            "The filtered results for the standard iterator do not match expected entries"
        );
        assert_eq!(
            $filter.visited_keys(),
            $expected_visited_keys,
            "The visited keys for the filtered standard iterator do not match expected entries"
        );

        $filter.reset();

        // Collect entries using the lending iterator with the traversal filter
        let mut lending_entries = Vec::new();
        let mut lending_iter = $trie.lending_iter().traversal_filter($filter.clone());
        while let Some((key, _)) = lending_iterator::LendingIterator::next(&mut lending_iter) {
            lending_entries.push(key.to_owned());
        }

        assert_eq!(
            lending_entries, $expected_entries,
            "The filtered results for the lending iterator do not match expected entries"
        );
        assert_eq!(
            $filter.visited_keys(),
            $expected_visited_keys,
            "The visited keys for the filtered lending iterator do not match expected entries"
        );
    }};
}

#[test]
fn traversal_filter() {
    let mut trie = TrieMap::new();
    trie.insert(&"".c_chars(), 0);
    trie.insert(&"apple".c_chars(), 1);
    trie.insert(&"ban".c_chars(), 2);
    trie.insert(&"banana".c_chars(), 3);
    trie.insert(&"apricot".c_chars(), 4);

    let mut no_ban_prefix = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[c_char]| {
            let is_prefixed = key.starts_with(&b"ban".c_chars());
            FilterOutcome {
                yield_current: !is_prefixed,
                visit_descendants: !is_prefixed,
            }
        },
    };
    assert_traversal!(
        trie,
        no_ban_prefix,
        c_chars_vec!["", "apple", "apricot"],
        // `ban` was visited, but `banana` was not.
        c_chars_vec!["", "ap", "apple", "apricot", "ban"]
    );

    // Don't yield `ban`, but visit keys that are prefixed with `ban`.
    let mut no_ban_exact = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[c_char]| FilterOutcome {
            yield_current: key != &b"ban".c_chars(),
            visit_descendants: true,
        },
    };
    assert_traversal!(
        trie,
        no_ban_exact,
        c_chars_vec!["", "apple", "apricot", "banana"],
        // Both `ban` and `banana` were visited.
        c_chars_vec!["", "ap", "apple", "apricot", "ban", "banana"]
    );

    // Skip all keys, traverse no descendants.
    let mut skip_all = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |_: &[c_char]| FilterOutcome {
            yield_current: false,
            visit_descendants: false,
        },
    };
    assert_traversal!(
        trie,
        skip_all,
        Vec::<Vec<c_char>>::new(),
        // Only the root was visited.
        c_chars_vec![""]
    );
}

mod property_based {
    #![cfg(not(miri))]

    use std::collections::BTreeMap;
    use std::ffi::c_char;
    use trie_rs::TrieMap;

    proptest::proptest! {
        #[test]
        /// Test whether [`trie_rs::iter::Iter`] yields the same entries as the BTreeMap entries iterator.
        /// In particular, entries are yielded in the same order.
        fn test_iter(entries: BTreeMap<Vec<c_char>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<c_char>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();
            let btree_entries: Vec<(Vec<c_char>, i32)> = entries.iter().map(|(k, v)| (k.clone(), *v)).collect();

            assert_eq!(trie_entries, btree_entries);
        }

        #[test]
        /// Verify that [`trie_rs::iter::Iter`] and [`trie_rs::iter::LendingIter`] yield the same entries, in the same order.
        fn test_lending_iter(entries: BTreeMap<Vec<c_char>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }
            let trie_entries: Vec<(Vec<c_char>, i32)> = trie.iter().map(|(k, v)| (k.clone(), *v)).collect();

            let mut lending_entries = Vec::new();
            let mut lending_iter = trie.lending_iter();
            while let Some((key, value)) = lending_iterator::LendingIterator::next(&mut lending_iter) {
                lending_entries.push((key.to_owned(), *value));
            }

            assert_eq!(trie_entries, lending_entries);
        }

        #[test]
        /// Test whether the [`trie_rs::iter::Values`] iterator yields the same results as the BTreeMap values iterator.
        /// In particular, entries are yielded in the same order.
        fn test_values(entries: BTreeMap<Vec<c_char>, i32>) {
            let mut trie = TrieMap::new();
            for (key, value) in entries.clone() {
                trie.insert(key.as_slice(), value);
            }

            let trie_values: Vec<i32> = trie.values().copied().collect();
            let btree_values: Vec<i32> = entries.values().copied().collect();

            assert_eq!(trie_values, btree_values);
        }
    }
}
