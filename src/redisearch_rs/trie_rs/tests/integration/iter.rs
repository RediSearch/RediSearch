use std::{cell::RefCell, ffi::c_char, rc::Rc};

use crate::{c_chars_vec, utils::ToCCharVec as _};
use trie_rs::{
    TrieMap,
    iter::filter::{FilterOutcome, TraversalFilter},
};

#[test]
fn prefix_constraint_is_honored() {
    let mut trie = TrieMap::new();
    trie.insert(&"apple".c_chars(), 1);
    trie.insert(&"ban".c_chars(), 2);
    trie.insert(&"banana".c_chars(), 3);
    trie.insert(&"apricot".c_chars(), 4);

    // Prefix search works when there is a node matching the prefix.
    let entries: Vec<_> = trie.prefixed_iter(&"ap".c_chars()).collect();
    let values: Vec<_> = trie.prefixed_values(&"ap".c_chars()).copied().collect();
    assert_eq!(
        entries,
        vec![("apple".c_chars(), &1), ("apricot".c_chars(), &4)]
    );
    assert_eq!(values, vec![1, 4]);

    // Prefix search works even when there isn't a node matching the prefix.
    let entries: Vec<_> = trie.prefixed_iter(&b"a".c_chars()).collect();
    let values: Vec<_> = trie.prefixed_values(&b"a".c_chars()).copied().collect();
    assert_eq!(
        entries,
        vec![("apple".c_chars(), &1), ("apricot".c_chars(), &4)]
    );
    assert_eq!(values, vec![1, 4]);

    // If the prefix matches an entry, it should be included in the results.
    let entries: Vec<_> = trie.prefixed_iter(&b"ban".c_chars()).collect();
    let values: Vec<_> = trie.prefixed_values(&b"ban".c_chars()).copied().collect();
    assert_eq!(
        entries,
        vec![("ban".c_chars(), &2), ("banana".c_chars(), &3)]
    );
    assert_eq!(values, vec![2, 3]);

    // If the prefix is empty, all entries should be included in the results,
    // ordered lexicographically by key.
    let entries: Vec<_> = trie.prefixed_iter(&b"".c_chars()).collect();
    let values: Vec<_> = trie.prefixed_values(&b"".c_chars()).copied().collect();
    assert_eq!(
        entries,
        vec![
            ("apple".c_chars(), &1),
            ("apricot".c_chars(), &4),
            ("ban".c_chars(), &2),
            ("banana".c_chars(), &3),
        ]
    );
    assert_eq!(values, vec![1, 4, 2, 3]);

    // If there is no entry matching the prefix, an empty iterator should be returned.
    let entries: Vec<_> = trie.prefixed_iter(&"xyz".c_chars()).collect();
    assert!(entries.is_empty());
    let values: Vec<_> = trie.prefixed_values(&"xyz".c_chars()).collect();
    assert!(values.is_empty());
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
}

impl<T: TraversalFilter> TraversalFilter for SpyFilter<T> {
    fn filter(&self, key: &[c_char]) -> FilterOutcome {
        self.visited_keys.borrow_mut().push(key.to_vec());
        self.inner.filter(key)
    }
}

#[test]
fn traversal_filter() {
    let mut trie = TrieMap::new();
    trie.insert(&"apple".c_chars(), 1);
    trie.insert(&"ban".c_chars(), 2);
    trie.insert(&"banana".c_chars(), 3);
    trie.insert(&"apricot".c_chars(), 4);

    let no_ban_prefix = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[c_char]| {
            let is_prefixed = key.starts_with(&b"ban".c_chars());
            FilterOutcome {
                yield_current: !is_prefixed,
                visit_descendants: !is_prefixed,
            }
        },
    };
    let entries: Vec<_> = trie
        .iter()
        .traversal_filter(no_ban_prefix.clone())
        .map(|(k, _)| k)
        .collect();
    assert_eq!(entries, c_chars_vec!["apple", "apricot"]);
    // `ban` was visited, but `banana` was not.
    assert_eq!(
        no_ban_prefix.visited_keys(),
        c_chars_vec!["", "ap", "apple", "apricot", "ban"]
    );

    // Don't yield `ban`, but visit keys that are prefixed with `ban`.
    let no_ban_exact = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[c_char]| FilterOutcome {
            yield_current: key != &b"ban".c_chars(),
            visit_descendants: true,
        },
    };
    let entries: Vec<_> = trie
        .iter()
        .traversal_filter(no_ban_exact.clone())
        .map(|(k, _)| k)
        .collect();
    assert_eq!(entries, c_chars_vec!["apple", "apricot", "banana"]);
    // Both `ban` and `banana` were visited.
    assert_eq!(
        no_ban_exact.visited_keys(),
        c_chars_vec!["", "ap", "apple", "apricot", "ban", "banana"]
    );

    // Skip all keys, traverse no descendants.
    let skip_all = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |_: &[c_char]| FilterOutcome {
            yield_current: false,
            visit_descendants: false,
        },
    };
    let entries: Vec<_> = trie
        .iter()
        .traversal_filter(skip_all.clone())
        .map(|(k, _)| k)
        .collect();
    assert!(entries.is_empty());
    // Only the root was visited.
    assert_eq!(skip_all.visited_keys(), c_chars_vec![""]);
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
