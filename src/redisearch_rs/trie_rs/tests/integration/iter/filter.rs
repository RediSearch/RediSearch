/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::RefCell, rc::Rc};
use trie_rs::{
    TrieMap,
    iter::filter::{FilterOutcome, TraversalFilter},
};

/// A wrapper around a traversal filter that records the keys visited during the traversal.
#[derive(Clone)]
pub struct SpyFilter<T> {
    visited_keys: Rc<RefCell<Vec<Vec<u8>>>>,
    inner: T,
}

impl<T> SpyFilter<T> {
    pub fn visited_keys(&self) -> Vec<Vec<u8>> {
        self.visited_keys.borrow().clone()
    }

    pub fn reset(&mut self) {
        self.visited_keys.borrow_mut().clear();
    }
}

impl<T: TraversalFilter> TraversalFilter for SpyFilter<T> {
    fn filter(&self, key: &[u8]) -> FilterOutcome {
        self.visited_keys.borrow_mut().push(key.to_vec());
        self.inner.filter(key)
    }
}

macro_rules! assert_traversal {
    ($trie:ident, $filter:ident, $expected_entries:expr, $expected_visited_keys:expr) => {{
        // Collect entries using the normal iterator with the traversal filter
        let normal_entries: Vec<Vec<u8>> = $trie
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
    trie.insert(b"", 0);
    trie.insert(b"apple", 1);
    trie.insert(b"ban", 2);
    trie.insert(b"banana", 3);
    trie.insert(b"apricot", 4);

    let mut no_ban_prefix = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[u8]| {
            let is_prefixed = key.starts_with(b"ban");
            FilterOutcome {
                yield_current: !is_prefixed,
                visit_descendants: !is_prefixed,
            }
        },
    };
    assert_traversal!(
        trie,
        no_ban_prefix,
        vec!["".as_bytes(), b"apple", b"apricot"],
        // `ban` was visited, but `banana` was not.
        vec!["".as_bytes(), b"ap", b"apple", b"apricot", b"ban"]
    );

    // Don't yield `ban`, but visit keys that are prefixed with `ban`.
    let mut no_ban_exact = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |key: &[u8]| FilterOutcome {
            yield_current: key != b"ban",
            visit_descendants: true,
        },
    };
    assert_traversal!(
        trie,
        no_ban_exact,
        vec!["".as_bytes(), b"apple", b"apricot", b"banana"],
        // Both `ban` and `banana` were visited.
        vec![
            "".as_bytes(),
            b"ap",
            b"apple",
            b"apricot",
            b"ban",
            b"banana"
        ]
    );

    // Skip all keys, traverse no descendants.
    let mut skip_all = SpyFilter {
        visited_keys: Rc::new(RefCell::new(Vec::new())),
        inner: |_: &[u8]| FilterOutcome {
            yield_current: false,
            visit_descendants: false,
        },
    };
    assert_traversal!(
        trie,
        skip_all,
        Vec::<Vec<u8>>::new(),
        // Only the root was visited.
        vec![b""]
    );
}
