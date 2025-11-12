/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::TrieMap;

// Assert that all variations of prefixed iterators return the expected entries.
macro_rules! assert_prefixed_iterators {
    ($trie:ident, $prefix:expr, $entries:expr) => {{
        let expected: Vec<_> = $entries.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
        // Standard iterator
        let trie_entries: Vec<(Vec<u8>, i32)> =
            $trie.prefixed_iter($prefix).map(|(k, v)| (k, *v)).collect();
        assert_eq!(trie_entries, expected, "Standard iterator failed");

        // Lending iterator
        let mut lending_entries = Vec::new();
        let mut lending_iter = $trie.prefixed_lending_iter($prefix);
        while let Some((key, value)) = lending_iterator::LendingIterator::next(&mut lending_iter) {
            lending_entries.push((key.to_owned(), *value));
        }
        assert_eq!(lending_entries, expected, "Lending iterator failed");

        // Verify the values iterator
        let expected_values: Vec<i32> = $entries.iter().map(|(_, v)| *v).collect();
        let trie_values: Vec<i32> = $trie.prefixed_values($prefix).copied().collect();
        assert_eq!(trie_values, expected_values, "Values iterator failed");
    }};
}

#[test]
fn prefix_constraint_is_honored() {
    let mut trie = TrieMap::new();
    trie.insert(b"", 0);
    trie.insert(b"apple", 1);
    trie.insert(b"ban", 2);
    trie.insert(b"banana", 3);
    trie.insert(b"apricot", 4);

    // Prefix search works when there is a node matching the prefix.
    // `ap` isn't stored in the trie as a key, but there is node
    // with `ap` as a label since `ap` is the shared prefix between
    // `apricot` and `apple`.
    assert_prefixed_iterators!(trie, b"ap", vec![("apple".as_bytes(), 1), (b"apricot", 4)]);

    // Prefix search works even when there isn't a node matching the prefix.
    assert_prefixed_iterators!(trie, b"a", vec![("apple".as_bytes(), 1), (b"apricot", 4)]);

    // If the prefix matches an entry, it should be included in the results.
    assert_prefixed_iterators!(trie, b"ban", vec![("ban".as_bytes(), 2), (b"banana", 3)]);

    // If the prefix is empty, all entries should be included in the results,
    // ordered lexicographically by key.
    assert_prefixed_iterators!(
        trie,
        b"",
        vec![
            ("".as_bytes(), 0),
            (b"apple", 1),
            (b"apricot", 4),
            (b"ban", 2),
            (b"banana", 3),
        ]
    );

    // If there is no entry matching the prefix, an empty iterator should be returned.
    let expected: Vec<(Vec<u8>, i32)> = vec![];
    assert_prefixed_iterators!(trie, b"xyz", expected);
}
