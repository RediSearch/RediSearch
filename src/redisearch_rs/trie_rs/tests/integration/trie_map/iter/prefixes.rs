/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_rs::TrieMap;

/// Return all the keys that are a prefix of the given target.
fn prefixes<Data: Clone>(trie: &TrieMap<Data>, target: &[u8]) -> Vec<Data> {
    trie.prefixes_iter(target).map(|v| v.to_owned()).collect()
}

#[test]
fn empty_is_a_prefix_of_itself() {
    let mut trie = TrieMap::new();
    trie.insert(b"", b"".to_vec());
    trie.insert(b"apple", b"apple".into());

    assert_eq!(prefixes(&trie, b""), vec![b""]);
}

#[test]
fn non_empty_prefixes() {
    let mut trie = TrieMap::new();
    trie.insert(b"apple", b"apple".to_vec());
    trie.insert(b"ban", b"ban".into());
    trie.insert(b"banana", b"banana".into());
    trie.insert(b"apricot", b"apricot".into());

    // No non-empty term is a prefix of the empty string.
    assert!(prefixes(&trie, b"").is_empty());

    assert_eq!(prefixes(&trie, b"apples"), vec![b"apple"]);

    // If the target is stored as a term in the trie,
    // it is returned as a valid prefix of itself.
    assert_eq!(prefixes(&trie, b"ban"), vec![b"ban"]);

    assert_eq!(
        prefixes(&trie, b"bananas"),
        vec!["ban".as_bytes(), b"banana"]
    );

    assert!(prefixes(&trie, b"peach").is_empty());
}
