/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod filter;
mod prefixed;
mod prefixes;
mod unfiltered;

use trie_rs::TrieMap;

#[test]
/// Verify the correct ordering for non-ASCII keys.
fn utf8() {
    let mut trie = TrieMap::new();
    trie.insert("бълга123".as_bytes(), 0);
    trie.insert(b"abcabc", 1);
    trie.insert("fußball straße".as_bytes(), 2);
    trie.insert("grüßen".as_bytes(), 3);

    let keys: Vec<_> = trie.iter().map(|(key, _)| key).collect();
    assert_eq!(
        keys,
        vec![
            "abcabc".as_bytes(),
            "fußball straße".as_bytes(),
            "grüßen".as_bytes(),
            "бълга123".as_bytes(),
        ]
    );
}
