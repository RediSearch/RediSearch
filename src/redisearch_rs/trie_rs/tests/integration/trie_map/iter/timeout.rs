/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Clock-deadline half of the polling contract documented on
//! [`TIMEOUT_CHECK_GRANULARITY`]: the amortized clock checker clears its
//! own counter on every probe, so without a latch an expired iterator
//! would resume for another window's worth of entries.

use std::time::{Duration, Instant};
use trie_rs::TrieMap;

/// Enough keys that a full walk takes well over
/// [`TIMEOUT_CHECK_GRANULARITY`](trie_rs::iter::TIMEOUT_CHECK_GRANULARITY)
/// traversal steps.
const N_KEYS: usize = 300;

fn seeded_trie() -> TrieMap<usize> {
    let mut trie = TrieMap::new();
    for i in 0..N_KEYS {
        trie.insert(format!("key{i:04}").as_bytes(), i);
    }
    trie
}

#[test]
fn expired_deadline_stops_the_walk_for_good() {
    let trie = seeded_trie();

    let mut iter = trie.prefixed_iter(b"key");
    iter.set_timeout(Some(Instant::now() - Duration::from_secs(1)));

    let yielded = iter.by_ref().count();
    assert!(
        yielded < N_KEYS,
        "an expired deadline must cut the walk short, got all {yielded} entries"
    );
    assert_eq!(
        iter.by_ref().count(),
        0,
        "a timed-out iterator stays exhausted"
    );
}

#[test]
fn unexpired_deadline_yields_every_entry() {
    let trie = seeded_trie();

    let mut iter = trie.prefixed_iter(b"key");
    iter.set_timeout(Some(Instant::now() + Duration::from_secs(3600)));

    assert_eq!(iter.count(), N_KEYS);
}
