/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use doc_key_map_ffi::DocKeyMap;

#[test]
fn colliding_candidates_survive_growth_and_independent_removal() {
    let mut map = DocKeyMap::default();
    // Full-hash collisions make this quadratic; 64 entries still exercise multiple resizes.
    let count = if cfg!(miri) { 64 } else { 1000 };
    let removed = count / 2;
    let empty = map.memory_usage();
    assert_eq!(map.find(42, |_| true), 0);
    for id in 1..=count {
        map.insert(42, id);
    }
    assert!(map.memory_usage() > empty);
    for id in 1..=count {
        assert_eq!(map.find(42, |candidate| candidate == id), id);
    }
    assert_eq!(map.find(43, |_| true), 0);
    assert!(map.remove(42, removed));
    assert!(!map.remove(42, removed));
    assert_eq!(map.find(42, |id| id == removed), 0);
    assert_eq!(map.find(42, |id| id == removed + 1), removed + 1);
    for id in 1..=count {
        if id != removed {
            assert!(map.remove(42, id));
        }
    }
    assert_eq!(map.find(42, |_| true), 0);
    map.insert(42, count + 1);
    assert_eq!(map.find(42, |_| true), count + 1);
}

#[test]
fn binary_keys_are_hashed_with_their_full_extent() {
    let mut map = DocKeyMap::default();
    for (i, key) in [b"".as_slice(), b"a", b"a\0", b"a\0b"]
        .into_iter()
        .enumerate()
    {
        let hash = map.hash(key);
        let id = i as u64 + 1;
        map.insert(hash, id);
        assert_eq!(map.get_id(key, |candidate| candidate == id), id);
    }
}
