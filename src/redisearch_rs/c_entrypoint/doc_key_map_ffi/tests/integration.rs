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
    let empty = map.memory_usage();
    assert_eq!(map.find(42, |_| true), 0);
    for id in 1..=1000 {
        map.insert(42, id);
    }
    assert!(map.memory_usage() > empty);
    for id in 1..=1000 {
        assert_eq!(map.find(42, |candidate| candidate == id), id);
    }
    assert_eq!(map.find(43, |_| true), 0);
    assert!(map.remove(42, 500));
    assert!(!map.remove(42, 500));
    assert_eq!(map.find(42, |id| id == 500), 0);
    assert_eq!(map.find(42, |id| id == 501), 501);
    for id in 1..=1000 {
        if id != 500 {
            assert!(map.remove(42, id));
        }
    }
    assert_eq!(map.find(42, |_| true), 0);
    map.insert(42, 1001);
    assert_eq!(map.find(42, |_| true), 1001);
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
