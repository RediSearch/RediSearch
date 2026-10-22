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
fn binary_keys_are_hashed_with_their_full_extent() {
    let mut map = DocKeyMap::default();
    for (i, key) in [b"".as_slice(), b"a", b"a\0", b"a\0b"]
        .into_iter()
        .enumerate()
    {
        let id = i as u64 + 1;
        map.insert(key, id);
        assert_eq!(map.get_id(key, |candidate| candidate == id), id);
        assert!(map.remove(key, id));
        assert_eq!(map.get_id(key, |_| true), 0);
    }
}

#[test]
fn ffi_mutations_accept_empty_and_binary_keys() {
    use doc_key_map_ffi::{DocKeyMap_Insert, DocKeyMap_Remove};
    let mut map = DocKeyMap::default();
    let keys: &[&[u8]] = &[b"", b"a", b"a\0", b"a\0b"];
    for (i, key) in keys.iter().enumerate() {
        let ptr = if key.is_empty() {
            std::ptr::null()
        } else {
            key.as_ptr()
        };
        // SAFETY: The map is exclusively borrowed, each ID is new, and each key
        // is readable for its length (the empty key deliberately uses null).
        unsafe { DocKeyMap_Insert(&mut map, ptr, key.len(), i as u64 + 1) };
    }
    for (i, key) in keys.iter().enumerate() {
        let id = i as u64 + 1;
        assert_eq!(map.get_id(key, |candidate| candidate == id), id);
        let ptr = if key.is_empty() {
            std::ptr::null()
        } else {
            key.as_ptr()
        };
        // SAFETY: The map is exclusively borrowed and the key bytes remain live.
        assert!(unsafe { DocKeyMap_Remove(&mut map, ptr, key.len(), id) });
        assert_eq!(map.get_id(key, |_| true), 0);
    }
}

#[test]
fn reserve_preserves_collisions_and_avoids_growth_during_backfill() {
    let mut map = DocKeyMap::default();
    map.insert(42, 1);
    map.insert(42, 2);
    map.reserve(64);
    let reserved = map.memory_usage();
    // A smaller request must neither shrink nor underflow the additional capacity.
    map.reserve(0);
    for id in 3..=64 {
        map.insert(id, id);
    }
    assert_eq!(map.memory_usage(), reserved);
    assert_eq!(map.find(42, |id| id == 1), 1);
    assert_eq!(map.find(42, |id| id == 2), 2);
    for id in 3..=64 {
        assert_eq!(map.find(id, |candidate| candidate == id), id);
    }
}

#[test]
fn backfill_deduplicates_ids_without_merging_hash_collisions() {
    let mut map = DocKeyMap::default();
    map.insert(42, 1);
    map.insert_if_absent(42, 1);
    map.insert_if_absent(42, 2);
    map.insert_if_absent(42, 2);
    assert!(map.remove(42, 1));
    assert!(!map.remove(42, 1));
    assert_eq!(map.find(42, |_| true), 2);
    assert!(map.remove(42, 2));
    assert_eq!(map.find(42, |_| true), 0);
}
