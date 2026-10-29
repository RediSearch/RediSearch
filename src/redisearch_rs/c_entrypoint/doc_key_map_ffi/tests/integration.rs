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
