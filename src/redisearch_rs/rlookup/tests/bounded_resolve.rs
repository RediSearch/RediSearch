/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use rlookup::{RLookup, RLookupKeyFlag};

#[test]
fn owns_transient_names_and_reuses_existing_flags_at_capacity() {
    let mut lookup = RLookup::new();
    let flags = RLookupKeyFlag::Hidden | RLookupKeyFlag::ExplicitReturn;
    let existing = lookup.get_key_write_ptr(c"existing", flags).unwrap();
    lookup.seal();
    assert_eq!(
        lookup.get_or_create_key_by_name_ptr(b"existing", 1),
        Some(existing)
    );
    assert!(
        lookup
            .get_or_create_key_by_name_ptr(b"missing", 1)
            .is_none()
    );
    assert_eq!(lookup.get_row_len(), 1);
    let owned = {
        let mut name = b"transient\xff".to_vec();
        let key = lookup.get_or_create_key_by_name_ptr(&name, 2).unwrap();
        name.fill(b'x');
        key
    };
    assert_eq!(
        lookup.get_or_create_key_by_name_ptr(b"transient\xff", 2),
        Some(owned)
    );
    assert!(
        lookup
            .get_or_create_key_by_name_ptr(b"overflow", 2)
            .is_none()
    );
    assert_eq!(lookup.get_row_len(), 2);
    // SAFETY: both pointers remain owned by the live sealed lookup, and are only read here.
    unsafe {
        assert_eq!(existing.as_ref().flags, flags | RLookupKeyFlag::QuerySrc);
        assert_eq!(owned.as_ref().name().as_ref().to_bytes(), b"transient\xff");
    }
}

#[test]
fn rejects_nul_and_zero_capacity_without_mutation() {
    let mut lookup = RLookup::new();
    assert!(lookup.get_or_create_key_by_name_ptr(b"field", 0).is_none());
    assert!(
        lookup
            .get_or_create_key_by_name_ptr(b"bad\0name", 2)
            .is_none()
    );
    assert_eq!(lookup.get_row_len(), 0);
    let empty = lookup.get_or_create_key_by_name_ptr(b"", 1).unwrap();
    assert_eq!(lookup.get_or_create_key_by_name_ptr(b"", 0), Some(empty));
    assert_eq!(lookup.get_row_len(), 1);
}

#[test]
fn raw_pointer_survives_sealed_appends_and_index_promotion() {
    let mut lookup = RLookup::new();
    lookup.seal();
    let first = lookup
        .get_or_create_key_by_name_ptr(b"first", usize::MAX)
        .unwrap();
    for i in 0..128 {
        let name = format!("field{i}");
        assert!(
            lookup
                .get_or_create_key_by_name_ptr(name.as_bytes(), usize::MAX)
                .is_some()
        );
    }
    assert_eq!(
        lookup.get_or_create_key_by_name_ptr(b"first", 0),
        Some(first)
    );
    assert_eq!(lookup.get_row_len(), 129);
    // SAFETY: appends retain the key allocation and the sealed lookup is still alive.
    assert_eq!(unsafe { first.as_ref() }.name().as_ref(), c"first");
}
