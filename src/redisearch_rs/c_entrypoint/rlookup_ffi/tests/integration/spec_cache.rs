/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_spec_cache::{CachedField, IndexSpecCache};
use rlookup_ffi::lookup::{
    RLookup_Cleanup, RLookup_FindTextFieldInSpecCache, RLookup_New, RLookup_SetCache,
};
use std::sync::Arc;

/// Only a full-text field yields its full-text id. For any other field, or a
/// name the cache does not have, `ft_id` is left untouched.
#[test]
fn find_text_field_reports_only_full_text_fields() {
    let cache = IndexSpecCache::new(
        [
            CachedField::new(b"title")
                .with_types(ffi::FieldType_INDEXFLD_T_FULLTEXT)
                .with_ft_id(3),
            CachedField::new(b"price")
                .with_types(ffi::FieldType_INDEXFLD_T_NUMERIC)
                .with_ft_id(7),
        ],
        [],
    );
    let mut lookup = RLookup_New();
    // SAFETY: `lookup` is a live lookup, and it takes over the only handle to
    // the new cache.
    unsafe { RLookup_SetCache(&mut lookup, Arc::into_raw(Arc::new(cache))) };

    let find = |name: &std::ffi::CStr, ft_id: &mut u16| {
        // SAFETY: `lookup` is live, `name` is NUL-terminated, and `ft_id` is
        // writable.
        unsafe { RLookup_FindTextFieldInSpecCache(&lookup, name.as_ptr(), ft_id) }
    };
    let mut ft_id = u16::MAX;
    assert!(find(c"title", &mut ft_id));
    assert_eq!(ft_id, 3);

    let mut ft_id = u16::MAX;
    assert!(!find(c"price", &mut ft_id));
    assert!(!find(c"missing", &mut ft_id));
    assert_eq!(ft_id, u16::MAX);

    // SAFETY: `lookup` is live and not used again.
    unsafe { RLookup_Cleanup(&mut lookup) };
}
