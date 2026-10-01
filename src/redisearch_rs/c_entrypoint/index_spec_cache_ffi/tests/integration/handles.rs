/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_spec_cache_ffi::{IndexSpecCache_Decref, IndexSpecCache_Incref, IndexSpecCache_New};
use std::ptr;

/// Every handle refers to the same cache, which stays readable until its
/// last handle is released.
#[test]
fn cache_lives_until_its_last_handle_is_released() {
    // SAFETY: there are no fields to read, and the rule names are null or
    // NUL-terminated.
    let first = unsafe {
        IndexSpecCache_New(
            ptr::null(),
            0,
            c"lang".as_ptr(),
            ptr::null(),
            c"payload".as_ptr(),
        )
    };
    // SAFETY: `first` has not been released.
    let second = unsafe { IndexSpecCache_Incref(first) };
    assert_eq!(first, second);

    // SAFETY: `first` has not been released, and is not used again.
    unsafe { IndexSpecCache_Decref(first) };

    // SAFETY: `second` has not been released.
    let cache = unsafe { &*second };
    assert!(cache.is_rule_special_field(c"lang"));
    assert!(cache.is_rule_special_field(c"payload"));
    assert!(!cache.is_rule_special_field(c"score"));
    assert!(cache.find_field(c"lang").is_none());

    // SAFETY: `second` has not been released, and is not used again.
    unsafe { IndexSpecCache_Decref(second) };
    // SAFETY: null is always accepted.
    unsafe { IndexSpecCache_Decref(ptr::null()) };
}
