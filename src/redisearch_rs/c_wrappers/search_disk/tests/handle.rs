/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use search_disk::SearchDiskHandle;

#[test]
fn new_returns_none_for_null_spec() {
    // SAFETY: a null `disk_spec` takes the `None` branch without being
    // dereferenced, so the validity precondition is vacuously satisfied.
    let handle = unsafe { SearchDiskHandle::new(std::ptr::null_mut()) };
    assert!(handle.is_none());
}
