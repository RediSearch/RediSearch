/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// `rqe_iterators` (a dependency of `search_disk`) links `libredisearch_all.a`,
// whose C objects call back into Rust FFI symbols exported only by the
// `c_entrypoint/*_ffi` crates. A normal build garbage-collects those C objects,
// but a coverage build keeps dead code, so the symbols must resolve. Force-link
// the aggregate `redisearch_rs` crate (which re-exports every `*_ffi` symbol)
// and stub any remaining Redis runtime symbols.
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use search_disk::SearchDiskHandle;

#[test]
fn new_returns_none_for_null_spec() {
    // SAFETY: a null `disk_spec` takes the `None` branch without being
    // dereferenced, so the validity precondition is vacuously satisfied.
    let handle = unsafe { SearchDiskHandle::new(std::ptr::null_mut()) };
    assert!(handle.is_none());
}
