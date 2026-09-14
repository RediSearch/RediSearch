/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// `rqe_iterators` (a dependency of `search_disk`) links `libredisearch_c_bundle.a`,
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

#[test]
fn view_returns_none_for_in_memory_context_without_snapshot() {
    // SAFETY: these C structs allow zero-initialization; the view only reads
    // `spec.diskSpec`, which is null for an in-memory index.
    let mut spec: ffi::IndexSpec = unsafe { std::mem::zeroed() };
    // SAFETY: RedisSearchCtx is a C struct whose fields allow zero-initialization.
    let mut sctx: ffi::RedisSearchCtx = unsafe { std::mem::zeroed() };
    sctx.spec = &mut spec;
    // SAFETY: `spec` outlives the borrow, and no disk handles exist in RAM mode.
    let view = unsafe { search_disk::DiskSpecView::new(&sctx) };
    assert!(view.is_none());
}
