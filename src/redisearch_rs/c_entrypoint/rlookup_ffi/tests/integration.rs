/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup_ffi::row::SearchResult_CmpByFields;
use search_result::SearchResult;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// With no sort keys, the comparison goes straight to the doc-id tiebreak,
/// without reading `keys`, and ignores the ascend map.
#[test]
fn no_sort_keys_with_a_null_array_falls_through_to_the_doc_id_tiebreak() {
    let mut h1 = SearchResult::new();
    h1.set_doc_id(1);
    let mut h2 = SearchResult::new();
    h2.set_doc_id(2);

    // SAFETY: (1) `nkeys` is 0, so `keys` may be NULL. (2) `h1` and `h2` are
    // live `SearchResult`s. (3) `qerr` is NULL.
    let rc = unsafe {
        SearchResult_CmpByFields(
            std::ptr::null(),
            0,
            &h1,
            &h2,
            u64::MAX,
            std::ptr::null_mut(),
        )
    };
    assert_eq!(rc, -1);
}
