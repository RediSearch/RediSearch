/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Memory checkpoint of the lazy vector range iterator, in a test binary of its own because the
//! forced memory ratio is process-global.

mod common;

use ffi::{
    QueryRequestTimeout_GetStopReason, QueryStopReason_QUERY_STOP_REASON_NONE,
    QueryStopReason_QUERY_STOP_REASON_OOM, VecSimQueryReply_Order_BY_ID,
};
use rqe_iterators::{RQEIterator, RQEIteratorError};
use vector_score_source::test_utils::{ForcedMemoryRatio, TestIndex, collect_ids, uniform_blob};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn range_query_under_memory_pressure_stops_for_oom_then_eof() {
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(50.0, 4);
    let _pressure = ForcedMemoryRatio::set(1.0);

    let mut it = common::range_iterator(&index, &query, 400.0, VecSimQueryReply_Order_BY_ID, true);
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
    // SAFETY: the fixture's timeout outlives this read.
    let reason = unsafe { QueryRequestTimeout_GetStopReason(index.timeout_ptr()) };
    assert_eq!(reason, QueryStopReason_QUERY_STOP_REASON_OOM);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn range_query_below_the_memory_limit_runs() {
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(50.0, 4);
    let _pressure = ForcedMemoryRatio::set(0.99);

    // Doc `i` is `[i; 4]`, so a radius of 400 around `[50; 4]` matches the ids within 10 of 50.
    let ids = collect_ids(&mut *common::range_iterator(
        &index,
        &query,
        400.0,
        VecSimQueryReply_Order_BY_ID,
        true,
    ));
    assert_eq!(ids, (40..=60).collect::<Vec<_>>());
    // SAFETY: the fixture's timeout outlives this read.
    let reason = unsafe { QueryRequestTimeout_GetStopReason(index.timeout_ptr()) };
    assert_eq!(reason, QueryStopReason_QUERY_STOP_REASON_NONE);
}
