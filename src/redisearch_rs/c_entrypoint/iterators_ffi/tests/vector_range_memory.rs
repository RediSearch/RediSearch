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

use ffi::VecSimQueryReply_Order_BY_ID;
use rqe_iterators::{RQEIterator, RQEIteratorError};
use std::sync::atomic::{AtomicBool, Ordering};
use vector_score_source::test_utils::{ForcedMemoryRatio, TestIndex, collect_ids, uniform_blob};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Whether the fixture's request has a memory stop requested.
fn memory_stopped(index: &TestIndex) -> bool {
    // SAFETY: the fixture's request limits outlive this read; projecting the field reads nothing.
    let flag = unsafe { &raw mut (*index.timeout_ptr()).memory.stopped };
    // SAFETY: the flag is aligned and valid, and every access to it is atomic.
    unsafe { AtomicBool::from_ptr(flag) }.load(Ordering::Relaxed)
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn range_query_under_memory_pressure_stops_for_oom_then_eof() {
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(50.0, 4);
    let _pressure = ForcedMemoryRatio::set(1.0);

    let mut it = common::range_iterator(&index, &query, 400.0, VecSimQueryReply_Order_BY_ID, true);
    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));
    assert!(memory_stopped(&index));
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
    assert!(!memory_stopped(&index));
}
