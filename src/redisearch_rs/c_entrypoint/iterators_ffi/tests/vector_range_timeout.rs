/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Timeout propagation for the lazy vector range iterator, in a test binary of its own because
//! [`MockTimeout`] is process-global.

mod common;

use ffi::VecSimQueryReply_Order_BY_ID;
use rqe_iterators::{RQEIterator, RQEIteratorError};
use vector_score_source::test_utils::{MockTimeout, TestIndex, uniform_blob};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn timed_out_range_query_reports_timeout_then_eof() {
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(50.0, 4);
    let _mock = MockTimeout::enable();

    for yields_metric in [false, true] {
        let mut it = common::range_iterator(
            &index,
            &query,
            400.0,
            VecSimQueryReply_Order_BY_ID,
            yields_metric,
        );
        assert!(
            matches!(it.read(), Err(RQEIteratorError::TimedOut)),
            "yields_metric {yields_metric}"
        );
        assert!(
            matches!(it.read(), Ok(None)),
            "yields_metric {yields_metric}"
        );
    }
}
