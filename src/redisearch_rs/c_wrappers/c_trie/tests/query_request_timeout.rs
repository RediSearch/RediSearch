/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`QueryRequestTimeoutHandle`], the view of a query
//! request's timeout.
//!
//! Each test builds the C timeout struct directly, with no C call involved, so
//! these also exercise the handle's union reads under Miri.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{mem, ptr};

use c_trie::QueryRequestTimeoutHandle;

const fn timeout_of_kind(kind: ffi::QueryRequestTimeoutKind) -> ffi::QueryRequestTimeout {
    // SAFETY: every field in the C timeout struct accepts an all-zero value.
    let mut timeout: ffi::QueryRequestTimeout = unsafe { mem::zeroed() };
    timeout.kind = kind;
    timeout
}

#[test]
fn clock_deadline_is_reported_for_a_clock_cycle() {
    let mut timeout =
        timeout_of_kind(ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
    timeout.source.clock.deadline = ffi::timespec {
        tv_sec: 12,
        tv_nsec: 34,
    };
    // SAFETY: `timeout` outlives the handle and is not otherwise accessed while it is borrowed.
    let handle = unsafe { QueryRequestTimeoutHandle::from_raw(ptr::from_mut(&mut timeout)) }
        .expect("the timeout pointer is non-null");

    let deadline = handle
        .clock_deadline()
        .expect("a clock cycle has a deadline");

    assert_eq!((deadline.tv_sec, deadline.tv_nsec), (12, 34));
}

#[test]
fn clock_deadline_is_absent_for_other_sources() {
    for kind in [
        ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_UNARMED,
        ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT,
    ] {
        let mut timeout = timeout_of_kind(kind);
        // SAFETY: `timeout` outlives the handle and is not otherwise accessed while it is
        // borrowed.
        let handle = unsafe { QueryRequestTimeoutHandle::from_raw(ptr::from_mut(&mut timeout)) }
            .expect("the timeout pointer is non-null");

        assert!(handle.clock_deadline().is_none(), "kind {kind}");
    }
}
