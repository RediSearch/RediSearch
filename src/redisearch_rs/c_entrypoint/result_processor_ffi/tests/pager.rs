/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This file contains tests to ensure the FFI functions behave as expected.

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use result_processor_ffi::pager::*;

/// Construct a pager, hand it to `check`, then tear it down through its `Free` VTable entry the
/// way the C pipeline does.
///
/// Every test needs that create/inspect/destroy triple, and leaving the `Free` out would leak, so
/// the unsafe FFI dance lives here once instead of in each test.
fn with_pager(offset: usize, limit: usize, check: impl FnOnce(*mut ffi::ResultProcessor)) {
    // SAFETY: `RPPager_New` returns a freshly heap-allocated processor. We never move out of the
    // returned pointer, and we hand it to `Free` exactly once below, satisfying its contract.
    let pager = unsafe { RPPager_New(offset, limit) };
    assert!(!pager.is_null(), "RPPager_New returned a null pointer");

    check(pager);

    // SAFETY: `pager` is the non-null pointer just returned by `RPPager_New` and has not been
    // freed yet, so its VTable is initialised and readable.
    let free_fn =
        unsafe { (*pager).Free }.expect("Rust result processor must have a free function");

    // SAFETY: `free_fn` is this processor's own destructor, and `pager` has not been freed yet, so
    // calling it exactly once here is sound.
    unsafe { free_fn(pager) };
}

#[test]
fn rp_pager_new_returns_valid_pointer() {
    // The non-null assertion lives in `with_pager`; reaching the closure at all proves it held.
    with_pager(4, 10, |_| {});
}

#[test]
fn rp_pager_new_sets_correct_type() {
    with_pager(4, 10, |pager| {
        // SAFETY: `pager` is a valid, live processor for the duration of this closure.
        let ty = unsafe { (*pager).type_ };
        assert_eq!(
            ty,
            ffi::ResultProcessorType_RP_PAGER_LIMITER,
            "Pager should set type `ffi::ResultProcessorType_RP_PAGER_LIMITER`"
        );
    });
}

#[test]
fn rp_pager_new_sets_vtable_entries() {
    with_pager(0, 10, |pager| {
        // SAFETY: `pager` is a valid, live processor for the duration of this closure.
        let next = unsafe { (*pager).Next };
        // SAFETY: as above.
        let free = unsafe { (*pager).Free };
        assert!(next.is_some(), "Next function should be set");
        assert!(free.is_some(), "Free function should be set");
    });
}

#[test]
fn rp_pager_new_creates_unique_instances() {
    with_pager(0, 10, |first| {
        with_pager(0, 10, |second| {
            assert_ne!(first, second, "Should create unique instances");
        });
    });
}
