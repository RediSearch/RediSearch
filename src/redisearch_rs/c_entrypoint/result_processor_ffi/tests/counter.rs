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

use result_processor_ffi::counter::*;

#[test]
fn rp_counter_new_returns_valid_pointer() {
    let counter = unsafe { RPCounter_New() };
    assert!(!counter.is_null(), "Should return non-null pointer");

    unsafe {
        let free_fn = (*counter)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter);
    }
}

#[test]
fn rp_counter_new_sets_correct_type() {
    let counter = unsafe { RPCounter_New() };

    unsafe {
        assert_eq!(
            (*counter).type_,
            ffi::ResultProcessorType_RP_COUNTER,
            "Counter should set type `ffi::ResultProcessorType_RP_COUNTER`"
        );

        let free_fn = (*counter)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter);
    }
}

#[test]
fn rp_counter_new_creates_unique_instances() {
    let counter1 = unsafe { RPCounter_New() };
    let counter2 = unsafe { RPCounter_New() };

    assert_ne!(counter1, counter2, "Should create unique instances");

    unsafe {
        let free_fn = (*counter1)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter1);

        let free_fn = (*counter2)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter2);
    }
}
