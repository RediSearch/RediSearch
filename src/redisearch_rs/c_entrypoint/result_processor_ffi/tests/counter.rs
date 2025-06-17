/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use result_processor_ffi::counter::*;
use std::ptr;

/// Mock implementation of `SearchResult_Clear` for tests
///
/// this doesn't actually free anything, so will leak resources but hopefully this is fine for the few Rust
/// tests for now
#[unsafe(no_mangle)]
unsafe extern "C" fn SearchResult_Clear(r: *mut ffi::SearchResult) {
    let r = unsafe { r.as_mut().unwrap() };

    // This won't affect anything if the result is null
    r.score = 0.0;

    // SEDestroy(r->scoreExplain);
    r.scoreExplain = ptr::null_mut();

    // IndexResult_Free(r->indexResult);
    r.indexResult = ptr::null_mut();

    r.flags = 0;
    // RLookupRow_Wipe(&r->rowdata);

    r.dmd = ptr::null();
    //   DMD_Return(r->dmd);
}

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
fn rp_counter_new_initializes_function_pointers() {
    let counter = unsafe { RPCounter_New() };

    unsafe {
        assert!((*counter).Next.is_some(), "Next function should be set");
        assert!((*counter).Free.is_some(), "Free function should be set");

        let free_fn = (*counter)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter);
    }
}

#[test]
fn rp_counter_new_initializes_null_fields() {
    let counter = unsafe { RPCounter_New() };

    unsafe {
        assert!((*counter).parent.is_null(), "Parent should be null");
        assert!((*counter).upstream.is_null(), "Upstream should be null");

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

#[test]
fn rp_counter_new_proper_alignment() {
    let counter = unsafe { RPCounter_New() };
    let alignment = std::mem::align_of::<ffi::ResultProcessor>();

    assert_eq!(
        counter as usize % alignment,
        0,
        "Pointer should be properly aligned"
    );

    unsafe {
        let free_fn = (*counter)
            .Free
            .expect("Rust result processor must have a free function");
        free_fn(counter);
    }
}

#[cfg(not(miri))] // FIXME miri isn't happy about this test, does it allocate new functions for each struct??
#[test]
fn rp_counter_new_function_pointer_consistency() {
    let counter1 = unsafe { RPCounter_New() };
    let counter2 = unsafe { RPCounter_New() };

    assert!(!counter1.is_null());
    assert!(!counter2.is_null());

    unsafe {
        // Function pointers should be the same across instances
        assert_eq!(
            (*counter1).Next,
            (*counter2).Next,
            "Next function pointers should be identical"
        );
        assert_eq!(
            (*counter1).Free,
            (*counter2).Free,
            "Free function pointers should be identical"
        );

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
