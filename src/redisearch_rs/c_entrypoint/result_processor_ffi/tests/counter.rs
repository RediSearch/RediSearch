/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This file contains tests to ensure the FFI functions behave as expected.

use result_processor_ffi::counter::*;

/// Stub implementation of `RPProfile_IncrementCount` for the linker to not complain when running these tests.
/// This should not be called during these tests.
///
// FIXME: replace with `Profile::increment_count` once the profile result processor is ported.
#[unsafe(no_mangle)]
unsafe extern "C" fn RPProfile_IncrementCount(_r: *mut ffi::ResultProcessor) {
    unreachable!()
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

/// Mock implementation of `DMD_Free` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn DMD_Free(_cmd: *const ffi::RSDocumentMetadata) {
    unreachable!()
}

/// Mock implementation of `RSValue_Decref` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn RSValue_Decref(_v: *mut ffi::RSValue) {
    unreachable!()
}

/// Mock implementation of `SEDestroy` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn SEDestroy(_scr_exp: *mut ffi::RSScoreExplain) {
    unreachable!()
}

/// Mock implementation of `RLookupRow_Wipe` for tests
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Wipe(_row: *mut ffi::RLookupRow) {
    unreachable!()
}
