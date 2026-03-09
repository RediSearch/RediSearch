/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations of C functions used across integration tests.
//!
//! These are unified mocks that satisfy the linker for all test modules in this
//! crate. Since all tests share a single binary, each mock symbol must be
//! defined exactly once.

use ffi::{RSQueryTerm, RSYieldableMetric};

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any test to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn RSYieldableMetric_Concat(
    _parent: *mut *mut RSYieldableMetric,
    _child: *const RSYieldableMetric,
) {
    // Do nothing since the code will call this
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut RSQueryTerm) {
    // Several tests use stack-allocated RSQueryTerm values, so this must be a
    // no-op rather than panicking on non-null pointers.
}

#[expect(non_snake_case)]
#[unsafe(no_mangle)]
unsafe fn RSYieldableMetrics_Clone(_src: *mut RSYieldableMetric) -> *mut RSYieldableMetric {
    panic!("none of the tests should set any metrics");
}
