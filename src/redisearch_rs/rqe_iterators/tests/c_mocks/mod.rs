/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains mock implementations of C functions that are used in tests. Linking to a
//! C static file with these implementations would have been a overkill.
//!
//! The integration tests can use these as is if they don't add anything to the metrics or set
//! any of the term record type's internals. Using these only requires the following:
//!
//! ```rust
//! mod c_mocks;
//! ```

use ffi::RSQueryTerm;
use inverted_index::{RSIndexResult, RSTermRecord};

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any test to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn IndexResult_ConcatMetrics(
    _parent: *mut RSIndexResult,
    _child: *const RSIndexResult,
) {
    // Do nothing since the code will call this
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Offset_Data_Free(_tr: *mut RSTermRecord) {
    panic!("Nothing should have copied the term record to require this call");
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(t: *mut RSQueryTerm) {
    if !t.is_null() {
        panic!("No test created a term record");
    }
}
