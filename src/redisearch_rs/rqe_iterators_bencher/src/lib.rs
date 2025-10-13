/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ::ffi::{RSValue, RSYieldableMetric, array_clear_func, array_ensure_append_n_func, array_free};
use std::ffi::c_void;

pub mod benchers;
pub mod ffi;

// Need this symbol to be defined for the benchers to run
pub use types_ffi::NewVirtualResult;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ::ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    unsafe {
        array_free(metrics as *mut c_void);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(val: f64) -> *mut RSValue {
    // Allocate the f64 value and cast the pointer
    let mock_value = Box::new(val);
    Box::into_raw(mock_value) as *mut RSValue
}

#[unsafe(no_mangle)]
pub extern "C" fn RSValue_DecrRef() {}

#[unsafe(no_mangle)]
pub extern "C" fn RSYieldableMetric_Concat(
    metrics: *mut *mut RSYieldableMetric,
    new_metric: *const RSYieldableMetric,
) {
    if new_metric.is_null() {
        return;
    }
    unsafe {
        let elem_sz = std::mem::size_of::<RSYieldableMetric>() as u16;
        // array_ensure_append_n_func returns a new array pointer, but we don't need to use it in this mock
        *metrics = array_ensure_append_n_func(
            *metrics as *mut c_void,
            new_metric as *mut c_void,
            1,
            elem_sz,
        ) as *mut RSYieldableMetric;

        // array_clear_func returns a new array pointer, but we don't need to use it in this mock
        let _ = array_clear_func(new_metric as *mut c_void, elem_sz);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut ::ffi::RSQueryTerm) {
    // RSQueryTerm used by the benchers are created on the stack so we don't need to free them.
}
