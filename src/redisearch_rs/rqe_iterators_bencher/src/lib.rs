/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;

pub mod benchers;
pub mod ffi;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(metrics: *mut ::ffi::RSYieldableMetric) {
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any benchmark to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut ::ffi::RSQueryTerm) {
    // RSQueryTerm used by the benchers are created on the stack so we don't need to free them.
}

#[unsafe(no_mangle)]
pub extern "C" fn NewVirtualResult(
    weight: f64,
    field_mask: ::ffi::t_fieldMask,
) -> *mut inverted_index::RSIndexResult<'static> {
    let result = inverted_index::RSIndexResult::virt()
        .field_mask(field_mask)
        .weight(weight);
    Box::into_raw(Box::new(result))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexResult_Free(result: *mut inverted_index::RSIndexResult) {
    debug_assert!(!result.is_null(), "result cannot be NULL");

    // SAFETY: caller is to ensure `result` points to a valid RSIndexResult created by one of the
    // constructors
    let _ = unsafe { Box::from_raw(result) };
}
