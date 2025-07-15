/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use inverted_index::{RSAggregateResult, RSAggregateResultIter, RSIndexResult};

#[unsafe(no_mangle)]
pub extern "C" fn Dummy(_ir: *const RSIndexResult) {}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_NumChildren(agg: *const RSAggregateResult) -> usize {
    debug_assert!(!agg.is_null(), "agg must not be null");

    let agg = unsafe { &*agg };

    agg.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_Capacity(agg: *const RSAggregateResult) -> usize {
    debug_assert!(!agg.is_null(), "agg must not be null");

    let agg = unsafe { &*agg };

    agg.capacity()
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_TypeMask(agg: *const RSAggregateResult) -> u32 {
    debug_assert!(!agg.is_null(), "agg must not be null");

    let agg = unsafe { &*agg };

    agg.type_mask().bits()
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_Get(
    agg: *const RSAggregateResult,
    index: usize,
) -> *const RSIndexResult {
    debug_assert!(!agg.is_null(), "agg must not be null");

    let agg = unsafe { &*agg };

    if let Some(next) = agg.get(index) {
        next
    } else {
        std::ptr::null()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_Iter(
    agg: *const RSAggregateResult,
) -> *mut RSAggregateResultIter<'static> {
    debug_assert!(!agg.is_null(), "agg must not be null");

    let agg = unsafe { &*agg };
    let iter = agg.iter();
    let iter_boxed = Box::new(iter);

    Box::into_raw(iter_boxed)
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResultIter_Next(
    iter: *mut RSAggregateResultIter<'static>,
    value: *mut *mut RSIndexResult,
) -> bool {
    debug_assert!(!iter.is_null(), "iter must not be null");
    debug_assert!(!value.is_null(), "value must not be null");

    let iter = unsafe { &mut *iter };

    if let Some(next) = iter.next() {
        unsafe {
            *value = next as *const _ as *mut _;
        }
        true
    } else {
        false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn AggregateResultIter_Free(iter: *mut RSAggregateResultIter<'static>) {
    debug_assert!(!iter.is_null(), "iter must not be null");

    let _boxed_iter = unsafe { Box::from_raw(iter) };
}
