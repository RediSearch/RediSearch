/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

use inverted_index::{RSAggregateResult, RSIndexResult};

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
