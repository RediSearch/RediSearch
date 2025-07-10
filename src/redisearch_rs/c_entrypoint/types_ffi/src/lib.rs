/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains pure Rust types that we want to expose to C code.

pub use inverted_index::RSIndexResult;
use inverted_index::{RSQueryTerm, t_fieldMask};

// TODO: add safety comments to this file

/// Allocate a new intersection result with a given capacity
#[unsafe(no_mangle)]
pub extern "C" fn NewIntersectResult(cap: usize, weight: f64) -> *mut RSIndexResult {
    let result = RSIndexResult::intersect(0, cap, weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new union result with a given capacity
#[unsafe(no_mangle)]
pub extern "C" fn NewUnionResult(cap: usize, weight: f64) -> *mut RSIndexResult {
    let result = RSIndexResult::union(0, cap, weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new virtual result with a given weight and field mask
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
pub extern "C" fn NewVirtualResult(weight: f64, field_mask: t_fieldMask) -> *mut RSIndexResult {
    let result = RSIndexResult::virt(0, field_mask, weight);
    Box::into_raw(Box::new(result))
}

/// Allocate a new numeric result
#[unsafe(no_mangle)]
pub extern "C" fn NewNumericResult() -> *mut RSIndexResult {
    let result = RSIndexResult::numeric(0, 0.0);
    Box::into_raw(Box::new(result))
}

/// Allocate a new metric result
#[unsafe(no_mangle)]
pub extern "C" fn NewMetricResult() -> *mut RSIndexResult {
    let result = RSIndexResult::metric(0);
    Box::into_raw(Box::new(result))
}

/// Allocate a new hybrid result
#[unsafe(no_mangle)]
pub extern "C" fn NewHybridResult() -> *mut RSIndexResult {
    let result = RSIndexResult::hybrid_metric(0);
    Box::into_raw(Box::new(result))
}

/// Allocate a new token record with a given term and weight
#[unsafe(no_mangle)]
pub extern "C" fn NewTokenRecord(term: *mut RSQueryTerm, weight: f64) -> *mut RSIndexResult {
    let result = RSIndexResult::term(0, term, weight);
    Box::into_raw(Box::new(result))
}

/// Free an index result's internal allocations and also free the result itself
#[unsafe(no_mangle)]
pub extern "C" fn IndexResult_Free(result: *mut RSIndexResult) {
    debug_assert!(!result.is_null(), "result cannot be NULL");

    // SAFETY: caller is to ensure `result` points to a valid RSIndexResult created by one of the
    // constructors
    unsafe {
        let _ = Box::from_raw(result);
    }
}

/// Append a child to an aggregate result
#[unsafe(no_mangle)]
pub extern "C" fn AggregateResult_AddChild(parent: *mut RSIndexResult, child: *mut RSIndexResult) {
    debug_assert!(!parent.is_null(), "parent cannot be NULL");
    debug_assert!(!child.is_null(), "child cannot be NULL");

    // SAFETY: we just checked that `parent` is a valid pointer and the caller is to ensure it is a
    // valid `RSIndexResult`
    let parent = unsafe { &mut *parent };

    // SAFETY: we just checked that `child` is a valid pointer and the caller is to ensure it is a
    // valid `RSIndexResult`
    let child = unsafe { &mut *child };

    parent.push(child);
}

/// Create a deep copy of the results that is totally thread safe. This is very slow so use it with
/// caution
#[unsafe(no_mangle)]
pub extern "C" fn IndexResult_DeepCopy(source: *const RSIndexResult) -> *mut RSIndexResult {
    let source = unsafe { &*source };

    let copy = source.clone();
    let copy = Box::new(copy);

    Box::into_raw(copy)
}
