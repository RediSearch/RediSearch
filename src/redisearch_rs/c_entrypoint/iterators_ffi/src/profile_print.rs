/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI entry point for iterator profile printing and C-only iterator
//! `PrintProfile` vtable implementations.
//!
//! Rust iterators have their `PrintProfile` vtable entry set at
//! construction time by [`rqe_iterators::interop::RQEIteratorWrapper::boxed_new`]. C-only
//! iterator types (Hybrid, Optimus, GeoShape) set theirs to the
//! `extern "C"` functions exported here.

use std::ffi::CStr;

use ffi::QueryIterator;
use rqe_iterators::{c2rust::call_print_profile, profile_print::ProfilePrintCtx};

// ── C-only iterator PrintProfile vtable implementations ─────────────────

/// `PrintProfile` vtable entry for Hybrid (vector search) iterators.
///
/// # Safety
///
/// 1. `self_` must be a valid pointer to a Hybrid iterator.
/// 2. `map` must be a valid pointer to a [`redis_reply::MapBuilder`].
/// 3. `ctx` must be a valid pointer to a [`ProfilePrintCtx`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Hybrid_PrintProfile(
    self_: *const QueryIterator,
    map: *mut redis_reply::MapBuilder,
    ctx: *mut ProfilePrintCtx,
) {
    // SAFETY: precondition 2.
    let map = unsafe { &mut *map };
    // SAFETY: precondition 3.
    let ctx = unsafe { &mut *ctx };

    map.kv_simple_string(c"Type", c"VECTOR");
    ctx.print_optional_counters(map);

    // SAFETY: precondition 1.
    let mode_str = unsafe { ffi::HybridIterator_GetSearchModeString(self_) };
    if !mode_str.is_null() {
        // SAFETY: mode_str is a valid C string (checked non-null).
        let mode_cstr = unsafe { CStr::from_ptr(mode_str) };
        map.kv_simple_string(c"Vector search mode", mode_cstr);
    }
    // SAFETY: precondition 1.
    if unsafe { ffi::HybridIterator_IsBatchMode(self_) } {
        // SAFETY: precondition 1.
        let num_iters = unsafe { ffi::HybridIterator_GetNumIterations(self_) };
        map.kv_long_long(c"Batches number", num_iters as i64);
        // SAFETY: precondition 1.
        let max_batch = unsafe { ffi::HybridIterator_GetMaxBatchSize(self_) };
        map.kv_long_long(c"Largest batch size", max_batch as i64);
        // SAFETY: precondition 1.
        let max_iter = unsafe { ffi::HybridIterator_GetMaxBatchIteration(self_) };
        map.kv_long_long(c"Largest batch iteration (zero based)", max_iter as i64);
    }

    // SAFETY: precondition 1.
    let child = unsafe { ffi::HybridIterator_GetChild(self_) };
    if let Some(child) = std::ptr::NonNull::new(child as *mut _) {
        let mut child_map = map.kv_map(c"Child iterator");
        let mut child_ctx = ctx.child_ctx();
        // SAFETY: child is valid (checked non-null) and its PrintProfile
        // vtable entry is set (it was profile-wrapped by Profile_AddIters).
        unsafe { call_print_profile(child, &mut child_map, &mut child_ctx) };
    }
}

/// `PrintProfile` vtable entry for Optimus (optimizer) iterators.
///
/// # Safety
///
/// 1. `self_` must be a valid pointer to an Optimus iterator.
/// 2. `map` must be a valid pointer to a [`redis_reply::MapBuilder`].
/// 3. `ctx` must be a valid pointer to a [`ProfilePrintCtx`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Optimus_PrintProfile(
    self_: *const QueryIterator,
    map: *mut redis_reply::MapBuilder,
    ctx: *mut ProfilePrintCtx,
) {
    // SAFETY: precondition 2.
    let map = unsafe { &mut *map };
    // SAFETY: precondition 3.
    let ctx = unsafe { &mut *ctx };

    map.kv_simple_string(c"Type", c"OPTIMIZER");
    ctx.print_optional_counters(map);

    // SAFETY: precondition 1.
    let mode_str = unsafe { ffi::OptimizerIterator_GetOptimizationType(self_) };
    if !mode_str.is_null() {
        // SAFETY: mode_str is a valid C string (checked non-null).
        let mode_cstr = unsafe { CStr::from_ptr(mode_str) };
        map.kv_simple_string(c"Optimizer mode", mode_cstr);
    }

    // SAFETY: precondition 1.
    let child = unsafe { ffi::OptimizerIterator_GetChild(self_) };
    if let Some(child) = std::ptr::NonNull::new(child as *mut _) {
        let mut child_map = map.kv_map(c"Child iterator");
        let mut child_ctx = ctx.child_ctx();
        // SAFETY: child is valid (checked non-null) and its PrintProfile
        // vtable entry is set (it was profile-wrapped by Profile_AddIters).
        unsafe { call_print_profile(child, &mut child_map, &mut child_ctx) };
    }
}

/// `PrintProfile` vtable entry for GeoShape iterators.
///
/// # Safety
///
/// 1. `self_` must be a valid pointer to a GeoShape iterator.
/// 2. `map` must be a valid pointer to a [`redis_reply::MapBuilder`].
/// 3. `ctx` must be a valid pointer to a [`ProfilePrintCtx`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoShape_PrintProfile(
    self_: *const QueryIterator,
    map: *mut redis_reply::MapBuilder,
    ctx: *mut ProfilePrintCtx,
) {
    let _ = self_;
    // SAFETY: precondition 2.
    let map = unsafe { &mut *map };
    // SAFETY: precondition 3.
    let ctx = unsafe { &mut *ctx };

    ctx.print_leaf(c"GEO-SHAPE", map);
}
