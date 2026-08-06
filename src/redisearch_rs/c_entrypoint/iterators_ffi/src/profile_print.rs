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
//! iterator types (Hybrid, Optimus) set theirs to the
//! `extern "C"` functions exported here.

use std::{ffi::CStr, ptr::NonNull};

use ffi::QueryIterator;
use redis_module::RedisModuleCtx;
use redis_reply::Replier;
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

// ── FFI entry point ─────────────────────────────────────────────────────

/// Print iterator profile tree as a Redis reply.
///
/// This is the FFI entry point called from C `Profile_PrintCommon`.
///
/// # Parameters
///
/// - `ctx`: The Redis module context used to emit reply protocol.
/// - `root`: The root of the profile-wrapped iterator tree to print.
///   May be null, in which case the function returns immediately.
/// - `limited`: When `true`, non-`UNION` union iterators collapse their
///   children into a summary count instead of printing each child
///   individually. Corresponds to `FT.PROFILE ... LIMITED`.
/// - `print_profile_clock`: When `true`, include wall-clock timing
///   (`"Time"`) in each profile entry. Corresponds to
///   `PROFILE_VERBOSE` / `_FT.DEBUG PROFILE_VERBOSE`.
///
/// # Safety
///
/// 1. `ctx` must be a valid [`RedisModuleCtx`] pointer.
/// 2. `root` must be null or a valid pointer to a [`QueryIterator`] tree
///    that has been profile-wrapped via `Profile_AddIters`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Profile_PrintIterators(
    ctx: *mut RedisModuleCtx,
    root: *const QueryIterator,
    limited: bool,
    print_profile_clock: bool,
) {
    let Some(root) = NonNull::new(root as *mut _) else {
        return;
    };

    // SAFETY: precondition 1.
    let mut replier = unsafe { Replier::new(ctx) };
    let mut profile_ctx = ProfilePrintCtx::new(limited, print_profile_clock);
    let mut map = replier.map();
    // SAFETY: precondition 2 (null case handled above). The PrintProfile
    // vtable entry is set because the tree was profile-wrapped.
    unsafe { call_print_profile(root, &mut map, &mut profile_ctx) };
}
