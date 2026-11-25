/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI entry point for iterator profile printing.
//!
//! Rust iterators have their `PrintProfile` vtable entry set at
//! construction time by [`rqe_iterators::interop::RQEIteratorWrapper::boxed_new`].

use std::ptr::NonNull;

use ffi::QueryIterator;
use redis_module::RedisModuleCtx;
use redis_reply::Replier;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{c2rust::call_print_profile, profile_print::ProfilePrintCtx};

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
///    that has been profile-wrapped via
///    [`CRQEIterator::into_profiled`](rqe_iterators::c2rust::CRQEIterator::into_profiled).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Profile_PrintIterators(
    ctx: *mut RedisModuleCtx,
    root: *const QueryIterator,
    limited: bool,
    print_profile_clock: bool,
) {
    let Some(root) = NonNull::new(root.cast_mut()) else {
        return;
    };
    debug_assert_eq!(
        // SAFETY: `root` is a valid `QueryIterator` (precondition 2).
        unsafe { root.as_ref() }.type_,
        IteratorType::Profile,
        "Profile_PrintIterators expects a profile-wrapped iterator tree",
    );

    debug_assert!(!ctx.is_null(), "`ctx` must not be null");
    // SAFETY: precondition 1, which implies `ctx` is non-null.
    let ctx = unsafe { NonNull::new_unchecked(ctx) };
    // SAFETY: precondition 1.
    let mut replier = unsafe { Replier::new(ctx) };
    let mut profile_ctx = ProfilePrintCtx::new(limited, print_profile_clock);
    let mut map = replier.map();
    // SAFETY: precondition 2 (null case handled above). Every iterator's
    // PrintProfile vtable entry is set at construction.
    unsafe { call_print_profile(root, &mut map, &mut profile_ctx) };
}
