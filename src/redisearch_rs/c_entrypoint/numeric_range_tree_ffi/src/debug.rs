/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI wrappers for debug/introspection functions.
//!
//! These functions provide high-level debug output for the FT.DEBUG commands:
//! - NUMIDX_SUMMARY: Tree statistics
//! - DUMP_NUMIDX: Index entries dump
//! - DUMP_NUMIDXTREE: Tree structure dump

use ffi::RedisModuleCtx;
use numeric_range_tree::NumericRangeTree;

/// Reply with a summary of the numeric range tree (for NUMIDX_SUMMARY).
///
/// This outputs the tree statistics in the format expected by FT.DEBUG NUMIDX_SUMMARY.
/// When `t` is NULL (index not yet created), all values are reported as zero.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
/// - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_DebugSummary(
    ctx: *mut RedisModuleCtx,
    t: *const NumericRangeTree,
) {
    debug_assert!(!ctx.is_null(), "ctx cannot be NULL");
    // SAFETY: Caller ensures `t` is either NULL or a valid pointer.
    let tree = unsafe { t.as_ref() };
    // SAFETY: ctx is valid per function docs
    unsafe {
        numeric_range_tree::debug::debug_summary(ctx, tree);
    }
}

/// Reply with a dump of the numeric index entries (for DUMP_NUMIDX).
///
/// This outputs all entries from all ranges in the tree. If `with_headers` is true,
/// each range's entries are prefixed with header information (numDocs, numEntries, etc).
/// When `t` is NULL (index not yet created), an empty array is returned.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
/// - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_DebugDumpIndex(
    ctx: *mut RedisModuleCtx,
    t: *const NumericRangeTree,
    with_headers: bool,
) {
    debug_assert!(!ctx.is_null(), "ctx cannot be NULL");
    // SAFETY: Caller ensures `t` is either NULL or a valid pointer.
    let tree = unsafe { t.as_ref() };
    // SAFETY: ctx is valid per function docs
    unsafe {
        numeric_range_tree::debug::debug_dump_index(ctx, tree, with_headers);
    }
}

/// Reply with a dump of the numeric index tree structure (for DUMP_NUMIDXTREE).
///
/// This outputs the tree structure as a nested map. If `minimal` is true,
/// range entry details are omitted (only tree structure is shown).
/// When `t` is NULL (index not yet created), all values are zero with an empty root.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
/// - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_DebugDumpTree(
    ctx: *mut RedisModuleCtx,
    t: *const NumericRangeTree,
    minimal: bool,
) {
    debug_assert!(!ctx.is_null(), "ctx cannot be NULL");
    // SAFETY: Caller ensures `t` is either NULL or a valid pointer.
    let tree = unsafe { t.as_ref() };
    // SAFETY: ctx is valid per function docs
    unsafe {
        numeric_range_tree::debug::debug_dump_tree(ctx, tree, minimal);
    }
}
