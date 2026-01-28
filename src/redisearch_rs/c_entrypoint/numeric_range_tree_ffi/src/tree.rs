/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for tree-level accessors and mutations.

use numeric_range_tree::NumericRangeNode;

use crate::NumericRangeTree;

// ============================================================================
// Tree accessor functions (for C code that needs to read tree metadata)
// ============================================================================

/// Get the revision ID of the tree.
///
/// The revision ID changes whenever the tree structure is modified (nodes split, etc.).
/// This is used by iterators to detect concurrent modifications.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetRevisionId(t: *const NumericRangeTree) -> u32 {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.revision_id()
}

/// Get the unique ID of the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetUniqueId(t: *const NumericRangeTree) -> u32 {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.unique_id()
}

/// Get the number of entries in the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetNumEntries(t: *const NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.num_entries()
}

/// Get the number of ranges in the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetNumRanges(t: *const NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.num_ranges()
}

/// Get the number of leaves in the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetNumLeaves(t: *const NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.num_leaves()
}

/// Get the total size of inverted indexes in the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetInvertedIndexesSize(
    t: *const NumericRangeTree,
) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.inverted_indexes_size()
}

/// Get the number of empty leaves in the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetEmptyLeaves(t: *const NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.empty_leaves()
}

/// Get the root node of the tree.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
/// - The returned pointer is valid until the tree is modified or freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GetRoot(
    t: *const NumericRangeTree,
) -> *const NumericRangeNode {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.root() as *const NumericRangeNode
}

// ============================================================================
// Tree mutation functions (for fork GC)
// ============================================================================

/// Subtract from the number of entries in the tree.
///
/// # Safety
///
/// - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_SubtractNumEntries(
    t: *mut NumericRangeTree,
    count: usize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &mut *t };
    tree.subtract_num_entries(count);
}

/// Update the inverted indexes size by a delta (can be negative).
///
/// # Safety
///
/// - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_UpdateInvertedIndexesSize(
    t: *mut NumericRangeTree,
    delta: isize,
) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &mut *t };
    tree.update_inverted_indexes_size(delta);
}

/// Increment the empty leaves counter.
///
/// # Safety
///
/// - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_IncrementEmptyLeaves(t: *mut NumericRangeTree) {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &mut *t };
    tree.increment_empty_leaves();
}
