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

/// Increment the revision ID.
///
/// This method is never needed in production code: the tree
/// revision ID is automatically incremented when the tree structure changes.
///
/// This method is provided primarily for testing purposes—e.g. to force the invalidation
/// of an iterator built on top of this tree in GC tests.
///
/// # Safety
///
/// - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
/// - The caller must have unique access to `t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_IncrementRevisionId(t: *mut NumericRangeTree) -> u32 {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: Caller ensures `t` is valid per function safety docs.
    let NumericRangeTree(tree) = unsafe { &mut *t };
    tree.increment_revision()
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
    u32::from(tree.unique_id())
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
