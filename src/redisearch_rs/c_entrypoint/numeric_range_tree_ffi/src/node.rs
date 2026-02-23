/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for accessing numeric range tree nodes.

use numeric_range_tree::{NumericRange, NumericRangeNode};

/// Get the [`NumericRange`] from a node, if present.
///
/// Returns a pointer to the range, or NULL if the node has no range
/// (e.g., an internal node whose range has been trimmed).
///
/// The returned pointer is valid until the tree is modified or freed.
/// Do NOT free the returned pointer - it points to memory owned by the tree.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `node` must point to a valid [`NumericRangeNode`] obtained from
///   [`crate::iterator::NumericRangeTreeIterator_Next`] and cannot be NULL.
/// - The tree from which this node came must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeNode_GetRange(
    node: *const NumericRangeNode,
) -> *const NumericRange {
    debug_assert!(!node.is_null(), "node cannot be NULL");

    // SAFETY: Caller is to ensure that `node` is a valid, non-null pointer
    // to a NumericRangeNode obtained from NumericRangeTreeIterator_Next.
    let node = unsafe { &*node };

    match node.range() {
        Some(range) => range as *const NumericRange,
        None => std::ptr::null(),
    }
}
