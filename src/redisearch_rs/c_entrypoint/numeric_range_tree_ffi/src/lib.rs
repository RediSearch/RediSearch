/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bindings for the Numeric Range Tree.
//!
//! This crate provides C-callable functions to interact with the Rust
//! [`numeric_range_tree`] implementation.
//!
//! The `FT.DEBUG` introspection replies live in [`debug`].
//!
//! # Tree ownership
//!
//! This crate does not create trees. `rqe_iterators::open_numeric_or_geo_index`
//! allocates each [`NumericRangeTree`] as a [`Box`] and stores it on the field
//! spec; C reaches it through `openNumericOrGeoIndex` in `iterators_ffi`.
//! [`NumericRangeTree_Free`] relies on that allocation.

#![expect(
    non_snake_case,
    reason = "exported C symbols and their parameters keep the names C code uses"
)]

pub mod debug;

pub use debug::*;
use numeric_range_tree::AddResult;
use numeric_range_tree::TrimEmptyLeavesResult;

use rqe_core::DocId;
use std::ffi::c_int;

// Re-export core types directly — they are opaque to C code (accessed via pointers).
pub use numeric_range_tree::NumericRangeTree;

// ============================================================================
// Core lifecycle functions
// ============================================================================

/// Add a (docId, value) pair to the tree.
///
/// If `isMulti` is non-zero, duplicate document IDs are allowed.
/// `maxDepthRange` specifies the maximum depth at which to retain ranges on inner nodes.
///
/// Returns information about what changed during the add operation.
///
/// # Safety
///
/// 1. `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
/// 2. The caller must have unique access to `t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn _NumericRangeTree_Add(
    t: *mut NumericRangeTree,
    doc_id: DocId,
    value: f64,
    has_field_expiration: bool,
    isMulti: c_int,
    maxDepthRange: usize,
) -> AddResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: 1, 2.
    let tree = unsafe { t.as_mut() };
    // SAFETY: 1: `t` is non-null, so `as_mut` returned `Some`.
    let tree = unsafe { tree.unwrap_unchecked() };

    tree.add(
        doc_id,
        value,
        has_field_expiration,
        isMulti != 0,
        maxDepthRange,
    )
}

/// Free a [`NumericRangeTree`] and all its contents.
///
/// # Safety
///
/// 1. `t` must be NULL (in which case this is a no-op), or a tree obtained
///    from `openNumericOrGeoIndex`, which allocates it as a [`Box`].
/// 2. After calling this function, `t` must not be used again, and nothing
///    may still borrow from the tree.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Free(t: *mut NumericRangeTree) {
    if t.is_null() {
        return;
    }

    // SAFETY: 1 (`t` is non-null here, so it came from `Box::into_raw`), 2.
    // Reconstructing the Box will free the memory when it's dropped.
    unsafe {
        let _ = Box::from_raw(t);
    };
}

/// Trim empty leaves from the tree (garbage collection).
///
/// Removes leaf nodes that have no documents and prunes the tree structure
/// accordingly.
///
/// # Safety
///
/// 1. `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
/// 2. The caller must have unique access to `t`: nothing may borrow from the
///    tree while this function runs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_TrimEmptyLeaves(
    t: *mut NumericRangeTree,
) -> TrimEmptyLeavesResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    // SAFETY: 1, 2.
    let tree = unsafe { t.as_mut() };
    // SAFETY: 1: `t` is non-null, so `as_mut` returned `Some`.
    let tree = unsafe { tree.unwrap_unchecked() };

    tree.trim_empty_leaves()
}

// ============================================================================
// Size constants for memory overhead calculations
// ============================================================================

/// Get the base size of a NumericRangeTree struct (not including contents).
///
/// This is used for memory overhead calculations.
#[unsafe(no_mangle)]
pub const extern "C" fn NumericRangeTree_BaseSize() -> usize {
    std::mem::size_of::<NumericRangeTree>()
}
