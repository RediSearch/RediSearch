/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use inverted_index::GcScanDelta;
use numeric_range_tree::{
    CompactIfSparseResult, NodeGcDelta, NodeIndex, NumericRangeTree, SingleNodeGcResult,
};
use serde::Deserialize;

/// Conditionally trim empty leaves and compact the node slab.
///
/// Checks if the number of empty leaves exceeds half the total number of
/// leaves. If so, trims empty leaves, compacts the slab to reclaim freed
/// slots, and returns the number of bytes freed. Returns 0 if no trimming
/// was needed.
///
/// # Safety
///
/// - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - No iterators should be active on this tree while calling this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_CompactIfSparse(
    t: *mut NumericRangeTree,
) -> CompactIfSparseResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller ensures `t` is a valid, non-null pointer
    let tree = unsafe { &mut *t };

    tree.compact_if_sparse()
}

// ============================================================================
// NumericRangeTree_ApplyGcEntry — parse and apply one serialized entry
// ============================================================================

/// Status of a [`NumericRangeTree_ApplyGcEntry`] call.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub enum ApplyGcEntryStatus {
    /// The node was found and GC was applied successfully.
    /// `gc_result` contains the result.
    #[default]
    Ok,
    /// The target node no longer exists in the tree
    /// (e.g. removed between scan and apply).
    NodeNotFound,
    /// The entry data could not be deserialized.
    /// The child probably crashed or corrupted the pipe.
    DeserializationError,
}

/// Result of [`NumericRangeTree_ApplyGcEntry`].
///
/// Wraps [`SingleNodeGcResult`] with a [`status`](ApplyGcEntryStatus) field
/// so C callers can distinguish success, node-not-found, and deserialization
/// errors.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct ApplyGcEntryResult {
    /// The GC result for the node. Only meaningful when `status` is
    /// [`ApplyGcEntryStatus::Ok`].
    pub gc_result: SingleNodeGcResult,
    /// Whether the operation succeeded, the node was missing, or the data
    /// could not be deserialized.
    pub status: ApplyGcEntryStatus,
}

/// Parse a serialized GC entry and apply it to the specified node.
///
/// The entry data must have the wire format produced by the numeric child
/// collector (`fork_gc::numeric::collect_numeric`):
/// ```text
/// [delta_msgpack][64-byte hll_with][64-byte hll_without]
/// ```
///
/// Returns an [`ApplyGcEntryResult`] whose [`status`](ApplyGcEntryStatus)
/// indicates success, node-not-found, or deserialization error.
///
/// # Safety
///
/// - `tree` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - `entry_data` must point to a valid byte buffer of at least `entry_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_ApplyGcEntry(
    tree: *mut NumericRangeTree,
    node_position: u32,
    node_generation: u32,
    entry_data: *const u8,
    entry_len: usize,
) -> ApplyGcEntryResult {
    debug_assert!(!tree.is_null(), "tree cannot be NULL");
    debug_assert!(!entry_data.is_null(), "entry_data cannot be NULL");

    const HLL_REGISTER_SIZE: usize = 64;

    // SAFETY: Caller ensures pointers are valid
    let tree = unsafe { &mut *tree };
    // SAFETY: Caller ensures entry_data is valid for entry_len bytes
    let data = unsafe { std::slice::from_raw_parts(entry_data, entry_len) };

    // The entry format is: [delta_msgpack][64-byte hll_with][64-byte hll_without]
    // We need at least 128 bytes for the two HLL register arrays.
    if data.len() < HLL_REGISTER_SIZE * 2 {
        tracing::warn!(
            node_position = node_position,
            node_generation = node_generation,
            entry_len = entry_len,
            tree_id = %tree.unique_id(),
            "Skipping a malformed GcEntry on the pipe: too short."
        );
        return ApplyGcEntryResult {
            status: ApplyGcEntryStatus::DeserializationError,
            ..Default::default()
        };
    }

    let hll_start = data.len() - HLL_REGISTER_SIZE * 2;
    let msgpack_data = &data[..hll_start];
    let hll_with = &data[hll_start..hll_start + HLL_REGISTER_SIZE];
    let hll_without = &data[hll_start + HLL_REGISTER_SIZE..];

    let delta: GcScanDelta = match GcScanDelta::deserialize(&mut rmp_serde::Deserializer::new(
        &mut std::io::Cursor::new(msgpack_data),
    )) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                node_position = node_position,
                node_generation = node_generation,
                entry_len = entry_len,
                tree_id = %tree.unique_id(),
                error_msg = %e,
                "Skipping a malformed GcScanDelta on the pipe."
            );
            return ApplyGcEntryResult {
                status: ApplyGcEntryStatus::DeserializationError,
                ..Default::default()
            };
        }
    };

    let mut regs_with = [0u8; HLL_REGISTER_SIZE];
    let mut regs_without = [0u8; HLL_REGISTER_SIZE];
    regs_with.copy_from_slice(hll_with);
    regs_without.copy_from_slice(hll_without);

    let node_gc_delta = NodeGcDelta {
        delta,
        registers_with_last_block: regs_with,
        registers_without_last_block: regs_without,
    };

    match tree.apply_gc_to_node(
        NodeIndex::from_raw_parts(node_position, node_generation),
        node_gc_delta,
    ) {
        Some(gc_result) => ApplyGcEntryResult {
            gc_result,
            status: ApplyGcEntryStatus::Ok,
        },
        None => ApplyGcEntryResult {
            status: ApplyGcEntryStatus::NodeNotFound,
            ..Default::default()
        },
    }
}
