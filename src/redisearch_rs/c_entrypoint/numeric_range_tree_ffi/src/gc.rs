/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use ffi::{DocTable_Exists, RedisSearchCtx};
use inverted_index::GcScanDelta;
use numeric_range_tree::{
    CompactIfSparseResult, IndexedReversePreOrderDfsIterator, NodeGcDelta, NodeIndex,
    NumericRangeTree, SingleNodeGcResult,
};
use serde::{Deserialize, Serialize};

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
// NumericGcScanner — streaming, one-node-at-a-time GC scanner
// ============================================================================

/// A single node's GC scan result, returned by [`NumericGcScanner_Next`].
///
/// The `data` pointer points into the scanner's internal buffer and is valid
/// until the next call to [`NumericGcScanner_Next`] or [`NumericGcScanner_Free`].
#[repr(C)]
pub struct NumericGcNodeEntry {
    /// The node's slab position.
    /// The first half of a [`NodeIndex`].
    pub node_position: u32,
    /// The node's slab generation.
    /// The second half of a [`NodeIndex`].
    pub node_generation: u32,
    /// Pointer to the serialized entry data (msgpack delta + HLL registers).
    pub data: *const u8,
    /// Length of the serialized entry data in bytes.
    pub data_len: usize,
}

/// Opaque streaming scanner that yields one node's GC delta at a time.
///
/// Created by [`NumericGcScanner_New`], advanced by [`NumericGcScanner_Next`],
/// and freed by [`NumericGcScanner_Free`].
///
/// Each call to `Next` scans the next node in DFS order via
/// [`NumericRangeNode::scan_gc`][numeric_range_tree::NumericRangeNode::scan_gc]
/// and serializes the delta + HLL registers into an internal buffer.
/// The caller can then write the entry data to the pipe immediately,
/// avoiding buffering all deltas in memory.
pub struct NumericGcScanner<'tree> {
    iter: IndexedReversePreOrderDfsIterator<'tree>,
    doc_exists: Box<dyn Fn(ffi::t_docId) -> bool>,
    /// Reusable buffer for serializing the current entry.
    buffer: Vec<u8>,
}

/// Create a new [`NumericGcScanner`] for streaming GC scans.
///
/// The scanner traverses the tree in pre-order DFS, scanning one node at a
/// time. Call [`NumericGcScanner_Next`] to advance.
///
/// # Safety
///
/// - `sctx` must point to a valid [`RedisSearchCtx`] and cannot be NULL.
/// - `tree` must point to a valid [`NumericRangeTree`] and cannot be NULL.
/// - Both `sctx` and `tree` must remain valid for the lifetime of the scanner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcScanner_New<'tree>(
    sctx: *mut RedisSearchCtx,
    tree: *mut NumericRangeTree,
) -> *mut NumericGcScanner<'tree> {
    debug_assert!(!tree.is_null(), "tree cannot be NULL");
    debug_assert!(!sctx.is_null(), "sctx cannot be NULL");

    // SAFETY: Caller ensures pointers are valid
    let sctx_ref = unsafe { &*sctx };
    debug_assert!(!sctx_ref.spec.is_null(), "sctx.spec cannot be NULL");

    // SAFETY: spec is valid from sctx
    let spec = unsafe { &*sctx_ref.spec };

    // SAFETY: tree is a valid pointer; caller guarantees it outlives the scanner
    let tree_ref = unsafe { &*tree };

    let doc_exists: Box<dyn Fn(ffi::t_docId) -> bool> = Box::new(move |id| {
        // SAFETY: doc_table is valid from spec for the lifetime of the scanner
        unsafe { DocTable_Exists(&spec.docs, id) }
    });

    let scanner = Box::new(NumericGcScanner {
        iter: tree_ref.indexed_iter(),
        doc_exists,
        buffer: Vec::new(),
    });
    Box::into_raw(scanner)
}

/// Advance the scanner to the next node with GC work.
///
/// Scans nodes in DFS order, skipping those without GC work. When a node
/// with work is found, its delta and HLL registers are serialized into the
/// scanner's internal buffer.
///
/// Returns `true` if an entry was produced (and `*entry` is populated),
/// `false` when all nodes have been visited.
///
/// The `entry.data` pointer is valid until the next call to `Next` or `Free`.
///
/// # Wire format for `entry.data`
///
/// ```text
/// [delta_msgpack][64-byte hll_with][64-byte hll_without]
/// ```
///
/// # Safety
///
/// - `scanner` must be a valid pointer returned by [`NumericGcScanner_New`].
/// - `entry` must be a valid pointer to a [`NumericGcNodeEntry`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcScanner_Next(
    scanner: *mut NumericGcScanner,
    entry: *mut NumericGcNodeEntry,
) -> bool {
    debug_assert!(!scanner.is_null(), "scanner cannot be NULL");
    debug_assert!(!entry.is_null(), "entry cannot be NULL");

    // SAFETY: Caller ensures pointers are valid
    let scanner = unsafe { &mut *scanner };

    for (node_idx, node) in scanner.iter.by_ref() {
        let Some(delta) = node.scan_gc(&*scanner.doc_exists) else {
            continue;
        };

        // Serialize into the reusable buffer.
        scanner.buffer.clear();

        if let Err(e) = delta
            .delta
            .serialize(&mut rmp_serde::Serializer::new(&mut scanner.buffer))
        {
            tracing::warn!(
                node_position = node_idx.key().position(),
                node_generation = node_idx.key().generation(),
                error_msg = %e,
                "Failed to serialize a GcScanDelta instance"
            );
            continue;
        }

        scanner
            .buffer
            .extend_from_slice(&delta.registers_with_last_block);
        scanner
            .buffer
            .extend_from_slice(&delta.registers_without_last_block);

        // SAFETY: Caller ensures `entry` is valid
        let entry = unsafe { &mut *entry };
        let key = node_idx.key();
        entry.node_position = key.position();
        entry.node_generation = key.generation();
        entry.data = scanner.buffer.as_ptr();
        entry.data_len = scanner.buffer.len();

        return true;
    }

    false
}

/// Free a [`NumericGcScanner`].
///
/// # Safety
///
/// - `scanner` must be a valid pointer returned by [`NumericGcScanner_New`],
///   or NULL (in which case this is a no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcScanner_Free(scanner: *mut NumericGcScanner) {
    if scanner.is_null() {
        return;
    }
    // SAFETY: Caller ensures pointer was returned by NumericGcScanner_New.
    unsafe {
        let _ = Box::from_raw(scanner);
    }
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
/// The entry data must have the wire format produced by [`NumericGcScanner_Next`]:
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
