/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use std::io::Write as _;

use ffi::{DocTable_Exists, RedisSearchCtx};
use hyperloglog::{HyperLogLog6, WyHasher};
use inverted_index::{GcScanDelta, IndexBlock, RSIndexResult};
use numeric_range_tree::NumericRangeNode;
use serde::Serialize;

use crate::{
    InvertedIndexGCCallback, InvertedIndexGCWriter, InvertedIndexNumeric, NumericRangeTree,
};

/// Scan a numeric inverted index for garbage collection.
///
/// This scans the index for deleted documents, computes the GC deltas,
/// and tracks HLL cardinality using Rust closures. If there are deltas,
/// the callback `cb` is called, then the deltas and HLL registers are
/// serialized to the writer `wr`.
///
/// The wire format written is: `[delta_msgpack][regs_with_64b][regs_without_64b]`.
///
/// Returns `true` if GC work was found and written, `false` otherwise.
///
/// # Safety
///
/// - `wr` must point to a valid [`InvertedIndexGCWriter`] and cannot be NULL.
/// - `sctx` must point to a valid [`RedisSearchCtx`] and cannot be NULL.
/// - `idx` must point to a valid [`InvertedIndexNumeric`] and cannot be NULL.
/// - `cb` must point to a valid [`InvertedIndexGCCallback`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_GcDelta_Scan(
    wr: *mut InvertedIndexGCWriter,
    sctx: *mut RedisSearchCtx,
    idx: *const InvertedIndexNumeric,
    cb: *mut InvertedIndexGCCallback,
) -> bool {
    debug_assert!(!wr.is_null(), "wr cannot be NULL");
    debug_assert!(!sctx.is_null(), "sctx cannot be NULL");
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    debug_assert!(!cb.is_null(), "cb cannot be NULL");

    // SAFETY: sctx is a valid pointer
    let sctx_ref = unsafe { &*sctx };
    debug_assert!(!sctx_ref.spec.is_null(), "sctx.spec cannot be NULL");

    // SAFETY: spec is valid
    let spec = unsafe { &*sctx_ref.spec };
    let doc_table = spec.docs;

    // SAFETY: doc_table is valid from spec
    let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };

    // SAFETY: idx is a valid pointer to InvertedIndexNumeric
    let idx = unsafe { &*idx };

    // HLL tracking for cardinality estimation
    let mut majority_hll = HyperLogLog6::<WyHasher>::new();
    let mut last_block_hll = HyperLogLog6::<WyHasher>::new();
    let mut last_block_ptr: *const IndexBlock = std::ptr::null();

    let mut repair_fn = |res: &RSIndexResult<'_>, block: &IndexBlock| {
        let bp = block as *const IndexBlock;
        if bp != last_block_ptr {
            // We are in a new block, merge the last block's cardinality into the majority
            majority_hll.merge(&last_block_hll);
            last_block_hll.clear();
            last_block_ptr = bp;
        }
        // Add the current record to the last block's cardinality
        // SAFETY: We know this is a numeric index result
        let value = unsafe { res.as_numeric_unchecked() };
        last_block_hll.add(value.to_ne_bytes());
    };

    let deltas = match idx {
        InvertedIndexNumeric::Uncompressed(entries) => {
            entries.scan_gc(doc_exists, Some(&mut repair_fn))
        }
        InvertedIndexNumeric::Compressed(entries) => {
            entries.scan_gc(doc_exists, Some(&mut repair_fn))
        }
    };

    let Ok(deltas) = deltas else {
        return false;
    };

    let Some(deltas) = deltas else {
        return false;
    };

    // SAFETY: cb is a valid pointer
    let cb_ref = unsafe { &*cb };
    let cb_call = cb_ref.call;
    cb_call(cb_ref.ctx);

    // SAFETY: wr is a valid pointer
    let wr_ref = unsafe { &mut *wr };

    if deltas
        .serialize(&mut rmp_serde::Serializer::new(&mut *wr_ref))
        .is_err()
    {
        return false;
    }

    // Merge majority into last_block to get "with last block" registers
    // The majority_hll now holds "without last block" registers
    last_block_hll.merge(&majority_hll);

    // Write registers: first "with last block", then "without last block"
    let _ = wr_ref.write_all(last_block_hll.registers());
    let _ = wr_ref.write_all(majority_hll.registers());

    true
}

/// Result of applying GC to a single numeric node.
///
/// Returned by [`NumericRangeTree_ApplyNodeGc`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NumericNodeGcResult {
    /// Number of entries removed from the index.
    pub entries_removed: usize,
    /// Number of bytes freed.
    pub bytes_freed: usize,
    /// Number of bytes allocated (for new compacted blocks).
    pub bytes_allocated: usize,
    /// Number of blocks that were skipped because the index changed since the scan.
    pub blocks_ignored: u64,
    /// Whether the GC was actually applied. `false` if the node had no range.
    pub valid: bool,
}

/// Apply GC deltas to a specific node in a numeric range tree.
///
/// This combines the per-node GC application logic:
/// 1. Gets the mutable range from the node
/// 2. Applies the GC delta to the inverted index
/// 3. Resets cardinality using HLL registers
/// 4. Updates tree statistics (entries, index size, empty leaves)
///
/// The `delta` pointer is consumed by this function - do NOT call
/// `InvertedIndex_GcDelta_Free` on it afterward.
///
/// If the node has no range, returns a result with `valid = false` and
/// the delta is freed.
///
/// # Safety
///
/// - `tree` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - `node` must point to a valid mutable [`NumericRangeNode`] belonging to the tree
///   and cannot be NULL.
/// - `delta` must point to a valid [`GcScanDelta`] created by `InvertedIndex_GcDelta_Read`.
/// - `registers_with_last_block` must point to 64 valid bytes and cannot be NULL.
/// - `registers_without_last_block` must point to 64 valid bytes and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_ApplyNodeGc(
    tree: *mut NumericRangeTree,
    node: *mut NumericRangeNode,
    delta: *mut GcScanDelta,
    registers_with_last_block: *const u8,
    registers_without_last_block: *const u8,
) -> NumericNodeGcResult {
    debug_assert!(!tree.is_null(), "tree cannot be NULL");
    debug_assert!(!node.is_null(), "node cannot be NULL");
    debug_assert!(!delta.is_null(), "delta cannot be NULL");
    debug_assert!(
        !registers_with_last_block.is_null(),
        "registers_with_last_block cannot be NULL"
    );
    debug_assert!(
        !registers_without_last_block.is_null(),
        "registers_without_last_block cannot be NULL"
    );

    // SAFETY: tree is a valid pointer to NumericRangeTree
    let NumericRangeTree(tree) = unsafe { &mut *tree };

    // SAFETY: node is a valid pointer to NumericRangeNode
    let node = unsafe { &mut *node };

    // SAFETY: delta was created by InvertedIndex_GcDelta_Read and is valid
    let delta = unsafe { Box::from_raw(delta) };

    // SAFETY: Caller guarantees `registers_with_last_block` points to 64 valid bytes.
    let regs_with: [u8; 64] =
        unsafe { std::ptr::read(registers_with_last_block as *const [u8; 64]) };
    // SAFETY: Caller guarantees `registers_without_last_block` points to 64 valid bytes.
    let regs_without: [u8; 64] =
        unsafe { std::ptr::read(registers_without_last_block as *const [u8; 64]) };

    let result = tree.apply_node_gc(node, *delta, &regs_with, &regs_without);

    NumericNodeGcResult {
        entries_removed: result.entries_removed,
        bytes_freed: result.bytes_freed,
        bytes_allocated: result.bytes_allocated,
        blocks_ignored: result.blocks_ignored,
        valid: result.valid,
    }
}

/// Conditionally trim empty leaves from a numeric range tree.
///
/// Checks if the number of empty leaves exceeds half the total number of
/// leaves. If so, trims empty leaves and returns the number of bytes freed.
/// Returns 0 if no trimming was needed.
///
/// # Safety
///
/// - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - No iterators should be active on this tree while calling this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_ConditionalTrimEmptyLeaves(
    t: *mut NumericRangeTree,
) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller ensures `t` is a valid, non-null pointer
    let NumericRangeTree(tree) = unsafe { &mut *t };

    tree.conditional_trim_empty_leaves()
}
