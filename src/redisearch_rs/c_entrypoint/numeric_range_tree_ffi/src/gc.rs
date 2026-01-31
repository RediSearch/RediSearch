/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use std::io::Read as _;

use ffi::{DocTable_Exists, RedisSearchCtx};
use hyperloglog::{HyperLogLog6, WyHasher};
use inverted_index::{GcScanDelta, IndexBlock, RSIndexResult};
use numeric_range_tree::{NodeGcDelta, NumericRangeNode};
use serde::{Deserialize, Serialize};

use crate::{InvertedIndexGCReader, InvertedIndexNumeric, NumericRangeTree};

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

/// Result of applying a batch of GC deltas to a numeric range tree.
///
/// Returned by [`NumericRangeTree_ApplyGcBatch`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NumericBatchGcResult {
    /// Total number of entries removed across all nodes.
    pub entries_removed: usize,
    /// Total bytes freed across all nodes.
    pub bytes_freed: usize,
    /// Total bytes allocated (for new compacted blocks) across all nodes.
    pub bytes_allocated: usize,
    /// Total blocks ignored across all nodes.
    pub blocks_ignored: u64,
    /// Number of nodes where GC was actually applied.
    pub nodes_applied: usize,
    /// Number of bytes consumed from the pipe reader.
    ///
    /// The parent uses this together with `batch_len` to skip any remaining
    /// bytes on deserialization error, preventing pipe desync.
    pub bytes_consumed: usize,
}

/// Result of a batch GC scan. Contains the serialized DFS-ordered deltas.
///
/// If `has_work` is false, the buffer is empty and should not be sent.
/// Free with [`NumericGcScanBuffer_Free`].
#[repr(C)]
pub struct NumericGcScanBuffer {
    /// Pointer to the buffer data. NULL if no work.
    pub data: *const u8,
    /// Length of the buffer in bytes.
    pub len: usize,
    /// Whether any node had GC work.
    pub has_work: bool,
    /// Capacity (for deallocation).
    capacity: usize,
}

/// Free a [`NumericGcScanBuffer`] returned by [`NumericRangeTree_GcScanBatch`].
///
/// # Safety
///
/// - `buf` must have been obtained from [`NumericRangeTree_GcScanBatch`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcScanBuffer_Free(buf: NumericGcScanBuffer) {
    if buf.data.is_null() {
        return;
    }
    // SAFETY: Reconstruct the Vec to free the allocation.
    unsafe {
        let _ = Vec::from_raw_parts(buf.data as *mut u8, buf.len, buf.capacity);
    }
}

/// Scan all nodes in a numeric range tree for GC work (child-side batch scan).
///
/// Traverses the tree in pre-order DFS and scans each node's range for deleted
/// documents. Returns a [`NumericGcScanBuffer`] containing the serialized
/// DFS-ordered deltas. The caller should check `has_work` and, if true, send
/// the header followed by the buffer content to the pipe.
///
/// # Wire format per node (in the buffer)
///
/// - `0x00` = skip (no range, or no GC work)
/// - `0x01` = apply, followed by `[delta_msgpack][64-byte hll_with][64-byte hll_without]`
///
/// # Safety
///
/// - `sctx` must point to a valid [`RedisSearchCtx`] and cannot be NULL.
/// - `tree` must point to a valid [`NumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_GcScanBatch(
    sctx: *mut RedisSearchCtx,
    tree: *const NumericRangeTree,
) -> NumericGcScanBuffer {
    debug_assert!(!sctx.is_null(), "sctx cannot be NULL");
    debug_assert!(!tree.is_null(), "tree cannot be NULL");

    // SAFETY: sctx is a valid pointer
    let sctx_ref = unsafe { &*sctx };
    debug_assert!(!sctx_ref.spec.is_null(), "sctx.spec cannot be NULL");

    // SAFETY: spec is valid
    let spec = unsafe { &*sctx_ref.spec };
    let doc_table = spec.docs;

    // SAFETY: doc_table is valid from spec
    let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };

    // SAFETY: tree is a valid pointer
    let NumericRangeTree(tree) = unsafe { &*tree };

    let mut buffer = Vec::new();
    let mut any_work = false;

    scan_batch_dfs(tree.root(), &doc_exists, &mut buffer, &mut any_work);

    if any_work {
        let result = NumericGcScanBuffer {
            data: buffer.as_ptr(),
            len: buffer.len(),
            has_work: true,
            capacity: buffer.capacity(),
        };
        std::mem::forget(buffer);
        result
    } else {
        NumericGcScanBuffer {
            data: std::ptr::null(),
            len: 0,
            has_work: false,
            capacity: 0,
        }
    }
}

/// Recursive DFS helper for batch scanning.
fn scan_batch_dfs(
    node: &NumericRangeNode,
    doc_exists: &dyn Fn(ffi::t_docId) -> bool,
    buffer: &mut Vec<u8>,
    any_work: &mut bool,
) {
    // Try to scan this node's range.
    let wrote = if let Some(range) = node.range() {
        scan_single_node_to_buffer(range.entries(), doc_exists, buffer)
    } else {
        false
    };

    if wrote {
        *any_work = true;
    } else {
        // Write skip marker.
        buffer.push(0x00);
    }

    // Recurse into children (pre-order DFS).
    if let Some((left, right)) = node.children() {
        scan_batch_dfs(left, doc_exists, buffer, any_work);
        scan_batch_dfs(right, doc_exists, buffer, any_work);
    }
}

/// Scan a single node's numeric index and write the result to a buffer.
///
/// Returns `true` if GC work was found and written (0x01 + data), `false` if not.
fn scan_single_node_to_buffer(
    idx: &InvertedIndexNumeric,
    doc_exists: &dyn Fn(ffi::t_docId) -> bool,
    buffer: &mut Vec<u8>,
) -> bool {
    // HLL tracking for cardinality estimation
    let mut majority_hll = HyperLogLog6::<WyHasher>::new();
    let mut last_block_hll = HyperLogLog6::<WyHasher>::new();
    let mut last_block_ptr: *const IndexBlock = std::ptr::null();

    let mut repair_fn = |res: &RSIndexResult<'_>, block: &IndexBlock| {
        let bp = block as *const IndexBlock;
        if bp != last_block_ptr {
            majority_hll.merge(&last_block_hll);
            last_block_hll.clear();
            last_block_ptr = bp;
        }
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

    // Write apply marker.
    buffer.push(0x01);

    // Serialize delta.
    if deltas
        .serialize(&mut rmp_serde::Serializer::new(&mut *buffer))
        .is_err()
    {
        // Remove the 0x01 marker we just wrote.
        buffer.pop();
        return false;
    }

    // Merge majority into last_block to get "with last block" registers.
    last_block_hll.merge(&majority_hll);

    // Write registers: first "with last block", then "without last block".
    buffer.extend_from_slice(last_block_hll.registers());
    buffer.extend_from_slice(majority_hll.registers());

    true
}

/// Apply a batch of GC deltas to a numeric range tree (parent-side).
///
/// Reads the DFS-ordered skip/apply messages from the reader and applies
/// them to the tree. The parent must have the same tree structure as when
/// the child performed the scan (verified by `revision_id` check before
/// calling this function).
///
/// # Wire format per node
///
/// - `0x00` = skip
/// - `0x01` = apply, followed by `[delta_msgpack][64-byte hll_with][64-byte hll_without]`
///
/// # Safety
///
/// - `tree` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - `rd` must point to a valid [`InvertedIndexGCReader`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_ApplyGcBatch(
    tree: *mut NumericRangeTree,
    rd: *mut InvertedIndexGCReader,
) -> NumericBatchGcResult {
    debug_assert!(!tree.is_null(), "tree cannot be NULL");
    debug_assert!(!rd.is_null(), "rd cannot be NULL");

    // SAFETY: tree is a valid pointer
    let NumericRangeTree(tree) = unsafe { &mut *tree };

    // SAFETY: rd is a valid pointer
    let rd = unsafe { &mut *rd };

    // Create an iterator adapter that reads from the pipe.
    let mut pipe_iter = PipeGcDeltaIterator {
        reader: rd,
        bytes_consumed: 0,
    };
    let result = tree.apply_gc_batch(&mut pipe_iter);
    let bytes_consumed = pipe_iter.bytes_consumed();

    NumericBatchGcResult {
        entries_removed: result.total_entries_removed,
        bytes_freed: result.total_bytes_freed,
        bytes_allocated: result.total_bytes_allocated,
        blocks_ignored: result.total_blocks_ignored,
        nodes_applied: result.nodes_applied,
        bytes_consumed,
    }
}

/// Iterator adapter that reads DFS-ordered GC deltas from a pipe reader.
///
/// Tracks the number of bytes consumed from the pipe so the caller can
/// skip any remaining bytes on error, preventing pipe desync.
struct PipeGcDeltaIterator<'a> {
    reader: &'a mut InvertedIndexGCReader,
    bytes_consumed: usize,
}

impl PipeGcDeltaIterator<'_> {
    /// Return the total number of bytes consumed from the pipe reader.
    fn bytes_consumed(&self) -> usize {
        self.bytes_consumed
    }
}

impl Iterator for PipeGcDeltaIterator<'_> {
    type Item = Option<NodeGcDelta>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read the tag byte.
        let mut tag = [0u8; 1];
        if self.reader.read_exact(&mut tag).is_err() {
            return None;
        }
        self.bytes_consumed += 1;

        match tag[0] {
            0x00 => Some(None), // Skip
            0x01 => {
                // Read delta (msgpack), then 64-byte HLL registers x2.
                // We track bytes via a counting reader adapter.
                let mut counting = CountingReader {
                    inner: &mut *self.reader,
                    count: 0,
                };
                let delta: GcScanDelta = match GcScanDelta::deserialize(
                    &mut rmp_serde::Deserializer::new(&mut counting),
                ) {
                    Ok(d) => {
                        self.bytes_consumed += counting.count;
                        d
                    }
                    Err(_) => {
                        self.bytes_consumed += counting.count;
                        return None;
                    }
                };

                let mut regs_with = [0u8; 64];
                let mut regs_without = [0u8; 64];
                if self.reader.read_exact(&mut regs_with).is_err() {
                    return None;
                }
                self.bytes_consumed += 64;
                if self.reader.read_exact(&mut regs_without).is_err() {
                    return None;
                }
                self.bytes_consumed += 64;

                Some(Some(NodeGcDelta {
                    delta,
                    registers_with_last_block: regs_with,
                    registers_without_last_block: regs_without,
                }))
            }
            _ => None, // Unknown tag — signal end of iteration
        }
    }
}

/// A reader wrapper that counts bytes read through it.
struct CountingReader<'a> {
    inner: &'a mut InvertedIndexGCReader,
    count: usize,
}

impl std::io::Read for CountingReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count += n;
        Ok(n)
    }
}
