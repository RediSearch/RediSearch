/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use std::io::Cursor;

use ffi::{DocTable_Exists, RedisSearchCtx};
use hyperloglog::{HyperLogLog6, WyHasher};
use inverted_index::{GcScanDelta, IndexBlock, RSIndexResult};
use numeric_range_tree::{NodeGcDelta, NodeIndex};
use serde::{Deserialize, Serialize};

use crate::{InvertedIndexNumeric, NumericRangeTree};

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
pub unsafe extern "C" fn NumericRangeTree_CompactIfSparse(t: *mut NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller ensures `t` is a valid, non-null pointer
    let NumericRangeTree(tree) = unsafe { &mut *t };

    tree.compact_if_sparse()
}

/// Result of applying GC to a single node in a numeric range tree.
///
/// Returned by [`NumericRangeTree_ApplyGcToNode`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct NumericSingleNodeGcResult {
    /// Number of entries removed from this node.
    pub entries_removed: usize,
    /// Bytes freed from this node.
    pub bytes_freed: usize,
    /// Bytes allocated (for new compacted blocks) in this node.
    pub bytes_allocated: usize,
    /// Blocks ignored in this node.
    pub blocks_ignored: u64,
    /// Whether this node became empty after GC.
    pub became_empty: bool,
}

/// Result of a batch GC scan. Contains the serialized index-tagged deltas.
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
/// index-tagged deltas. The caller should check `has_work` and, if true, send
/// the header followed by the buffer content to the pipe.
///
/// # Wire format per entry (in the buffer)
///
/// Only nodes with actual GC work are written:
/// ```text
/// [node_index: u32 LE][delta_msgpack][64-byte hll_with][64-byte hll_without]
/// ```
///
/// End of buffer is determined by buffer length (already known from header).
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

    scan_batch_dfs(
        tree,
        tree.root_index(),
        &doc_exists,
        &mut buffer,
        &mut any_work,
    );

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
///
/// For each node that has GC work, writes its `NodeIndex` (as `u32` LE)
/// followed by the delta payload. Nodes without work are simply skipped.
fn scan_batch_dfs(
    tree: &numeric_range_tree::NumericRangeTree,
    node_idx: NodeIndex,
    doc_exists: &dyn Fn(ffi::t_docId) -> bool,
    buffer: &mut Vec<u8>,
    any_work: &mut bool,
) {
    let node = tree.node(node_idx);

    // Try to scan this node's range.
    if let Some(range) = node.range()
        && scan_single_node_to_buffer(node_idx, range.entries(), doc_exists, buffer)
    {
        *any_work = true;
    }

    // Recurse into children (pre-order DFS).
    if let Some((left, right)) = node.child_indices() {
        scan_batch_dfs(tree, left, doc_exists, buffer, any_work);
        scan_batch_dfs(tree, right, doc_exists, buffer, any_work);
    }
}

/// Scan a single node's numeric index and write the result to a buffer.
///
/// Returns `true` if GC work was found and written, `false` if not.
///
/// Wire format for each entry:
/// ```text
/// [node_index: u32 LE][delta_msgpack][64-byte hll_with][64-byte hll_without]
/// ```
fn scan_single_node_to_buffer(
    node_idx: NodeIndex,
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

    // Remember position in case serialization fails.
    let start_pos = buffer.len();

    // Write node index as u32 LE.
    buffer.extend_from_slice(&u32::from(node_idx).to_le_bytes());

    // Serialize delta.
    if deltas
        .serialize(&mut rmp_serde::Serializer::new(&mut *buffer))
        .is_err()
    {
        // Remove everything we wrote for this entry.
        buffer.truncate(start_pos);
        return false;
    }

    // Merge majority into last_block to get "with last block" registers.
    last_block_hll.merge(&majority_hll);

    // Write registers: first "with last block", then "without last block".
    buffer.extend_from_slice(last_block_hll.registers());
    buffer.extend_from_slice(majority_hll.registers());

    true
}

// ============================================================================
// NumericGcBatchReader — cursor over pre-read buffer for per-node apply
// ============================================================================

/// Opaque reader that iterates over index-tagged GC entries from a pre-read buffer.
///
/// Created by [`NumericGcBatchReader_New`], advanced by [`NumericGcBatchReader_Next`],
/// and freed by [`NumericGcBatchReader_Free`].
pub struct NumericGcBatchReader {
    cursor: Cursor<Vec<u8>>,
    /// Total length of the buffer.
    total_len: usize,
    /// The most recently read delta (set by `Next`, consumed by `ApplyGcToNode`).
    current_delta: Option<NodeGcDelta>,
    /// The node index from the most recently read entry.
    current_node_index: NodeIndex,
}

/// Create a new [`NumericGcBatchReader`] from a pre-read buffer.
///
/// The reader takes ownership of a copy of the data. The caller retains
/// ownership of the original `data` pointer.
///
/// # Safety
///
/// - `data` must point to a valid byte buffer of at least `len` bytes,
///   or be NULL if `len` is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcBatchReader_New(
    data: *const u8,
    len: usize,
) -> *mut NumericGcBatchReader {
    let slice = if len == 0 || data.is_null() {
        &[]
    } else {
        // SAFETY: Caller ensures `data` is valid for `len` bytes.
        unsafe { std::slice::from_raw_parts(data, len) }
    };

    let reader = Box::new(NumericGcBatchReader {
        cursor: Cursor::new(slice.to_vec()),
        total_len: len,
        current_delta: None,
        current_node_index: NodeIndex::from(0u32),
    });
    Box::into_raw(reader)
}

/// Read the next entry from the batch reader.
///
/// Returns `true` if an entry was read, `false` when the buffer is exhausted.
/// On success, `*node_index` is set to the node's slab index and the delta
/// is stored internally for use by [`NumericRangeTree_ApplyGcToNode`].
///
/// # Safety
///
/// - `reader` must be a valid pointer returned by [`NumericGcBatchReader_New`].
/// - `node_index` must be a valid pointer to a `u32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcBatchReader_Next(
    reader: *mut NumericGcBatchReader,
    node_index: *mut u32,
) -> bool {
    debug_assert!(!reader.is_null(), "reader cannot be NULL");
    debug_assert!(!node_index.is_null(), "node_index cannot be NULL");

    // SAFETY: Caller ensures pointers are valid.
    let reader = unsafe { &mut *reader };

    // Check if we've consumed everything.
    if reader.cursor.position() as usize >= reader.total_len {
        reader.current_delta = None;
        return false;
    }

    // Read node index (u32 LE).
    let mut idx_bytes = [0u8; 4];
    if std::io::Read::read_exact(&mut reader.cursor, &mut idx_bytes).is_err() {
        reader.current_delta = None;
        return false;
    }
    let idx = u32::from_le_bytes(idx_bytes);

    // Read delta (msgpack).
    let mut counting = CountingReader {
        inner: &mut reader.cursor,
        count: 0,
    };
    let delta: GcScanDelta =
        match GcScanDelta::deserialize(&mut rmp_serde::Deserializer::new(&mut counting)) {
            Ok(d) => d,
            Err(_) => {
                reader.current_delta = None;
                return false;
            }
        };

    // Read HLL registers (64 + 64 bytes).
    let mut regs_with = [0u8; 64];
    let mut regs_without = [0u8; 64];
    if std::io::Read::read_exact(&mut reader.cursor, &mut regs_with).is_err() {
        reader.current_delta = None;
        return false;
    }
    if std::io::Read::read_exact(&mut reader.cursor, &mut regs_without).is_err() {
        reader.current_delta = None;
        return false;
    }

    reader.current_node_index = NodeIndex::from(idx);
    reader.current_delta = Some(NodeGcDelta {
        delta,
        registers_with_last_block: regs_with,
        registers_without_last_block: regs_without,
    });

    // SAFETY: Caller ensures node_index is valid.
    unsafe { *node_index = idx };
    true
}

/// Apply the most recently read delta to the given node in the tree.
///
/// The delta is consumed from the reader's internal state (set by the
/// most recent successful [`NumericGcBatchReader_Next`] call).
///
/// # Safety
///
/// - `tree` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
/// - `reader` must be a valid pointer returned by [`NumericGcBatchReader_New`],
///   and [`NumericGcBatchReader_Next`] must have returned `true` immediately before.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_ApplyGcToNode(
    tree: *mut NumericRangeTree,
    reader: *mut NumericGcBatchReader,
) -> NumericSingleNodeGcResult {
    debug_assert!(!tree.is_null(), "tree cannot be NULL");
    debug_assert!(!reader.is_null(), "reader cannot be NULL");

    // SAFETY: Caller ensures pointers are valid.
    let NumericRangeTree(tree) = unsafe { &mut *tree };
    // SAFETY: Caller ensures `reader` is a valid pointer from `NumericGcBatchReader_New`.
    let reader = unsafe { &mut *reader };

    let Some(delta) = reader.current_delta.take() else {
        return NumericSingleNodeGcResult {
            entries_removed: 0,
            bytes_freed: 0,
            bytes_allocated: 0,
            blocks_ignored: 0,
            became_empty: false,
        };
    };

    let result = tree.apply_gc_to_node(reader.current_node_index, delta);

    NumericSingleNodeGcResult {
        entries_removed: result.entries_removed,
        bytes_freed: result.bytes_freed,
        bytes_allocated: result.bytes_allocated,
        blocks_ignored: result.blocks_ignored,
        became_empty: result.became_empty,
    }
}

/// Get the number of bytes consumed so far from the reader's buffer.
///
/// # Safety
///
/// - `reader` must be a valid pointer returned by [`NumericGcBatchReader_New`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcBatchReader_BytesConsumed(
    reader: *const NumericGcBatchReader,
) -> usize {
    debug_assert!(!reader.is_null(), "reader cannot be NULL");

    // SAFETY: Caller ensures pointer is valid.
    let reader = unsafe { &*reader };
    reader.cursor.position() as usize
}

/// Free a [`NumericGcBatchReader`].
///
/// # Safety
///
/// - `reader` must be a valid pointer returned by [`NumericGcBatchReader_New`],
///   or NULL (in which case this is a no-op).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericGcBatchReader_Free(reader: *mut NumericGcBatchReader) {
    if reader.is_null() {
        return;
    }
    // SAFETY: Caller ensures pointer was returned by NumericGcBatchReader_New.
    unsafe {
        let _ = Box::from_raw(reader);
    }
}

/// A reader wrapper that counts bytes read through it.
struct CountingReader<'a, R: std::io::Read> {
    inner: &'a mut R,
    count: usize,
}

impl<R: std::io::Read> std::io::Read for CountingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.count += n;
        Ok(n)
    }
}
