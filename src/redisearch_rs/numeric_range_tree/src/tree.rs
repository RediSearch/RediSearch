/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range tree implementation.

use ffi::t_docId;

use crate::NumericRangeNode;

/// Result of adding a value to the tree.
///
/// This captures the changes that occurred during the add operation,
/// including memory growth and structural changes.
#[derive(Debug, Clone, Copy, Default)]
pub struct AddResult {
    /// The number of bytes the tree's memory usage grew by.
    pub size_delta: i64,
    /// The number of records added.
    pub num_records: i32,
    /// Whether the tree structure changed (splits occurred).
    pub changed: bool,
    /// The change in the number of ranges.
    pub num_ranges_delta: i32,
    /// The change in the number of leaves.
    pub num_leaves_delta: i32,
}

/// A numeric range tree for efficient range queries over numeric values.
///
/// The tree organizes documents by their numeric field values into a balanced
/// binary tree of ranges. Each leaf node contains a range of values, and
/// internal nodes may optionally retain their ranges for query efficiency.
#[derive(Debug)]
pub struct NumericRangeTree {
    /// The root node of the tree.
    root: NumericRangeNode,
    /// Total number of ranges in the tree.
    num_ranges: usize,
    /// Total number of leaf nodes in the tree.
    num_leaves: usize,
    /// Total number of entries (document, value) pairs in the tree.
    num_entries: usize,
    /// Total memory used by inverted indexes in bytes.
    inverted_indexes_size: usize,
    /// The last document ID added to the tree.
    last_doc_id: t_docId,
    /// Revision ID, incremented when the tree structure changes.
    /// Used to invalidate concurrent iterators.
    revision_id: u32,
    /// Unique identifier for this tree instance.
    unique_id: u32,
    /// Number of empty leaves (for GC tracking).
    empty_leaves: usize,
}

/// Global counter for unique tree IDs.
static UNIQUE_ID_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

impl NumericRangeTree {
    /// Create a new empty numeric range tree.
    pub fn new() -> Self {
        let root = NumericRangeNode::new_leaf();
        let inverted_indexes_size = root
            .range()
            .map(|r| r.inverted_index_size())
            .unwrap_or(0);

        Self {
            root,
            num_ranges: 1,
            num_leaves: 1,
            num_entries: 0,
            inverted_indexes_size,
            last_doc_id: 0,
            revision_id: 0,
            unique_id: UNIQUE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            empty_leaves: 0,
        }
    }

    /// Add a (docId, value) pair to the tree.
    ///
    /// If `is_multi` is true, duplicate document IDs are allowed.
    /// Returns information about what changed during the add operation.
    pub fn add(&mut self, doc_id: t_docId, value: f64, is_multi: bool) -> AddResult {
        // Check for duplicate document IDs when not handling multi-values
        if doc_id <= self.last_doc_id && !is_multi {
            return AddResult::default();
        }
        self.last_doc_id = doc_id;

        // TODO: Implement recursive add with splitting and balancing
        // For now, just add to the root's range (stub implementation)
        let size_delta = if let Some(range) = self.root.range_mut() {
            range.add(doc_id, value).unwrap_or(0) as i64
        } else {
            0
        };

        self.num_entries += 1;
        self.inverted_indexes_size = (self.inverted_indexes_size as i64 + size_delta) as usize;

        AddResult {
            size_delta,
            num_records: 1,
            changed: false,
            num_ranges_delta: 0,
            num_leaves_delta: 0,
        }
    }

    /// Get a reference to the root node.
    pub const fn root(&self) -> &NumericRangeNode {
        &self.root
    }

    /// Get a mutable reference to the root node.
    pub fn root_mut(&mut self) -> &mut NumericRangeNode {
        &mut self.root
    }

    /// Get the total number of ranges in the tree.
    pub const fn num_ranges(&self) -> usize {
        self.num_ranges
    }

    /// Get the total number of leaf nodes in the tree.
    pub const fn num_leaves(&self) -> usize {
        self.num_leaves
    }

    /// Get the total number of entries in the tree.
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Get the total memory used by inverted indexes in bytes.
    pub const fn inverted_indexes_size(&self) -> usize {
        self.inverted_indexes_size
    }

    /// Get the last document ID added to the tree.
    pub const fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    /// Get the revision ID of the tree.
    pub const fn revision_id(&self) -> u32 {
        self.revision_id
    }

    /// Get the unique identifier for this tree.
    pub const fn unique_id(&self) -> u32 {
        self.unique_id
    }

    /// Get the number of empty leaves (for GC tracking).
    pub const fn empty_leaves(&self) -> usize {
        self.empty_leaves
    }

    /// Increment the revision ID. Call this when the tree structure changes.
    pub fn increment_revision(&mut self) {
        self.revision_id = self.revision_id.wrapping_add(1);
    }

    /// Calculate the total memory usage of the tree.
    pub fn mem_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let ranges_size = self.num_ranges * std::mem::size_of::<crate::NumericRange>();
        // Our tree is a full binary tree, so #nodes = 2 * #leaves - 1
        let nodes_count = 2 * self.num_leaves.saturating_sub(1) + 1;
        let nodes_size = nodes_count * std::mem::size_of::<NumericRangeNode>();
        // HLL memory: 64 registers per range
        let hll_size = self.num_ranges * 64;

        base_size + self.inverted_indexes_size + ranges_size + nodes_size + hll_size
    }
}

impl Default for NumericRangeTree {
    fn default() -> Self {
        Self::new()
    }
}
