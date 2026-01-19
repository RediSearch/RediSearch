/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NumericRangeTree - The main tree structure for numeric indexing.

use std::sync::atomic::{AtomicU32, Ordering};

use ffi::t_docId;
use inverted_index::NumericFilter;

use crate::iterator::NumericRangeTreeIterator;
use crate::node::NumericRangeNode;
use crate::range::NumericRange;

/// Global counter for generating unique tree IDs.
static NUMERIC_TREES_UNIQUE_ID: AtomicU32 = AtomicU32::new(0);

/// Default maximum depth at which inner nodes retain their ranges.
/// Nodes deeper than this will shed their ranges to save memory.
pub const DEFAULT_MAX_DEPTH_RANGE: i32 = 2;

/// Result of an add operation, tracking various statistics.
#[derive(Debug, Default, Clone)]
pub struct AddResult {
    /// Change in memory size (can be negative when removing)
    pub size_change: i64,

    /// Number of records affected
    pub num_records: i64,

    /// Whether the tree structure changed (splits/rotations)
    pub changed: bool,

    /// Change in number of ranges
    pub num_ranges: i64,

    /// Change in number of leaves
    pub num_leaves: i64,
}

impl AddResult {
    /// Merges another result into this one.
    pub const fn merge(&mut self, other: &AddResult) {
        self.size_change += other.size_change;
        self.num_records += other.num_records;
        self.changed |= other.changed;
        self.num_ranges += other.num_ranges;
        self.num_leaves += other.num_leaves;
    }
}

/// A self-balancing binary search tree with adaptive range bucketing for
/// numeric value indexing.
///
/// The tree provides efficient range queries and automatically splits ranges
/// when they exceed cardinality thresholds based on depth.
#[derive(Debug)]
pub struct NumericRangeTree {
    /// Root node of the tree
    root: NumericRangeNode,

    /// Total number of ranges (leaf + retained inner node ranges)
    num_ranges: usize,

    /// Number of leaf nodes
    num_leaves: usize,

    /// Total number of entries across all ranges
    num_entries: usize,

    /// Total memory size of all inverted indexes
    inverted_indexes_size: usize,

    /// Last document ID added (for duplicate detection)
    last_doc_id: t_docId,

    /// Revision ID, incremented when tree structure changes
    revision_id: u32,

    /// Unique ID for this tree instance
    unique_id: u32,

    /// Number of empty leaves (tracked for cleanup)
    empty_leaves: usize,

    /// Maximum depth at which inner nodes retain their ranges
    max_depth_range: i32,
}

impl NumericRangeTree {
    /// Creates a new empty numeric range tree.
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_depth_range(DEFAULT_MAX_DEPTH_RANGE)
    }

    /// Creates a new empty numeric range tree with a custom max depth range.
    #[must_use]
    pub fn with_max_depth_range(max_depth_range: i32) -> Self {
        let root = NumericRangeNode::new_leaf();
        let inverted_indexes_size = root.range.as_ref().map_or(0, |r| r.inverted_index_size());

        Self {
            root,
            num_ranges: 1,
            num_leaves: 1,
            num_entries: 0,
            inverted_indexes_size,
            last_doc_id: 0,
            revision_id: 0,
            unique_id: NUMERIC_TREES_UNIQUE_ID.fetch_add(1, Ordering::Relaxed),
            empty_leaves: 0,
            max_depth_range,
        }
    }

    /// Returns the number of ranges in the tree.
    #[must_use]
    pub const fn num_ranges(&self) -> usize {
        self.num_ranges
    }

    /// Returns the number of leaf nodes.
    #[must_use]
    pub const fn num_leaves(&self) -> usize {
        self.num_leaves
    }

    /// Returns the total number of entries.
    #[must_use]
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Returns the total memory size of all inverted indexes.
    #[must_use]
    pub const fn inverted_indexes_size(&self) -> usize {
        self.inverted_indexes_size
    }

    /// Returns the revision ID.
    #[must_use]
    pub const fn revision_id(&self) -> u32 {
        self.revision_id
    }

    /// Returns the unique ID of this tree.
    #[must_use]
    pub const fn unique_id(&self) -> u32 {
        self.unique_id
    }

    /// Returns the last document ID added.
    #[must_use]
    pub const fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    /// Returns a reference to the root node.
    #[must_use]
    pub const fn root(&self) -> &NumericRangeNode {
        &self.root
    }

    /// Returns the number of empty leaves.
    #[must_use]
    pub const fn empty_leaves(&self) -> usize {
        self.empty_leaves
    }

    /// Adds a value to the tree.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - The document ID
    /// * `value` - The numeric value to add
    /// * `is_multi` - If true, allows duplicate doc IDs (for multi-value fields)
    ///
    /// # Returns
    ///
    /// The result of the add operation, including memory changes and whether
    /// the tree structure changed.
    pub fn add(&mut self, doc_id: t_docId, value: f64, is_multi: bool) -> std::io::Result<AddResult> {
        // Protect against duplicate entries when not handling multi-values
        if doc_id <= self.last_doc_id && !is_multi {
            return Ok(AddResult::default());
        }
        self.last_doc_id = doc_id;

        let rv = self.root.add(doc_id, value, 0, self.max_depth_range)?;

        // Update tree statistics
        if rv.changed {
            self.revision_id += 1;
        }

        self.num_ranges = (self.num_ranges as i64 + rv.num_ranges) as usize;
        self.num_leaves = (self.num_leaves as i64 + rv.num_leaves) as usize;
        self.num_entries += 1;
        self.inverted_indexes_size = (self.inverted_indexes_size as i64 + rv.size_change) as usize;

        Ok(rv)
    }

    /// Finds all ranges that overlap with the given filter.
    ///
    /// Returns a vector of references to NumericRange objects that may contain
    /// matching values. The ranges are returned in the order specified by the
    /// filter's `ascending` flag.
    #[must_use]
    pub fn find(&self, filter: &NumericFilter) -> Vec<&NumericRange> {
        let mut result = Vec::new();
        let mut total = 0;
        Self::find_recursive(&self.root, filter, &mut result, &mut total);
        result
    }

    /// Recursive helper for finding matching ranges.
    fn find_recursive<'a>(
        node: &'a NumericRangeNode,
        filter: &NumericFilter,
        result: &mut Vec<&'a NumericRange>,
        total: &mut usize,
    ) {
        // Check limit
        if filter.limit > 0 && *total >= filter.offset + filter.limit {
            return;
        }

        let min = filter.min;
        let max = filter.max;

        if let Some(ref range) = node.range {
            // If the range is completely contained in the search, we can add it
            if range.is_contained(min, max) {
                let docs = range.num_docs() as usize;
                *total += docs;
                if *total > filter.offset {
                    result.push(range);
                }
                return;
            }

            // No overlap at all - skip
            if !range.overlaps(min, max) {
                return;
            }
        }

        // For non-leaf nodes, descend into children
        if !node.is_leaf() {
            if filter.ascending {
                // Ascending order: left first, then right
                if min <= node.value
                    && let Some(ref left) = node.left
                {
                    Self::find_recursive(left, filter, result, total);
                }
                if max >= node.value
                    && let Some(ref right) = node.right
                {
                    Self::find_recursive(right, filter, result, total);
                }
            } else {
                // Descending order: right first, then left
                if max >= node.value
                    && let Some(ref right) = node.right
                {
                    Self::find_recursive(right, filter, result, total);
                }
                if min <= node.value
                    && let Some(ref left) = node.left
                {
                    Self::find_recursive(left, filter, result, total);
                }
            }
        } else if let Some(ref range) = node.range {
            // Leaf node with overlap
            if range.overlaps(min, max) {
                *total += if *total == 0 && filter.offset == 0 {
                    1
                } else {
                    range.num_docs() as usize
                };
                if *total > filter.offset {
                    result.push(range);
                }
            }
        }
    }

    /// Trims empty leaves from the tree.
    ///
    /// Returns the result of the operation, including memory freed.
    pub fn trim_empty_leaves(&mut self) -> AddResult {
        let (_, rv) = self.root.remove_empty_children();

        if rv.changed {
            self.revision_id += 1;
            self.num_ranges = (self.num_ranges as i64 + rv.num_ranges) as usize;
            self.num_leaves = (self.num_leaves as i64 + rv.num_leaves) as usize;
            self.empty_leaves = (self.empty_leaves as i64 + rv.num_leaves) as usize;
            self.inverted_indexes_size = (self.inverted_indexes_size as i64 + rv.size_change) as usize;
        }

        rv
    }

    /// Returns an iterator over all nodes in the tree.
    #[must_use]
    pub fn iter(&self) -> NumericRangeTreeIterator<'_> {
        NumericRangeTreeIterator::new(self)
    }

    /// Returns the total memory usage of the tree.
    #[must_use]
    pub const fn memory_usage(&self) -> usize {
        use crate::range::NR_REG_SIZE;

        let tree_size = std::mem::size_of::<Self>();
        let range_size = std::mem::size_of::<NumericRange>() + NR_REG_SIZE as usize;
        let node_size = std::mem::size_of::<NumericRangeNode>();

        let mut total = tree_size;
        total += self.inverted_indexes_size;
        total += self.num_ranges * range_size;
        // Our tree is a full binary tree, so #nodes = 2 * #leaves - 1
        total += (2 * self.num_leaves - 1) * node_size;

        total
    }
}

impl Default for NumericRangeTree {
    fn default() -> Self {
        Self::new()
    }
}
