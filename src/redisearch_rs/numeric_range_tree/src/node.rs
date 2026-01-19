/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NumericRangeNode - A node in the numeric range tree.

use std::collections::BinaryHeap;
use std::cmp::Reverse;

use ffi::t_docId;
use inverted_index::{IndexReader, RSIndexResult};

use crate::range::NumericRange;
use crate::tree::AddResult;

/// Minimum cardinality threshold before splitting.
const NR_MIN_RANGE_CARD: usize = 16;

/// Maximum cardinality threshold for splits.
const NR_MAX_RANGE_CARD: usize = 2500;

/// Maximum number of entries in a range before forcing a split.
const NR_MAX_RANGE_SIZE: usize = 10000;

/// Last depth at which we use exponentially growing split cardinality.
/// Beyond this depth, we use the max cardinality.
const LAST_DEPTH_OF_NON_MAX_CARD: usize = 3;

/// Maximum depth imbalance before rebalancing.
pub const NR_MAX_DEPTH_BALANCE: i32 = 2;

/// Calculates the split cardinality threshold for a given depth.
///
/// The cardinality grows exponentially with depth:
/// - Depth 0: 16
/// - Depth 1: 64
/// - Depth 2: 256
/// - Depth 3: 1024
/// - Depth 4+: 2500 (capped)
#[inline]
const fn get_split_cardinality(depth: usize) -> usize {
    if depth > LAST_DEPTH_OF_NON_MAX_CARD {
        NR_MAX_RANGE_CARD
    } else {
        NR_MIN_RANGE_CARD << (depth * 2)
    }
}

/// A node in the numeric range tree. Can be either a leaf node (with a range)
/// or an internal node with left/right children.
#[derive(Debug)]
pub struct NumericRangeNode {
    /// Split point value for internal nodes. Values less than this go left.
    pub value: f64,

    /// Maximum depth of the subtree rooted at this node.
    pub max_depth: i32,

    /// Left child (values < split point)
    pub left: Option<Box<NumericRangeNode>>,

    /// Right child (values >= split point)
    pub right: Option<Box<NumericRangeNode>>,

    /// The numeric range data (present in leaf nodes, may be retained in some internal nodes)
    pub range: Option<NumericRange>,
}

impl NumericRangeNode {
    /// Creates a new leaf node.
    #[must_use]
    pub fn new_leaf() -> Self {
        Self {
            value: 0.0,
            max_depth: 0,
            left: None,
            right: None,
            range: Some(NumericRange::new()),
        }
    }

    /// Returns true if this is a leaf node (has no children).
    #[must_use]
    pub const fn is_leaf(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }

    /// Adds a value to this node, potentially splitting it.
    ///
    /// Returns the accumulated result including memory changes and tree modifications.
    pub fn add(
        &mut self,
        doc_id: t_docId,
        value: f64,
        depth: usize,
        max_depth_range: i32,
    ) -> std::io::Result<AddResult> {
        if !self.is_leaf() {
            // Recursively add to the appropriate child
            let child = if value < self.value {
                self.left.as_mut().expect("Internal node must have left child")
            } else {
                self.right.as_mut().expect("Internal node must have right child")
            };

            let mut rv = child.add(doc_id, value, depth + 1, max_depth_range)?;

            // If this inner node retains a range, add the value to it without updating cardinality
            if let Some(ref mut range) = self.range {
                let mem_growth = range.add(doc_id, value)?;
                rv.size_change += mem_growth as i64;
                rv.num_records += 1;
            }

            // Rebalance if needed
            if rv.changed {
                self.balance();

                // Check if we're too high up - remove the range if so
                if self.max_depth > max_depth_range
                    && let Some(removed_range) = self.remove_range()
                {
                    rv.size_change -= removed_range.inverted_index_size() as i64;
                    rv.num_records -= removed_range.num_entries() as i64;
                    rv.num_ranges -= 1;
                }
            }

            Ok(rv)
        } else {
            // Leaf node - add and potentially split
            let range = self.range.as_mut().expect("Leaf node must have a range");

            // Update cardinality and add the value
            range.update_cardinality(value);
            let mem_growth = range.add(doc_id, value)?;

            let mut rv = AddResult {
                size_change: mem_growth as i64,
                num_records: 1,
                changed: false,
                num_ranges: 0,
                num_leaves: 0,
            };

            // Check if we need to split
            let card = range.cardinality();
            let should_split = card >= get_split_cardinality(depth)
                || (range.num_entries() > NR_MAX_RANGE_SIZE && card > 1);

            if should_split {
                self.split(&mut rv)?;

                // Check if we should remove this node's range after splitting
                if self.max_depth > max_depth_range
                    && let Some(removed_range) = self.remove_range()
                {
                    rv.size_change -= removed_range.inverted_index_size() as i64;
                    rv.num_records -= removed_range.num_entries() as i64;
                    rv.num_ranges -= 1;
                }
            }

            Ok(rv)
        }
    }

    /// Splits this leaf node into two children.
    fn split(&mut self, rv: &mut AddResult) -> std::io::Result<()> {
        // Find the median value first (needs immutable borrow)
        let split = self.find_median()?;

        let range = self.range.as_ref().expect("Cannot split node without range");
        let min_val = range.min_val;

        // Adjust split to avoid being the same as min value
        let split = if split == min_val {
            // Get next representable f64 above min
            f64::from_bits(min_val.to_bits().wrapping_add(1))
        } else {
            split
        };

        // Create new leaf children
        let mut left = Box::new(NumericRangeNode::new_leaf());
        let mut right = Box::new(NumericRangeNode::new_leaf());

        rv.size_change += left.range.as_ref().unwrap().inverted_index_size() as i64;
        rv.size_change += right.range.as_ref().unwrap().inverted_index_size() as i64;

        // Read all entries and redistribute using the reader
        let range = self.range.as_ref().expect("Cannot split node without range");
        let mut reader = range.entries.reader();
        let mut record = RSIndexResult::numeric(0.0);

        while reader.next_record(&mut record)? {
            // SAFETY: We know this is a numeric record since we're iterating a numeric index
            let value = unsafe { record.as_numeric_unchecked() };
            let doc_id = record.doc_id;

            let (left_range, right_range) = (
                left.range.as_mut().unwrap(),
                right.range.as_mut().unwrap(),
            );

            if value < split {
                left_range.update_cardinality(value);
                let mem_growth = left_range.add(doc_id, value)?;
                rv.size_change += mem_growth as i64;
            } else {
                right_range.update_cardinality(value);
                let mem_growth = right_range.add(doc_id, value)?;
                rv.size_change += mem_growth as i64;
            }
            rv.num_records += 1;
        }

        // Update this node to be an internal node
        self.max_depth = 1;
        self.value = split;
        self.left = Some(left);
        self.right = Some(right);

        rv.changed = true;
        rv.num_ranges += 2;
        rv.num_leaves += 1; // We split a single leaf into two, adding one net leaf

        Ok(())
    }

    /// Finds the median value in this node's range.
    fn find_median(&self) -> std::io::Result<f64> {
        let range = self.range.as_ref().expect("Cannot find median without range");
        let num_entries = range.num_entries();

        if num_entries == 0 {
            return Ok(0.0);
        }

        let median_idx = num_entries / 2;

        // Use a min-heap of the smaller half to find median
        // We use Reverse to create a max-heap so we can efficiently find the median
        let mut low_half: BinaryHeap<Reverse<OrderedFloat>> =
            BinaryHeap::with_capacity(median_idx);

        // Read entries using the reader
        let mut reader = range.entries.reader();
        let mut record = RSIndexResult::numeric(0.0);
        let mut count = 0;

        while reader.next_record(&mut record)? {
            // SAFETY: We know this is a numeric record
            let value = unsafe { record.as_numeric_unchecked() };

            if count < median_idx {
                low_half.push(Reverse(OrderedFloat(value)));
            } else if let Some(&Reverse(OrderedFloat(max_low))) = low_half.peek()
                && value < max_low
            {
                // Check if current value is smaller than the largest in the low half
                low_half.pop();
                low_half.push(Reverse(OrderedFloat(value)));
            }
            count += 1;
        }

        // The median is the largest value in the low half
        let Reverse(OrderedFloat(median)) = low_half.peek().copied().unwrap_or(Reverse(OrderedFloat(0.0)));
        Ok(median)
    }

    /// Removes and returns this node's range if present.
    pub const fn remove_range(&mut self) -> Option<NumericRange> {
        self.range.take()
    }

    /// Balances the subtree rooted at this node using AVL rotations.
    fn balance(&mut self) {
        let left = self.left.as_ref().expect("Cannot balance without left child");
        let right = self.right.as_ref().expect("Cannot balance without right child");

        let left_depth = left.max_depth;
        let right_depth = right.max_depth;

        if right_depth - left_depth > NR_MAX_DEPTH_BALANCE {
            // Rotate left
            self.rotate_left();
        } else if left_depth - right_depth > NR_MAX_DEPTH_BALANCE {
            // Rotate right
            self.rotate_right();
        }

        // Update max_depth
        self.update_max_depth();
    }

    /// Performs a left rotation.
    fn rotate_left(&mut self) {
        // Take ownership of right child
        let mut right = self.right.take().expect("Cannot rotate left without right child");

        // Move right's left child to our right
        self.right = right.left.take();

        // Update our max_depth
        self.update_max_depth();

        // Swap self with right
        std::mem::swap(self, &mut right);

        // Put old self as left child of new self (which was right)
        self.left = Some(right);

        // Update our new max_depth
        self.update_max_depth();
    }

    /// Performs a right rotation.
    fn rotate_right(&mut self) {
        // Take ownership of left child
        let mut left = self.left.take().expect("Cannot rotate right without left child");

        // Move left's right child to our left
        self.left = left.right.take();

        // Update our max_depth
        self.update_max_depth();

        // Swap self with left
        std::mem::swap(self, &mut left);

        // Put old self as right child of new self (which was left)
        self.right = Some(left);

        // Update our new max_depth
        self.update_max_depth();
    }

    /// Updates max_depth based on children.
    fn update_max_depth(&mut self) {
        let left_depth = self.left.as_ref().map_or(0, |n| n.max_depth);
        let right_depth = self.right.as_ref().map_or(0, |n| n.max_depth);
        self.max_depth = left_depth.max(right_depth) + 1;
    }

    /// Recursively frees this node and its children, returning stats.
    pub fn free(&mut self) -> AddResult {
        let mut rv = AddResult::default();

        if self.is_leaf() {
            rv.num_leaves -= 1;
        }

        if let Some(range) = self.remove_range() {
            rv.size_change -= range.inverted_index_size() as i64;
            rv.num_records -= range.num_entries() as i64;
            rv.num_ranges -= 1;
        }

        if let Some(mut left) = self.left.take() {
            let left_rv = left.free();
            rv.merge(&left_rv);
        }

        if let Some(mut right) = self.right.take() {
            let right_rv = right.free();
            rv.merge(&right_rv);
        }

        rv
    }

    /// Removes empty children from this subtree.
    /// Returns true if this node itself is empty and should be removed.
    pub fn remove_empty_children(&mut self) -> (bool, AddResult) {
        if self.is_leaf() {
            // Check if this leaf is empty
            let is_empty = self.range.as_ref().is_none_or(|r| r.num_docs() == 0);
            return (is_empty, AddResult::default());
        }

        // Process children recursively
        let mut rv = AddResult::default();
        let mut left_empty = false;
        let mut right_empty = false;

        if let Some(ref mut left) = self.left {
            let (empty, left_rv) = left.remove_empty_children();
            left_empty = empty;
            rv.merge(&left_rv);
        }

        if let Some(ref mut right) = self.right {
            let (empty, right_rv) = right.remove_empty_children();
            right_empty = empty;
            rv.merge(&right_rv);
        }

        // If both children are non-empty, just rebalance
        if !left_empty && !right_empty {
            if rv.changed {
                self.balance();
            }
            return (false, rv);
        }

        // Check if this node has data that prevents trimming
        if self.range.as_ref().is_some_and(|r| r.num_docs() != 0) {
            // We have data, can't trim
            return (false, rv);
        }

        rv.changed = true;

        // Replace this node with the non-empty child
        if right_empty {
            // Right is empty, keep left (if any)
            if let Some(mut left) = self.left.take() {
                // Free the right subtree
                if let Some(mut right) = self.right.take() {
                    let free_rv = right.free();
                    rv.merge(&free_rv);
                }
                // Move left's content to self
                self.value = left.value;
                self.max_depth = left.max_depth;
                self.left = left.left.take();
                self.right = left.right.take();
                self.range = left.range.take();
            }
        } else {
            // Left is empty, keep right
            if let Some(mut right) = self.right.take() {
                // Free the left subtree
                if let Some(mut left) = self.left.take() {
                    let free_rv = left.free();
                    rv.merge(&free_rv);
                }
                // Move right's content to self
                self.value = right.value;
                self.max_depth = right.max_depth;
                self.left = right.left.take();
                self.right = right.right.take();
                self.range = right.range.take();
            }
        }

        let is_empty = !left_empty && !right_empty;
        (is_empty, rv)
    }
}

/// Wrapper for f64 that implements Ord for use in BinaryHeap.
#[derive(Clone, Copy, PartialEq)]
struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // This test needs access to the private get_split_cardinality function
    #[test]
    fn test_get_split_cardinality() {
        assert_eq!(get_split_cardinality(0), 16);
        assert_eq!(get_split_cardinality(1), 64);
        assert_eq!(get_split_cardinality(2), 256);
        assert_eq!(get_split_cardinality(3), 1024);
        assert_eq!(get_split_cardinality(4), 2500);
        assert_eq!(get_split_cardinality(5), 2500);
    }
}
