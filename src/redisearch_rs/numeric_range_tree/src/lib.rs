/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! NumericRangeTree - A self-balancing binary search tree with adaptive range bucketing.
//!
//! This module provides a data structure for indexing numeric values with efficient range queries.
//! Each leaf contains a `NumericRange` with min/max boundaries, HLL cardinality estimator, and
//! an inverted index. The tree automatically splits ranges when cardinality exceeds depth-based
//! thresholds and uses AVL-style rotations for balance.
//!
//! # Overview
//!
//! The `NumericRangeTree` is designed for efficient range queries on numeric fields in search
//! indexes. It provides:
//!
//! - Automatic splitting of ranges when they exceed cardinality thresholds
//! - AVL-style balancing to maintain O(log n) lookup times
//! - HyperLogLog-based cardinality estimation for split decisions
//! - Efficient range queries that minimize the number of index scans
//!
//! # Example
//!
//! ```
//! use numeric_range_tree::NumericRangeTree;
//! use inverted_index::NumericFilter;
//!
//! let mut tree = NumericRangeTree::new();
//!
//! // Add some numeric values
//! tree.add(1, 10.0, false);
//! tree.add(2, 20.0, false);
//! tree.add(3, 15.0, false);
//!
//! // Query a range
//! let filter = NumericFilter {
//!     min: 10.0,
//!     max: 20.0,
//!     min_inclusive: true,
//!     max_inclusive: true,
//!     ..Default::default()
//! };
//!
//! let ranges = tree.find(&filter);
//! ```

mod iterator;
mod node;
mod range;
mod tree;

pub use iterator::NumericRangeTreeIterator;
pub use node::NumericRangeNode;
pub use range::NumericRange;
pub use tree::{AddResult, NumericRangeTree};
