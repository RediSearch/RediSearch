/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric Range Tree implementation for RediSearch.
//!
//! This module provides an adaptive binary tree data structure for organizing
//! numeric values into ranges for efficient range queries. It is used for
//! secondary indexing of numeric fields.
//!
//! # Architecture
//!
//! The tree consists of three main components:
//!
//! - [`NumericRange`]: A leaf-level storage unit containing an inverted index
//!   of document IDs and their numeric values, along with a HyperLogLog for
//!   cardinality estimation.
//!
//! - [`NumericRangeNode`]: A binary tree node that can be either an internal
//!   node (with children) or a leaf node (with a range). Internal nodes may
//!   optionally retain their ranges for query efficiency.
//!
//! - [`NumericRangeTree`]: The root container managing the tree structure,
//!   providing insertion, lookup, and maintenance operations. See its
//!   documentation for details on splitting thresholds, balancing, and
//!   other implementation details.
//!
//! # Design Overview
//!
//! ## Adaptive Splitting
//!
//! Since we do not know the distribution of numeric values ahead of time, we use
//! an adaptive splitting approach. The tree starts with a single leaf node, and
//! when a node's estimated cardinality exceeds a depth-dependent threshold, it
//! splits into two children using the median value as the split point.
//!
//! Nodes split based on two conditions:
//! - **Cardinality threshold**: When the estimated distinct value count exceeds
//!   a depth-dependent limit (grows exponentially with depth).
//! - **Size overflow**: When entry count exceeds a maximum, even if cardinality
//!   is low. This handles cases where many documents share few values.
//!
//! ## Cardinality Estimation
//!
//! We use HyperLogLog with 6-bit precision (64 registers) for cardinality estimation.
//! This provides an error rate of approximately `1.04 / sqrt(64) ≈ 13%`, which is
//! acceptable for split decisions while using only 64 bytes per range.
//!
//! ## Range Retention
//!
//! Internal nodes may retain their ranges up to a configurable depth (`max_depth_range`).
//! This allows queries that span multiple leaf ranges to use the parent's aggregated
//! range instead, reducing the number of ranges that need to be unioned during iteration.
//!
//! ## Balancing
//!
//! The tree uses AVL-like rotations to maintain balance.
//!
//! ## Concurrency
//!
//! The tree is designed for single-threaded write access. A `revision_id` is incremented
//! whenever the tree structure changes (splits occur), allowing concurrent read iterators
//! to detect modifications and abort gracefully.

mod arena;
pub mod debug;
mod index;
mod iter;
mod node;
mod range;
mod tree;
mod unique_id;

pub use arena::NodeIndex;
pub use index::{NumericIndex, NumericIndexReader};
pub use inverted_index::NumericFilter;
pub use iter::{IndexedReversePreOrderDfsIterator, ReversePreOrderDfsIterator};
pub use node::{InternalNode, LeafNode, NumericRangeNode};
pub use range::NumericRange;
pub use tree::{
    AddResult, CompactIfSparseResult, NodeGcDelta, NumericRangeTree, SingleNodeGcResult,
    TrimEmptyLeavesResult,
};
pub use unique_id::TreeUniqueId;
