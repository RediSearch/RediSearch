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
//!   providing insertion, lookup, and maintenance operations.
//!
//! # Splitting Strategy
//!
//! Nodes split adaptively based on:
//! - HyperLogLog-estimated cardinality exceeding a depth-dependent threshold
//! - Entry count exceeding a maximum size with cardinality > 1
//!
//! The split point is determined by computing the median value in the range.

mod iter;
mod node;
mod range;
mod tree;

pub use iter::NumericRangeTreeIterator;
pub use node::NumericRangeNode;
pub use range::NumericRange;
pub use tree::{AddResult, NumericRangeTree};
