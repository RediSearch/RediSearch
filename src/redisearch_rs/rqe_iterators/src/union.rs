/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Union iterator implementations.
//!
//! The union iterator yields documents appearing in ANY child iterator (OR semantics).
//! Two separate types are provided for different performance characteristics:
//!
//! - [`UnionFlat`]: Uses a flat array scan for O(n) min-finding. Best for small
//!   numbers of children (typically <20). No heap overhead.
//!
//! - [`UnionHeap`]: Uses a binary heap for O(log n) min-finding. Better for large
//!   numbers of children (typically >20). Has heap allocation overhead.
//!
//! Both types support a `QUICK_EXIT` const generic:
//! - If `true`, returns after finding the first matching child without aggregating.
//! - If `false`, collects results from all children with the same document.

pub use crate::union_flat::UnionFlat;
pub use crate::union_heap::UnionHeap;

// ============================================================================
// Type aliases for convenient access
// ============================================================================

/// Full mode, flat array - aggregates all matching children, O(n) min-finding.
/// This is the most common variant for small numbers of children.
pub type UnionFullFlat<'index, I> = UnionFlat<'index, I, false>;

/// Quick mode, flat array - returns after first match, O(n) min-finding.
pub type UnionQuickFlat<'index, I> = UnionFlat<'index, I, true>;

/// Full mode, heap - aggregates all matching children, O(log n) min-finding.
/// More efficient for large numbers of children (>20).
pub type UnionFullHeap<'index, I> = UnionHeap<'index, I, false>;

/// Quick mode, heap - returns after first match, O(log n) min-finding.
pub type UnionQuickHeap<'index, I> = UnionHeap<'index, I, true>;

/// Backwards compatibility alias - defaults to flat full mode.
pub type Union<'index, I> = UnionFullFlat<'index, I>;
