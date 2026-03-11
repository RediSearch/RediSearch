/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Union iterator implementation.
//!
//! The union iterator yields documents appearing in ANY child iterator (OR semantics).
//!
//! [`UnionFlat`] uses a flat array scan for O(n) min-finding. Best for small
//! numbers of children (typically <20). No heap overhead.
//!
//! The `QUICK_EXIT` const generic controls aggregation behavior:
//! - If `true`, returns after finding the first matching child without aggregating.
//! - If `false`, collects results from all children with the same document.

pub use crate::union_flat::UnionFlat;

// ============================================================================
// Type aliases for convenient access
// ============================================================================

/// Full mode, flat array - aggregates all matching children, O(n) min-finding.
/// This is the most common variant for small numbers of children.
pub type UnionFullFlat<'index, I> = UnionFlat<'index, I, false>;

/// Quick mode, flat array - returns after first match, O(n) min-finding.
pub type UnionQuickFlat<'index, I> = UnionFlat<'index, I, true>;

/// Backwards compatibility alias - defaults to flat full mode.
pub type Union<'index, I> = UnionFullFlat<'index, I>;
