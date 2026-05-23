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

use crate::ResumeOutcome;

pub use crate::union_flat::UnionFlat;
pub use crate::union_heap::UnionHeap;
pub use crate::union_trimmed::UnionTrimmed;

/// Where settling left a union, after its children moved underneath it.
///
/// Both union variants settle through a single decision point on the `Box<Self>`
/// [`resume`](crate::RQESuspendedIterator::resume) path and report the result as
/// one of these three, which [`into_resume_outcome`](Self::into_resume_outcome)
/// maps onto a [`ResumeOutcome`]. `Eof` maps to `Moved` like `Moved` does: the
/// outcome carries the iterator itself, so the caller asks it whether a current
/// result remains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettleOutcome {
    /// The union is still on the document it was on, and that document is still backed
    /// by an active child.
    Unchanged,
    /// The union advanced, and its result describes the new position.
    Moved,
    /// The union ran out of documents while settling; it is now at EOF and has no
    /// current result.
    Eof,
}

impl SettleOutcome {
    /// Map onto the `Box<Self>` [`resume`](crate::RQESuspendedIterator::resume)
    /// outcome, handing `it` back either way.
    ///
    /// `Eof` joins `Moved`: both leave the union somewhere other than where it
    /// was, and `Eof` has already set `is_eof`, so the caller sees no current
    /// from either.
    pub(crate) const fn into_resume_outcome<T>(self, it: T) -> ResumeOutcome<T> {
        match self {
            Self::Unchanged => ResumeOutcome::Ok(it),
            Self::Moved | Self::Eof => ResumeOutcome::Moved(it),
        }
    }
}

// ============================================================================
// Type aliases for convenient access
// ============================================================================

/// Full mode, flat array - aggregates all matching children, O(n) min-finding.
pub type UnionFullFlat<'index, I> = UnionFlat<'index, I, false>;

/// Quick mode, flat array - returns after first match, O(n) min-finding.
pub type UnionQuickFlat<'index, I> = UnionFlat<'index, I, true>;

/// Full mode, heap - aggregates all matching children, O(log n) min-finding.
pub type UnionFullHeap<'index, I> = UnionHeap<'index, I, false>;

/// Quick mode, heap - returns after first match, O(log n) min-finding.
pub type UnionQuickHeap<'index, I> = UnionHeap<'index, I, true>;

/// Backwards compatibility alias - defaults to flat full mode.
pub type Union<'index, I> = UnionFullFlat<'index, I>;
