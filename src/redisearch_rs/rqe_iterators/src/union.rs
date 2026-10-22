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

use index_result::{
    MetricsVec, RSIndexResult, RSResultKind, RawBorrowedAggregateResult, RawIndexResult,
};
use ref_mode::{Ref, Suspended};
use rqe_core::{DocId, FieldMask};

use crate::{RQEIterator, RQEValidateStatus, ResumeOutcome, composite::repush_borrowed_entries};

pub use crate::union_flat::UnionFlat;
pub use crate::union_heap::UnionHeap;
pub use crate::union_trimmed::UnionTrimmed;

/// Where settling left a union, after its children moved underneath it.
///
/// Both union variants settle through a single decision point, shared by the
/// legacy [`RQEIterator::revalidate`] and the `Box<Self>`
/// [`resume`](crate::RQESuspendedIterator::resume) path, and report the result
/// as one of these three. Neither path matches on them directly:
/// [`into_validate_status`](Self::into_validate_status) and
/// [`into_resume_outcome`](Self::into_resume_outcome) below own the two
/// mappings, so a variant cannot come to mean one thing on one path and
/// something else on the other.
///
/// | `SettleOutcome` | `RQEValidateStatus`         | `ResumeOutcome` |
/// |-----------------|-----------------------------|-----------------|
/// | `Unchanged`     | `Ok`                        | `Ok`            |
/// | `Moved`         | `Moved { current: Some(_) }`| `Moved`         |
/// | `Eof`           | `Moved { current: None }`   | `Moved`         |
///
/// The two disagree on `Eof` for a reason: `RQEValidateStatus` distinguishes
/// "moved, and here is the new current" from "moved, and there is no current",
/// while `ResumeOutcome` carries the iterator itself and lets the caller ask.
///
/// [`RQEIterator::revalidate`]: crate::RQEIterator::revalidate
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
    /// Map onto the legacy [`RQEIterator::revalidate`] outcome, publishing
    /// `result` as the new current where there is one.
    ///
    /// [`RQEIterator::revalidate`]: crate::RQEIterator::revalidate
    pub(crate) const fn into_validate_status<'a, 'index>(
        self,
        result: &'a mut RSIndexResult<'index>,
    ) -> RQEValidateStatus<'a, 'index> {
        match self {
            Self::Unchanged => RQEValidateStatus::Ok,
            Self::Moved => RQEValidateStatus::Moved {
                current: Some(result),
            },
            // At EOF the union publishes no current, so the borrow goes unused.
            Self::Eof => RQEValidateStatus::Moved { current: None },
        }
    }

    /// Map onto the `Box<Self>` [`resume`](crate::RQESuspendedIterator::resume)
    /// outcome, handing `it` back either way.
    ///
    /// `Eof` joins `Moved` here rather than splitting off as it does for
    /// `revalidate`: both leave the union somewhere other than where it was, and
    /// `Eof` has already set `is_eof`, so the caller sees no current from either.
    pub(crate) const fn into_resume_outcome<T>(self, it: T) -> ResumeOutcome<T> {
        match self {
            Self::Unchanged => ResumeOutcome::Ok(it),
            Self::Moved | Self::Eof => ResumeOutcome::Moved(it),
        }
    }
}

/// Puts back the metrics a union set aside before re-settling, once it has
/// settled back onto the document they were gathered for.
///
/// The re-settle rebuilds the aggregate, which clears its metrics and drains the
/// children's again — but the children gave theirs up when the aggregate was
/// first built on this document, so `gathered` is the only record left. In full
/// mode, any metric drained by the rebuild, from a child that only now reached
/// the document, is kept after them.
///
/// A `QUICK_EXIT` union reports a single child's metrics, but the re-settle may
/// pick a different backer than the one that gave up `gathered`, and that
/// sibling still holds its own. Keeping both would publish two children's
/// metrics — a yield key twice, if both score the document — so `gathered`
/// wins, and the new backer's stand in only when it is empty. If the original
/// backer was dropped in the meantime, its metrics are still the ones
/// published: they are the last record of what the union reported for this
/// document.
pub(crate) fn restore_gathered_metrics<'index, const QUICK_EXIT: bool>(
    result: &mut RSIndexResult<'index>,
    mut gathered: MetricsVec<'index>,
) {
    if QUICK_EXIT && !gathered.is_empty() {
        result.metrics = gathered;
        return;
    }
    gathered.concat(&mut result.metrics);
    result.metrics = gathered;
}

/// Whether `result` is a borrowed union aggregate, the only representation a
/// union rebuilds in place.
///
/// [`current`](RQEIterator::current) hands the result out mutably, so a
/// consumer may have replaced it. The exact kind is required: `is_aggregate()`
/// also admits `HybridMetric`, whose children are owned boxes. So is the
/// borrowed representation: an owned `Union` aggregate boxes its children into
/// the result's own allocation, out of the rebuild's reach, so a resume would
/// re-narrow pointers nothing re-validated.
pub(crate) fn result_is_borrowed_union<Rf: Ref>(result: &RawIndexResult<'_, Rf>) -> bool {
    result.kind() == RSResultKind::Union
        && result
            .as_aggregate()
            .is_some_and(|aggregate| aggregate.as_borrowed().is_some())
}

/// Whether the rebuilt aggregate still has a child behind it.
///
/// `#[must_use]` sits on the type so it covers every producer: an
/// [`Unbacked`](Self::Unbacked) that nobody looks at is a result published with
/// nothing backing the document it claims.
#[must_use = "an unbacked aggregate no longer describes a document; the caller \
              must rebuild it or abort the resume"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildOutcome {
    /// At least one survivor sits on the result's document, and the aggregate
    /// now borrows exactly those survivors. The result stands as a description
    /// of that document, and re-narrowing it is sound.
    Backed,
    /// No survivor sits on the result's document, so the aggregate came out
    /// empty. Re-narrowing it is still sound — it points nowhere — but it no
    /// longer describes a document, so the composite must rebuild it before
    /// publishing it or report [`ResumeOutcome::Aborted`] instead.
    Unbacked,
}

/// Rebuild a suspended union's borrowed entries from its just-resumed children.
///
/// [`repush_borrowed_entries`] does the rebuilding and owns its contract; this
/// adds the union's verdict, which needs a single contributor, and skips the
/// union suspended before its first read.
pub(crate) fn rebuild_borrowed_entries<'a, 'child, R, const QUICK_EXIT: bool>(
    aggregate: &mut RawBorrowedAggregateResult<'a, Suspended>,
    doc_id: DocId,
    freq: &mut u32,
    field_mask: &mut FieldMask,
    children: impl IntoIterator<Item = &'child mut R>,
) -> RebuildOutcome
where
    R: RQEIterator<'a> + 'child,
{
    if aggregate.is_empty() {
        // Nothing borrowed, so nothing to rebuild. The union holds an empty
        // aggregate before its first read, after a rewind, and after a skip a
        // child error interrupted; none of them publishes a document, and
        // aborting the last would drop a cursor's remaining results.
        return RebuildOutcome::Backed;
    }

    let contributors =
        repush_borrowed_entries::<_, QUICK_EXIT>(aggregate, doc_id, freq, field_mask, children);
    if contributors == 0 {
        RebuildOutcome::Unbacked
    } else {
        RebuildOutcome::Backed
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
