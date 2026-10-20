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

use std::marker::PhantomData;

use index_result::{
    MetricsVec, RSIndexResult, RSResultKind, RawBorrowedAggregateResult, RawIndexResult,
};
use ref_mode::{Ref, Suspended};
use rqe_core::{DocId, FieldMask};

use crate::{RQEIterator, RQEValidateStatus, ResumeOutcome};

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

/// RAII guard owning a suspended union's shell while its children buffer is
/// part-resumed and part-suspended.
///
/// The buffer is three regions during [`resume`](crate::RQESuspendedIterator::resume),
/// and the guard's fields *are* their boundaries: [`kept`](Self::kept) resumed
/// survivors compacted to the front, vacated slots up to
/// [`cursor`](Self::cursor) — moved left by the compaction, or consumed by a
/// child that aborted or failed — and still-suspended children from there to
/// [`len`](Self::len). [`resume`](crate::RQESuspendedIterator::resume) advances the
/// boundaries as it walks, through [`vacate_next`](Self::vacate_next) and
/// [`keep_last_vacated`](Self::keep_last_vacated), so the drop reads the state
/// that actually holds rather than being handed indices that have to agree
/// with it. The fields are private because they decide what the drop frees: a
/// stray write to one would be a double drop, or a drop at the wrong type.
///
/// On drop — an early return or a panic — it drops the prefix as `Resumed`,
/// drops the suspended tail as `Elem`, leaves the vacated middle alone, empties
/// the `Vec` so it frees only its buffer, and drops the shell, which frees
/// whatever else the shell owns along with the allocation.
/// [`disarm`](Self::disarm) it once the buffer is whole again and the cast is
/// about to run.
///
/// `Shell` is the suspended union, `Elem` the element type its children `Vec`
/// is declared with, and `Resumed` the type a resumed survivor is dropped at.
pub(crate) struct FreeSuspendedShell<Shell, Elem, Resumed> {
    /// The shell, still owned here. Came from `Box::into_raw`.
    raw: *mut Shell,
    /// The shell's children `Vec`, projected from [`raw`](Self::raw).
    children: *mut Vec<Elem>,
    /// `children[..kept]` hold resumed survivors.
    kept: usize,
    /// `children[kept..cursor]` are vacated and must not be dropped.
    cursor: usize,
    /// `children[cursor..len]` still hold suspended children.
    len: usize,
    /// The type the prefix must be dropped at. Never materialised, so it adds
    /// no drop glue of its own.
    _resumed: PhantomData<fn() -> Resumed>,
}

impl<Shell, Elem, Resumed> FreeSuspendedShell<Shell, Elem, Resumed> {
    /// Takes ownership of `raw`, with every child still suspended.
    ///
    /// `Resumed` must share `Elem`'s size and alignment, which is checked at
    /// compile time.
    ///
    /// # Safety
    ///
    /// 1. `raw` came from [`Box::into_raw`], and ownership of it passes to the
    ///    guard: nothing else frees it, and nothing reaches it except through
    ///    the guard's boundaries until [`disarm`](Self::disarm).
    /// 2. `children` points to the children `Vec` field inside `*raw`.
    /// 3. Once that `Vec` is emptied, dropping `*raw` as a `Shell` is sound.
    pub(crate) unsafe fn new(raw: *mut Shell, children: *mut Vec<Elem>) -> Self {
        const {
            assert!(size_of::<Resumed>() == size_of::<Elem>());
            assert!(align_of::<Resumed>() == align_of::<Elem>());
        }
        debug_assert!(!raw.is_null(), "the shell must come from a live box");
        debug_assert!(
            {
                let start = raw.addr();
                let end = start + size_of::<Shell>();
                (start..end).contains(&children.addr())
                    && children.addr() + size_of::<Vec<Elem>>() <= end
            },
            "`children` must be a field of the shell",
        );
        // SAFETY: `children` points into the live, exclusively owned `*raw`
        // (preconditions 1 and 2).
        let len = unsafe { (*children).len() };
        Self {
            raw,
            children,
            kept: 0,
            cursor: 0,
            len,
            _resumed: PhantomData,
        }
    }

    /// The number of resumed survivors compacted to the front of the buffer.
    pub(crate) const fn kept(&self) -> usize {
        self.kept
    }

    /// Hands the next still-suspended slot to the caller, and returns its index.
    ///
    /// The slot counts as vacated from here on, so the drop leaves it alone:
    /// call this *before* handing the child to anything that may consume it,
    /// so that an `Err` or a panic in between cannot drop it twice. A slot
    /// vacated and never consumed merely leaks.
    ///
    /// # Panics
    ///
    /// If every slot has already been handed out.
    pub(crate) fn vacate_next(&mut self) -> usize {
        assert!(self.cursor < self.len, "every slot was already vacated");
        let i = self.cursor;
        self.cursor += 1;
        i
    }

    /// Keeps the child in the slot last handed out by
    /// [`vacate_next`](Self::vacate_next) as a resumed survivor, compacting it
    /// down onto the end of the resumed prefix.
    ///
    /// # Safety
    ///
    /// The slot last returned by [`vacate_next`](Self::vacate_next) holds a
    /// valid, owned `Resumed`, not yet kept, whose ownership passes to the
    /// guard.
    pub(crate) unsafe fn keep_last_vacated(&mut self) {
        // `kept < cursor` is what "not yet kept" means: every kept survivor
        // came from a slot vacated before it.
        debug_assert!(
            self.kept < self.cursor,
            "no vacated slot is waiting to be kept: kept={}, cursor={}",
            self.kept,
            self.cursor,
        );
        let src = self.cursor - 1;
        // SAFETY: `children` points into the live, exclusively owned shell
        // (preconditions 1 and 2 of `new`); `as_mut_ptr` reaches the buffer
        // without asserting anything about its elements.
        let base = unsafe { (*self.children).as_mut_ptr() };
        if self.kept < src {
            // SAFETY: `src < cursor <= len`, in bounds of the children buffer.
            let from = unsafe { base.add(src) };
            // SAFETY: `kept < src`, in bounds of the children buffer.
            let to = unsafe { base.add(self.kept) };
            // SAFETY: the slots are distinct and in bounds. Slot `kept` is
            // vacated — its element moved left or was consumed — and slot `src`
            // holds a valid `Resumed` (this function's precondition), so this is
            // a single-element move; `src` stays inside the vacated middle,
            // which the drop leaves alone.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    from.cast::<Resumed>().cast_const(),
                    to.cast::<Resumed>(),
                    1,
                )
            };
        }
        self.kept += 1;
    }

    /// Hands the shell back to the caller without tearing it down.
    pub(crate) const fn disarm(self) {
        std::mem::forget(self);
    }
}

impl<Shell, Elem, Resumed> Drop for FreeSuspendedShell<Shell, Elem, Resumed> {
    fn drop(&mut self) {
        // Catches boundaries that cross, not a `cursor` that lags behind a
        // consumed slot; that double free only shows under miri, via
        // `resume_child_error_frees_a_shell_with_a_suspended_tail`.
        debug_assert!(
            self.kept <= self.cursor && self.cursor <= self.len,
            "region boundaries out of order: kept={}, cursor={}, len={}",
            self.kept,
            self.cursor,
            self.len,
        );
        // SAFETY: the shell is exclusively owned by this guard (precondition 1
        // of `new`) and `children` points into it (precondition 2); the borrow
        // reaches the buffer pointer and the length, and is confined to this
        // function.
        let children: &mut Vec<Elem> = unsafe { &mut *self.children };
        debug_assert!(
            self.len <= children.capacity(),
            "the recorded length outruns the buffer it indexes",
        );
        let base = children.as_mut_ptr();
        for i in 0..self.kept {
            // SAFETY: `i < kept <= len`, in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: the slot holds a resumed survivor; drop it through the
            // resumed type, which shares `Elem`'s size and alignment (checked
            // in `new`).
            unsafe { std::ptr::drop_in_place(slot.cast::<Resumed>()) };
        }
        for i in self.cursor..self.len {
            // SAFETY: `i < len`, in bounds of the children buffer.
            let slot = unsafe { base.add(i) };
            // SAFETY: the slot still holds a valid suspended child.
            unsafe { std::ptr::drop_in_place(slot) };
        }
        // SAFETY: every element has been dropped, moved or consumed; zeroing the
        // length keeps the `Vec` from dropping them again, so it frees only its
        // buffer.
        unsafe { children.set_len(0) };
        // SAFETY: `raw` came from `Box::into_raw` (precondition 1), and with
        // its children emptied it is sound to drop (precondition 3).
        drop(unsafe { Box::from_raw(self.raw) });
    }
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
/// A union's aggregate borrows one entry per child sitting on the union's
/// document, and each entry is a pointer derived from a borrow of that child's
/// result. Transitioning a child reads it out of its slot, passes it by value
/// through its own `resume`, and writes it back: the entry's address still
/// designates the child's result afterwards, but the borrow it was derived from
/// does not survive the round trip — so every entry has to be written afresh,
/// or dropped, before the union's own allocation is re-narrowed. This is the
/// writing-afresh half, for the one resume exit that publishes the aggregate
/// without a settle behind it.
///
/// # Why rebuild rather than re-derive
///
/// The alternative is to keep the entry list and refresh each entry in place,
/// which means recognising which child an entry came from — and an entry
/// records only an address, so the only handle available is "the survivor that
/// still lives at that address". That handle is weak in both directions: it
/// stops identifying anything once a dropped child's slot has been compacted
/// over, and it cannot distinguish the child that moved onto that address from
/// the one that was always there.
///
/// A union does not need the handle. Its contributor set is a *function* of the
/// children — every child whose `current()` sits on the union's document, which
/// is what the read path evaluates when it builds the aggregate — so it can be
/// recomputed from the survivors in one pass, and the entries never have to be
/// identified at all.
///
/// # What is recomputed, and what is carried across
///
/// `freq` and `field_mask` are recomputed. They are copies of what each
/// contributing child holds, so they are functions of the same contributor set
/// as the entries; recomputing them together is what keeps the two from
/// disagreeing. Preserving them across a rebuild that lost a contributor would
/// leave the result claiming a frequency and — worse — a set of fields that no
/// surviving entry accounts for, and `field_mask` decides field filtering and
/// field-weighted scoring.
///
/// `metrics` are carried across untouched, because they are *not* a function of
/// the children: [`RSIndexResult::push_borrowed`] **moves** a child's metrics
/// into the union when the aggregate is first built, leaving the child with
/// none. There is nothing to re-accumulate, so resetting them would discard
/// them for good — a KNN child's `__vector_score` vanishing from the reply. The
/// cost of keeping them is that a metric contributed by a child that has since
/// been dropped stays; the alternative loses the surviving children's metrics
/// too, to remove it. Should the union then re-settle,
/// [`restore_gathered_metrics`] decides which of them it goes on publishing.
///
/// `doc_id` is the document the rebuild is *for*: the input to the contributor
/// test rather than an output of it.
///
/// # Order
///
/// Entries are pushed in the order `children` yields them, and a `QUICK_EXIT`
/// union, which reports a single child rather than aggregating, stops at the
/// first contributor. Which child that is, and the order a full union's entries
/// come back in, are therefore the caller's choice; each union documents its own
/// at the call site.
///
/// # Call it last
///
/// Not merely "before the cast": **after** the child walk, **after** any
/// side-table rebuild, and with no `&mut` taken to a child afterwards. Each
/// entry is derived from a shared reborrow of a `&mut` to its child, so any
/// later mutable access to that child — a `read`, a `skip_to`, another
/// `current`, an `iter_mut` over the children, a `swap_remove`, a sort — pops
/// the tag it carries. `&mut` to the union itself is fine; the entries point
/// into the *children's* allocations.
///
/// A resume that goes on to settle cannot honour that: the settle seeks laggers
/// before it rebuilds the aggregate itself. Such a resume clears the records
/// instead of calling this, so it never holds an entry the settle would leave
/// stale.
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

    // Drops the records and the kind mask; `push_borrowed_ptr_from_ref` rebuilds
    // both, entry by entry. Deliberately not `reset_aggregate`, which would also
    // take `doc_id` and the metrics, which cannot be put back. See
    // `# What is recomputed` above.
    aggregate.reset();
    *freq = 0;
    *field_mask = 0;

    let mut contributors = 0;
    for child in children {
        // A child at EOF publishes no result, and one whose own resume carried
        // it past the union's document no longer describes that document.
        let Some(current) = child.current() else {
            continue;
        };
        if current.doc_id != doc_id {
            continue;
        }
        // The entry is taken from a shared reborrow, so the tag it carries stays
        // valid for as long as nothing takes `&mut` to this child again. The
        // loop only ever moves on to a *different* child, and the union takes
        // none after this call returns.
        let current: &RSIndexResult<'a> = &*current;

        *freq += current.freq;
        *field_mask |= current.field_mask;
        aggregate.push_borrowed_ptr_from_ref(current);
        contributors += 1;

        if QUICK_EXIT {
            break;
        }
    }

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
