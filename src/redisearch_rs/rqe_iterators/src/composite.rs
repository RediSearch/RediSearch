/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Machinery shared by composite iterators — the unions and the intersection: the
//! composition of the score-bound hooks, and the `Box<Self>`
//! [`resume`](crate::RQESuspendedIterator::resume), which transitions a `Vec` of children in
//! place and re-narrows an aggregate that borrows from them.

use std::marker::PhantomData;

use index_result::{RSIndexResult, RawBorrowedAggregateResult};
use ref_mode::Suspended;
use rqe_core::{DocId, FieldMask};

use crate::{
    RQEIterator, RQEIteratorError,
    score_bound::{self, BlockBound, MinCompetitiveScore},
};

/// [`RQEIterator::current_max_score`] of a composite whose score is `weight` times the sum of
/// the scores of `children`.
///
/// Scaling by `max(weight, 1)` rather than `weight` keeps the bound valid for scorers that ignore
/// the query tree (`DOCSCORE`), for which every child's bound is already a bound on the whole
/// score.
pub(crate) fn summed_max_score<'index, 'c, I>(
    children: impl IntoIterator<Item = &'c I>,
    weight: f64,
) -> f64
where
    I: RQEIterator<'index> + 'c,
{
    let sum: f64 = children
        .into_iter()
        .map(|child| child.current_max_score())
        .sum();
    sum * weight.max(1.0)
}

/// [`RQEIterator::current_block_bound`] of a composite whose score is `weight` times the sum of
/// the scores of `children`, scaled as in [`summed_max_score`]: `None` unless there is a child
/// and every one reports a block.
pub(crate) fn summed_block_bound<'index, 'c, I>(
    children: impl IntoIterator<Item = &'c I>,
    weight: f64,
) -> Option<BlockBound>
where
    I: RQEIterator<'index> + 'c,
{
    let mut window: Option<BlockBound> = None;
    for child in children {
        let block = child.current_block_bound()?;
        window = Some(match window {
            None => block,
            Some(window) => BlockBound {
                last_doc_id: window.last_doc_id.min(block.last_doc_id),
                max_score: window.max_score + block.max_score,
            },
        });
    }
    window.map(|window| BlockBound {
        max_score: window.max_score * weight.max(1.0),
        ..window
    })
}

/// A union whose children [`union_read_with_threshold`] inspects to choose its next position.
pub(crate) trait PivotingUnion<'index>: RQEIterator<'index> {
    /// The type of the union's children.
    type Child: RQEIterator<'index>;

    /// Every child that can still yield.
    fn live_children<'a>(&'a self) -> impl Iterator<Item = &'a Self::Child>
    where
        Self::Child: 'a;

    /// The weight that scales the union's score, applied as in [`summed_max_score`].
    fn score_weight(&self) -> f64;
}

/// Where a thresholded union read moves next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PivotStep {
    /// The next document may be competitive: move with a plain [`read`](RQEIterator::read).
    Read,
    /// No document before the target can reach the threshold.
    SkipTo(DocId),
}

/// Chooses where `union`, searching from `start`, moves next, Block-Max WAND style, from its
/// children's block bounds alone.
///
/// A child can first match at its `from`: its position, or `start` when it stands earlier (a
/// quick-exit union returns on the first child that matches, leaving the others behind or not
/// yet positioned). A document `d` is matched only by children whose `from` is at most `d`, so
/// its score is bounded by the sum of their block maxima, as long as `d` lies inside every one of
/// their blocks. A child's block bounds it from `from` only when the block reaches `from`;
/// otherwise the child is unbounded.
///
/// The walk visits the children's `from`s in increasing order, adding each child once, when its
/// `from` is reached. The pivot is the first `from` at which the sum reaches the threshold: no
/// document before it can. The sum only covers documents up to the earliest block end among the
/// children added so far, and a `from` past it ends the walk: past that end the earlier children's
/// next blocks are unknown, so the skip stops there.
///
/// Each round rescans the children for the next `from`, so a step costs the number of children
/// times the number of `from`s visited; in exchange it needs no buffer and no sort.
fn pivot_step<'index, U>(
    union: &U,
    start: DocId,
    min_competitive_score: MinCompetitiveScore,
) -> PivotStep
where
    U: PivotingUnion<'index>,
{
    let scale = union.score_weight().max(1.0);
    let mut sum = 0.0;
    let mut window_end = DocId::MAX;
    // Children whose `from` is in `added_through + 1..=at` are added this round. `start` is
    // always past a position, so it is at least 1.
    let mut added_through = start - 1;
    let mut at = start;
    loop {
        let mut next_from = None;
        for child in union.live_children() {
            let from = child.last_doc_id().max(start);
            if from > at {
                next_from = Some(next_from.map_or(from, |next: DocId| next.min(from)));
            } else if from > added_through {
                match child.current_block_bound() {
                    Some(block) if block.last_doc_id >= from => {
                        sum += block.max_score;
                        window_end = window_end.min(block.last_doc_id);
                    }
                    _ => sum = f64::INFINITY,
                }
            }
        }
        if !score_bound::can_drop(sum * scale, min_competitive_score) {
            return if at > start {
                PivotStep::SkipTo(at)
            } else {
                PivotStep::Read
            };
        }
        match next_from {
            Some(next) if next <= window_end => {
                added_through = at;
                at = next;
            }
            _ => break,
        }
    }
    // Nothing up to the window's end can reach the threshold. A window that never closed (no
    // live child) or that closes at the last id leaves nothing to skip to, and a plain read then
    // finds what follows.
    window_end
        .checked_add(1)
        .map_or(PivotStep::Read, PivotStep::SkipTo)
}

/// [`RQEIterator::read_with_threshold`] of a union that yields in doc-id order, built on its own
/// [`read`](RQEIterator::read) and [`skip_to`](RQEIterator::skip_to).
///
/// Before each move it chooses a pivot from its children's block bounds ([`pivot_step`]) and
/// skips straight to it, so the children behind the pivot jump over documents that cannot reach
/// the threshold without reading them. A document it lands on whose own bound cannot reach the
/// threshold is dropped.
pub(crate) fn union_read_with_threshold<'index, U>(
    union: &mut U,
    min_competitive_score: MinCompetitiveScore,
) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>
where
    U: PivotingUnion<'index>,
{
    loop {
        if union.at_eof() {
            return Ok(None);
        }
        let step = match union.last_doc_id().checked_add(1) {
            None => PivotStep::Read,
            Some(start) => pivot_step(union, start, min_competitive_score),
        };
        let found = match step {
            PivotStep::SkipTo(target) => union.skip_to(target)?.is_some(),
            PivotStep::Read => union.read()?.is_some(),
        };
        if !found {
            return Ok(None);
        }
        if !score_bound::can_drop(union.current_max_score(), min_competitive_score) {
            return Ok(union.current());
        }
    }
}

/// [`RQEIterator::reports_score_bounds`] of a composite that sums the scores of `children`: only
/// when there is a child and every child reports bounds.
pub(crate) fn all_report_score_bounds<'index, 'c, I>(
    children: impl IntoIterator<Item = &'c I>,
) -> bool
where
    I: RQEIterator<'index> + 'c,
{
    let mut children = children.into_iter().peekable();
    children.peek().is_some() && children.all(|child| child.reports_score_bounds())
}

/// RAII guard owning a suspended composite's shell while its children buffer
/// is part-resumed and part-suspended.
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
/// `Shell` is the suspended composite, `Elem` the element type its children
/// `Vec` is declared with, and `Resumed` the type a resumed survivor is dropped
/// at.
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
    ///    guard: until [`disarm`](Self::disarm), nothing else frees it, nothing
    ///    reaches the children `Vec` except through the guard's boundaries, and
    ///    no reference into `*raw` is used once the guard has dropped. The
    ///    shell's other fields may still be accessed through `raw` meanwhile.
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

/// Clear `aggregate` and push back one entry per child sitting on `doc_id`,
/// returning how many were pushed.
///
/// A composite's aggregate borrows one entry per child sitting on the
/// composite's document, and each entry is a pointer derived from a borrow of
/// that child's result. Transitioning a child reads it out of its slot, passes
/// it by value through its own `resume`, and writes it back: the entry's address
/// still designates the child's result afterwards, but the borrow it was
/// derived from does not survive the round trip — so every entry has to be
/// written afresh, or dropped, before the composite's own allocation is
/// re-narrowed. This is the writing-afresh half, for a resume exit that
/// publishes the aggregate without a re-seek behind it.
///
/// Each composite turns the returned count into its own verdict: a union needs
/// one contributor, an intersection needs every child.
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
/// A composite does not need the handle. Its contributor set is a *function* of
/// the children — every child whose `current()` sits on the composite's
/// document, which is what the read path evaluates when it builds the
/// aggregate — so it can be recomputed from the survivors in one pass, and the
/// entries never have to be identified at all.
///
/// # What is recomputed, and what is carried across
///
/// [`freq`] and [`field_mask`] are recomputed. They are copies of what each
/// contributing child holds, so they are functions of the same contributor set
/// as the entries; recomputing them together is what keeps the two from
/// disagreeing. Preserving them across a rebuild that lost a contributor would
/// leave the result claiming a frequency and — worse — a set of fields that no
/// surviving entry accounts for, and `field_mask` decides field filtering and
/// field-weighted scoring.
///
/// [`metrics`] are carried across untouched, because they are *not* a function of
/// the children: [`RSIndexResult::push_borrowed`] **moves** a child's metrics
/// into the composite when the aggregate is first built, leaving the child with
/// none. There is nothing to re-accumulate, so resetting them would discard
/// them for good — a KNN child's `__vector_score` vanishing from the reply. The
/// cost of keeping them is that a metric contributed by a child that has since
/// been dropped stays; the alternative loses the surviving children's metrics
/// too, to remove it. Should a union then re-settle,
/// [`restore_gathered_metrics`] decides which of them it goes on publishing.
///
/// `doc_id` is the document the rebuild is *for*: the input to the contributor
/// test rather than an output of it.
///
/// # Order
///
/// Entries are pushed in the order `children` yields them, and with
/// `QUICK_EXIT`, for a union that reports a single child rather than
/// aggregating, the loop stops at the first contributor. Which child that is,
/// and the order the entries come back in, are therefore the caller's choice;
/// each composite documents its own at the call site.
///
/// # Call it last
///
/// Not merely "before the cast": **after** the child walk, **after** any
/// side-table rebuild, and with no `&mut` taken to a child afterwards. Each
/// entry is derived from a shared reborrow of a `&mut` to its child, so any
/// later mutable access to that child — a `read`, a `skip_to`, another
/// `current`, an `iter_mut` over the children, a `swap_remove`, a sort — pops
/// the tag it carries. `&mut` to the composite itself is fine; the entries
/// point into the *children's* allocations.
///
/// A resume that goes on to re-seek cannot honour that: the re-seek moves
/// children before it rebuilds the aggregate itself. Such a resume clears the
/// records instead of calling this, so it never holds an entry the re-seek
/// would leave stale.
///
/// [`freq`]: index_result::RawIndexResult::freq
/// [`field_mask`]: index_result::RawIndexResult::field_mask
/// [`metrics`]: index_result::RawIndexResult::metrics
/// [`restore_gathered_metrics`]: crate::union::restore_gathered_metrics
pub(crate) fn repush_borrowed_entries<'a, 'child, R, const QUICK_EXIT: bool>(
    aggregate: &mut RawBorrowedAggregateResult<'a, Suspended>,
    doc_id: DocId,
    freq: &mut u32,
    field_mask: &mut FieldMask,
    children: impl IntoIterator<Item = &'child mut R>,
) -> usize
where
    R: RQEIterator<'a> + 'child,
{
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
        // it past the composite's document no longer describes that document.
        let Some(current) = child.current() else {
            continue;
        };
        if current.doc_id != doc_id {
            continue;
        }
        // The entry is taken from a shared reborrow, so the tag it carries stays
        // valid for as long as nothing takes `&mut` to this child again. The
        // loop only ever moves on to a *different* child, and the composite
        // takes none after this call returns.
        let current: &RSIndexResult<'a> = &*current;

        *freq += current.freq;
        *field_mask |= current.field_mask;
        aggregate.push_borrowed_ptr_from_ref(current);
        contributors += 1;

        if QUICK_EXIT {
            break;
        }
    }
    contributors
}
