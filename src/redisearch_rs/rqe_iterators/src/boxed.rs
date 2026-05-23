/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `Box<Self>`-based suspend/resume trait scaffolding.
//!
//! This module introduces the suspend/resume trait hierarchy that will
//! supersede the legacy
//! [`RQEIterator::revalidate`] design:
//!
//! | Concept              | Concrete (type-state preserved)   | Dyn-safe sibling                    |
//! |----------------------|-----------------------------------|-------------------------------------|
//! | Active iterator      | [`RQEIteratorBoxed`]              | [`RQEDynIterator`]                  |
//! | Suspended iterator   | [`RQESuspendedIterator`]          | [`RQEDynSuspendedIterator`]         |
//! | Erasure wrapper type | [`TypeErasedRQEIterator`]         | [`TypeErasedRQESuspendedIterator`]  |
//!
//! Implementers only need to provide the *concrete* traits
//! ([`RQEIteratorBoxed`] / [`RQESuspendedIterator`]); blanket bridge impls
//! produce the corresponding [`RQEDynIterator`] / [`RQEDynSuspendedIterator`]
//! implementations automatically.
//!
//! The receiver shape (`self: Box<Self>`) is what unlocks object safety
//! while still letting the suspend/resume body reinterpret the heap
//! allocation byte-identically — see [`RQEIteratorBoxed::suspend`] for the
//! intended idiom.
//!
//! # Transitional shape
//!
//! During the first phase of the revalidation work,
//! the new `RQEIteratorBoxed` trait is a **subtrait** of the legacy [`RQEIterator`]:
//!
//! ```text
//! trait RQEIteratorBoxed<'index>: RQEIterator<'index> + 'index {
//!     type Suspended: RQESuspendedIterator + 'static;
//!     fn suspend(self: Box<Self>) -> Box<Self::Suspended>;
//! }
//! ```
//!
//! This means every iterator only needs to *add* `type Suspended` and
//! `fn suspend` to migrate — the read/skip/rewind/etc. surface comes from
//! the supertrait, and there is no method-name ambiguity at internal call
//! sites that reach for `self.foo()`. The same goes for [`RQEDynIterator`]
//! against the legacy trait on the dyn-erased side.
//!
//! In the second phase of the revalidation work, the legacy [`RQEIterator`] trait will be
//! deleted entirely; its method signatures (sans `revalidate`) will be folded
//! directly into [`RQEIteratorBoxed`] / [`RQEDynIterator`], and
//! [`RQEIteratorBoxed`] will be renamed back to `RQEIterator`.

use std::ptr::NonNull;

use ffi::t_docId;
use index_result::{RSIndexResult, SuspendedIndexResult};
use index_spec::IndexSpecReadGuard;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    c2rust,
};

/// Concrete-typed active iterator trait — the new shape of
/// [`RQEIterator`].
///
/// Compared with the legacy trait it adds:
///
/// 1. [`suspend`](Self::suspend) consumes `self: Box<Self>` and returns
///    `Box<Self::Suspended>`. The intended implementation is a pure pointer
///    cast layout-compatible `Active`/`Suspended` counterparts. This preserves
///    the box's heap address across the suspend/resume cycle, which matters
///    for iterators that give out raw pointers to (parts of their) internal state.
///
/// The [`Box<Self>`] receiver also makes this method object-safe, which is
/// what lets the [`RQEDynIterator`] sibling exist as a free blanket impl.
pub trait RQEIteratorBoxed<'index>: RQEIterator<'index> + 'index {
    /// The suspended counterpart of this iterator. Carries no live
    /// references into the *index* (those are weakened to raw pointers on
    /// suspend), but may still borrow query-pipeline data for `'index` — see
    /// the `'query` parameter on [`RQESuspendedIterator`]. It can therefore be
    /// held across a lock release/reacquire cycle: the index pointers are
    /// re-validated on resume, while the query-pipeline borrows stay live.
    type Suspended: RQESuspendedIterator<'index> + 'index;

    /// Transition to the suspended state.
    ///
    /// # Precondition
    ///
    /// The spec read lock must still be held for the duration of the call — suspending is the
    /// step that *earns* the right to release it, not something to do afterwards. Most
    /// implementations only re-type Rust-owned state and would not notice, but
    /// [`CRQEIterator`](c2rust::CRQEIterator) reads its estimate off the C iterator underneath,
    /// which may deref index-owned memory. A generic caller such as
    /// [`suspend_child_slot_in_place`] cannot tell from the type whether the subtree it is
    /// suspending holds such a child, so the obligation is on every caller.
    fn suspend(self: Box<Self>) -> Box<Self::Suspended>;
}

/// Concrete-typed suspended iterator trait — counterpart of
/// [`RQEIteratorBoxed`].
///
/// Implementers are typically the `Raw…<Suspended, 'query>` instantiations of
/// the same `#[repr(C)]` struct used in active mode.
///
/// The `'query` parameter is the lifetime of the **query-pipeline** data the
/// iterator borrows — e.g. the `RLookupKey` a metric result yields against, or
/// a term record's borrowed query term. Unlike index-derived pointers (which
/// are weakened to raw pointers on suspend and re-validated via the spec guard
/// on resume), query-pipeline data is *not* invalidated by concurrent index
/// mutation, so it stays a live borrow across the whole suspend/resume cycle.
/// That is why this trait carries `'query` instead of a `'static` bound: a
/// suspended iterator holds no live *index* references, but may still borrow
/// query-pipeline data for `'query`.
pub trait RQESuspendedIterator<'query> {
    /// The active counterpart this iterator resumes into, parameterised by
    /// the lifetime of the freshly re-acquired read guard.
    ///
    /// `'query: 'index` is required because the retained query-pipeline
    /// borrows must outlive the (shorter) guard window the iterator is
    /// resumed into.
    type Resumed<'index>: RQEIteratorBoxed<'index>
    where
        'query: 'index;

    /// Resume from the suspended state, re-acquiring references into the
    /// index and re-validating the iterator's state against any changes
    /// that happened while the iterator was suspended.
    ///
    /// Returns a [`ResumeOutcome`], mirroring the legacy
    /// [`RQEIterator::revalidate`]'s [`RQEValidateStatus`]:
    ///
    /// - [`Ok`](ResumeOutcome::Ok) — resumed at the same position.
    /// - [`Moved`](ResumeOutcome::Moved) — resumed but the position moved
    ///   forward (the previous `last_doc_id` was deleted or otherwise no
    ///   longer present); query [`current`](RQEIterator::current) on the
    ///   returned iterator.
    /// - [`Aborted`](ResumeOutcome::Aborted) — the iterator's underlying state
    ///   is unrecoverable. No active iterator is produced; the suspended
    ///   iterator is dropped.
    ///
    /// # Implementer obligation
    ///
    /// A composite returning [`Moved`](ResumeOutcome::Moved) must leave the
    /// resumed iterator answering [`current`](RQEIterator::current) per that
    /// method's contract. Otherwise it hands back the pre-suspend result — the
    /// stale position that made the resume necessary — and a composite one level
    /// up cannot recover its own position from it.
    ///
    /// Resume re-reads/seeks the index to restore position (mirroring
    /// [`RQEIterator::revalidate`]), so it can fail with an
    /// [`RQEIteratorError`] (e.g. [`IoError`](RQEIteratorError::IoError) or
    /// [`TimedOut`](RQEIteratorError::TimedOut)) — distinct from `Aborted`. On
    /// `Err` the suspended iterator is consumed and dropped.
    fn resume<'index>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index;

    /// Read the cached `last_doc_id` from the suspended state without
    /// resuming. Composite iterators use this during resume to compare
    /// their previous position against the child's pre-resume position.
    fn last_doc_id(&self) -> t_docId;

    /// Read the cached `num_estimated` from the suspended state without
    /// resuming. Used by FFI introspection (`FT.PROFILE` printing) which
    /// is called after the iterator has been suspended at the unlock site.
    ///
    /// The value is an estimate, so returning a snapshot from construction
    /// is acceptable — the underlying invariant is that the FFI consumer
    /// uses it for display only. Default returns 0 for iterators that do
    /// not maintain a cached estimate.
    fn num_estimated(&self) -> usize;
}

/// Dyn-safe sibling of [`RQEIteratorBoxed`].
///
/// You shouldn't implement this trait by hand; the blanket
/// `impl<T: RQEIteratorBoxed<'index> + 'index> RQEDynIterator<'index> for T` below
/// produces it for every concrete iterator.
pub trait RQEDynIterator<'index>: RQEIterator<'index> + 'index {
    /// Type-erased counterpart of [`RQEIteratorBoxed::suspend`].
    fn suspend(self: Box<Self>) -> TypeErasedRQESuspendedIterator<'index>;
}

/// Dyn-safe sibling of [`RQESuspendedIterator`].
///
/// As with [`RQEDynIterator`], implementers don't write this directly — the
/// blanket bridge below produces it from any
/// `T: RQESuspendedIterator`.
pub trait RQEDynSuspendedIterator<'query> {
    /// Type-erased counterpart of [`RQESuspendedIterator::resume`].
    fn resume<'index>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<TypeErasedRQEIterator<'index>>, RQEIteratorError>
    where
        'query: 'index;

    fn last_doc_id(&self) -> t_docId;

    fn num_estimated(&self) -> usize;
}

/// Type-erased, active iterator.
///
/// Newtype around `Box<dyn RQEDynIterator<'index> + 'index>`. The wrapper itself
/// implements [`RQEIterator`] and [`RQEIteratorBoxed`] so composites can
/// take it as their `I` parameter without knowing it's holding a trait
/// object.
#[repr(transparent)]
pub struct TypeErasedRQEIterator<'index>(pub Box<dyn RQEDynIterator<'index> + 'index>);

/// Type-erased, suspended iterator.
///
/// Newtype around `Box<dyn RQEDynSuspendedIterator<'query> + 'query>`. Mirrors
/// [`TypeErasedRQEIterator`] in the suspended state. Carries the `'query`
/// lifetime of the borrowed query-pipeline data (see [`RQESuspendedIterator`]).
#[repr(transparent)]
pub struct TypeErasedRQESuspendedIterator<'query>(
    pub Box<dyn RQEDynSuspendedIterator<'query> + 'query>,
);

impl<'index> TypeErasedRQEIterator<'index> {
    /// Wrap a concrete iterator into the type-erased wrapper.
    pub fn new<I: RQEIteratorBoxed<'index> + 'index>(iter: Box<I>) -> Self {
        Self(iter as Box<dyn RQEDynIterator<'index> + 'index>)
    }
}

impl<'query> TypeErasedRQESuspendedIterator<'query> {
    /// Wrap a concrete suspended iterator into the type-erased wrapper.
    pub fn new<S: RQESuspendedIterator<'query> + 'query>(iter: Box<S>) -> Self {
        Self(iter as Box<dyn RQEDynSuspendedIterator<'query> + 'query>)
    }
}

/// Compile-time assertion that `A` and `B` have the same size and alignment.
///
/// Call it in a `const {}` block so the check runs at monomorphization: a
/// mismatch fails to compile instead of causing undefined behaviour when the
/// suspend/resume helpers reinterpret an allocation from `A` to `B`.
pub(crate) const fn assert_layout_compatible<A, B>() {
    assert!(
        std::mem::size_of::<A>() == std::mem::size_of::<B>(),
        "size mismatch across suspend/resume transition: active and suspended representations must have identical size"
    );
    assert!(
        std::mem::align_of::<A>() == std::mem::align_of::<B>(),
        "alignment mismatch across suspend/resume transition: active and suspended representations must have identical alignment"
    );
}

/// Suspend a single child slot in place: read the value out, call its
/// [`RQEIteratorBoxed::suspend`] through the trait, and write the suspended
/// counterpart back into the same slot.
///
/// This is the composite-side primitive that lets `Vec<I>` storage hold
/// children whose `I::Suspended` byte representation has different invariants
/// from `I`'s — most importantly, dyn-erased children like [`TypeErasedRQEIterator`]
/// whose active and suspended forms carry different vtables. The trait
/// `suspend` call dispatches via the vtable for those, correctly transitioning
/// the inner concrete iterator; for concrete-typed `I` (where `I` and
/// `I::Suspended` are byte-layout-compatible by `#[repr(C)]`), the trait call
/// is the same whole-box cast that the composite would have done at the outer
/// level — just per-child instead of per-composite.
///
/// # Safety
///
/// * `slot` must point to a valid, exclusively-owned `I` value.
/// * After this call, the slot's bytes are a valid `I::Suspended` value. The
///   caller is responsible for ensuring the slot is *interpreted* as
///   `I::Suspended` from this point on — typically by performing a whole-box
///   cast on the containing composite (relabelling the Vec slot's static
///   type) and not reading the slot as `I` again.
///
/// The size/alignment compatibility of `I` and `I::Suspended` that the internal
/// `ptr::write` cast relies on is *not* a caller obligation: it is enforced at
/// compile time by the `assert_layout_compatible` guard at the top of the body
/// (a mismatched implementer fails to build). It holds for all
/// `RQEIteratorBoxed` impls in this crate by their `#[repr(C)]` layouts over
/// `SharedPtr`/fat-pointer fields.
///
/// Between the `ptr::read` and the matching `ptr::write` the slot is logically
/// uninitialised while the caller (and any composite that owns it) still
/// considers it live. `RQEIteratorBoxed::suspend` is a safe trait method that
/// may dispatch to arbitrary (including dyn) implementations and could panic;
/// if it did, unwinding past the uninitialised slot would let the owner drop a
/// moved-from value (double drop). To keep the window sound, a panic from
/// `suspend` is converted into a process abort rather than an unwind.
pub unsafe fn suspend_child_slot_in_place<'index, I>(slot: *mut I)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    // Statically enforce the size/alignment invariant the `ptr::write` cast
    // below relies on: a mismatched-layout implementer fails to compile here.
    const { assert_layout_compatible::<I, I::Suspended>() };

    debug_assert!(!slot.is_null(), "slot must not be null");

    /// Aborts the process if dropped during unwinding through the
    /// uninitialised-slot window. Disarmed with [`std::mem::forget`] once the
    /// slot has been reinitialised.
    struct AbortOnUnwind;
    impl Drop for AbortOnUnwind {
        fn drop(&mut self) {
            std::process::abort();
        }
    }

    // SAFETY: caller guarantees `slot` is exclusively owned and points to a
    // valid `I` value. `ptr::read` moves the value out; the slot bytes are
    // typed-but-moved-from until the matching `ptr::write` below.
    let active = unsafe { std::ptr::read(slot) };
    // Armed across the uninitialised-slot window: if `suspend` panics, drop
    // aborts instead of unwinding through the moved-from slot.
    let bomb = AbortOnUnwind;
    // Dispatches via:
    // - the vtable for dyn-erased `I` (e.g. `TypeErasedRQEIterator`);
    // - a transmute at the leaf level for concrete `I`.
    // Either way the *inner* concrete iterator's heap allocation is preserved.
    // Only the outer wrapper bytes may differ (and the wrapper's address doesn't
    // matter, see [`crate::interop::revalidate`] for the rationale).
    let suspended = *<I as RQEIteratorBoxed<'index>>::suspend(Box::new(active));
    // SAFETY: `I` and `I::Suspended` share size and alignment (guaranteed by the
    // `assert_layout_compatible` guard at the top of the body). The slot is
    // uninitialised after the earlier `ptr::read`; writing a valid `I::Suspended`
    // reinitialises it.
    unsafe { std::ptr::write(slot as *mut I::Suspended, suspended) };
    // Slot reinitialised — disarm the abort guard.
    std::mem::forget(bomb);
}

/// Outcome of [`resume_child_slot_in_place`], mirroring the recoverable
/// discriminants of [`ResumeOutcome`] but *without* carrying the iterator —
/// the resumed child is written back into the slot instead.
pub enum ResumeSlotOutcome {
    /// The child resumed at the same position; the slot now holds the active child.
    Unchanged,
    /// The child resumed but its position moved forward; the slot now holds the
    /// active child.
    Moved,
    /// The child's state was unrecoverable. It was consumed and the slot is
    /// **left uninitialised** — see the safety contract.
    Aborted,
}

/// Resume a single child slot in place: read the suspended child out, drive its
/// consuming [`RQESuspendedIterator::resume`] through the trait, and — on a
/// recoverable outcome — write the resumed counterpart back into the **same
/// slot**. This is the resume-direction mirror of
/// [`suspend_child_slot_in_place`].
///
/// The inner iterator's own heap allocation is preserved by its `resume`, and
/// the resumed value is written back to the same slot, so the *slot* address is
/// stable. A caller that also reuses its own allocation (via
/// `Box::into_raw`/`Box::from_raw`) therefore preserves every interior address
/// across the suspend/resume cycle.
///
/// # Safety
///
/// * `slot` must point to a valid, exclusively-owned `S` value.
/// * On [`Ok`](ResumeSlotOutcome::Unchanged)/[`Moved`](ResumeSlotOutcome::Moved)
///   the slot's bytes are a valid `S::Resumed<'a>`; the caller must interpret
///   the slot (and its container) as `S::Resumed<'a>` from this point on and not
///   read it as `S` again.
/// * On [`Aborted`](ResumeSlotOutcome::Aborted) or [`Err`] the child was consumed
///   by its `resume` and the slot is **left uninitialised**; the caller must tear
///   the container down WITHOUT dropping the slot.
///
/// The size/alignment compatibility of `S` and `S::Resumed<'a>` that the internal
/// `ptr::write` cast relies on is *not* a caller obligation: it is enforced at
/// compile time by the `assert_layout_compatible` guard at the top of the body
/// (a mismatched implementer fails to build). It holds for all
/// `RQEIteratorBoxed`/`RQESuspendedIterator` impls in this crate by their
/// `#[repr(C)]` layouts.
///
/// Between the `ptr::read` and the matching `ptr::write` (or the caller's
/// teardown) the slot is logically uninitialised. [`RQESuspendedIterator::resume`]
/// is a safe trait method that may dispatch to arbitrary (including dyn)
/// implementations and could panic; as in [`suspend_child_slot_in_place`], a
/// panic is converted into a process abort rather than an unwind through the
/// uninitialised slot.
pub unsafe fn resume_child_slot_in_place<'query, 'a, S>(
    slot: *mut S,
    guard: &IndexSpecReadGuard<'a>,
) -> Result<ResumeSlotOutcome, RQEIteratorError>
where
    S: RQESuspendedIterator<'query>,
    'query: 'a,
{
    // Statically enforce the size/alignment invariant the `ptr::write` cast
    // below relies on: a mismatched-layout implementer fails to compile here.
    const { assert_layout_compatible::<S, S::Resumed<'a>>() };

    debug_assert!(!slot.is_null(), "slot must not be null");

    /// Aborts the process if dropped during unwinding through the
    /// uninitialised-slot window. Disarmed with [`std::mem::forget`] once
    /// `resume` has returned.
    struct AbortOnUnwind;
    impl Drop for AbortOnUnwind {
        fn drop(&mut self) {
            std::process::abort();
        }
    }

    // SAFETY: caller guarantees `slot` is exclusively owned and points to a
    // valid `S` value. `ptr::read` moves the value out; the slot bytes are
    // typed-but-moved-from until the matching `ptr::write` (or teardown) below.
    let suspended = unsafe { std::ptr::read(slot) };
    // Armed across the uninitialised-slot window: if `resume` panics, drop
    // aborts instead of unwinding through the moved-from slot.
    let bomb = AbortOnUnwind;
    let outcome = Box::new(suspended).resume(guard);
    // `resume` returned normally (Ok/Aborted or Err) — the remaining steps below
    // cannot panic, so disarm before we either write the slot back or hand an
    // uninitialised slot to the caller for teardown.
    std::mem::forget(bomb);

    let (active, moved) = match outcome? {
        ResumeOutcome::Aborted => return Ok(ResumeSlotOutcome::Aborted),
        ResumeOutcome::Ok(active) => (active, false),
        ResumeOutcome::Moved(active) => (active, true),
    };
    // SAFETY: `S` and `S::Resumed<'a>` share size and alignment (guaranteed by
    // the `assert_layout_compatible` guard at the top of the body). The slot is
    // uninitialised after the earlier `ptr::read`; writing a valid
    // `S::Resumed<'a>` reinitialises it.
    unsafe { std::ptr::write(slot as *mut S::Resumed<'a>, *active) };
    Ok(if moved {
        ResumeSlotOutcome::Moved
    } else {
        ResumeSlotOutcome::Unchanged
    })
}

/// What [`rederive_aggregate_entries`] (or [`clear_aggregate_entries`]) left
/// behind, and the composite's cue for what it may still publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RederiveOutcome {
    /// Every borrowed entry was re-derived from a live child sitting on the
    /// result's own document. The result stands exactly as the composite left
    /// it, and re-narrowing it is sound.
    Rederived,
    /// At least one entry had no live, on-document child to be re-derived from,
    /// so *all* of them were dropped.
    ///
    /// The result keeps its position and its accumulated `freq`, `field_mask`
    /// and `metrics`; only the list of contributing children is gone. It is
    /// therefore sound to re-narrow — nothing points anywhere any more — but it
    /// no longer describes a document. The composite must either rebuild it
    /// before publishing it (which every path that re-derives its position does
    /// anyway) or, when it has no way to, report
    /// [`ResumeOutcome::Aborted`] instead of handing it back.
    Cleared,
}

/// Drop a suspended composite's borrowed aggregate entries, keeping everything
/// else about the result.
///
/// This is [`rederive_aggregate_entries`]'s shortfall path, exposed on its own
/// for the callers that already know re-derivation cannot succeed — a composite
/// that compacted its children's slots, say, where an entry's address no longer
/// identifies the child it was taken from.
///
/// Only the entries go.
/// [`reset_aggregate`](index_result::RawIndexResult::reset_aggregate) would be
/// the obvious tool and is the wrong one: it also zeroes `doc_id`, `freq` and `field_mask`
/// and calls `metrics.reset()`. Every other caller in the composites follows it
/// immediately with a `push_borrowed` rebuild that puts all of that back; here
/// there is no rebuild, so a `reset_aggregate` would silently drop the metrics
/// the aggregate accumulated — `__vector_score` disappearing from a KNN+text
/// union's reply — and misreport the result to field-mask filtering and
/// field-weighted scoring as `field_mask == 0`.
pub fn clear_aggregate_entries(result: &mut SuspendedIndexResult<'_>) -> RederiveOutcome {
    if let Some(aggregate) = result.as_aggregate_mut() {
        // Clears the records and the kind mask, which is derived from them.
        aggregate.reset();
    }
    RederiveOutcome::Cleared
}

/// Give a composite's aggregate entries fresh provenance from its
/// just-transitioned children — or clear the aggregate, if that cannot be done.
///
/// This is the last step before a composite that owns an aggregate reinterprets
/// its allocation as the resumed form, and it is not optional: the entries are
/// pointers derived from borrows of the children's results, and transitioning a
/// child invalidates the borrow its entry came from even though the child never
/// leaves its slot. See
/// [`RawAggregateResult::rederive_borrowed`](index_result::RawAggregateResult::rederive_borrowed)
/// for why, and for why rebuilding the aggregate from the children instead would
/// lose what it has already accumulated.
///
/// Call it once, after every child slot has been transitioned and before the
/// whole-box cast, passing **every** surviving child — including any parked
/// outside the composite's active region, since an entry can point at any child
/// the aggregate was built from. A result that is not an aggregate borrows
/// nothing, and is left alone.
///
/// # What counts as re-derivable
///
/// A child answers for an entry when it is still live, still publishes a
/// `current()`, and that `current()` sits on the aggregate's own document — and
/// the check is per entry, not a running total: the same child offered twice
/// would otherwise cover for an entry no child answers for at all. Every entry
/// must be answered for, or the aggregate is
/// [`Cleared`](RederiveOutcome::Cleared) as a whole.
///
/// The document test is what keeps a re-derived entry *meaningful* rather than
/// merely live. A child whose own resume moved it forward while a sibling held
/// the composite in place still occupies its slot, so its address still matches;
/// re-deriving from it would leave the aggregate describing a document its
/// entries no longer belong to. The legacy `revalidate` path rebuilds the result
/// in that situation, and so must this one.
///
/// # What this cannot see
///
/// An address identifies a child only for as long as the children stay put. A
/// composite that **compacts** its child slots — moving a survivor into the hole
/// a dropped sibling left — can hand a survivor whose result now sits exactly
/// where the dropped child's did, for a child whose results live inline in that
/// buffer. The stale entry then matches a live child, and no check on this side
/// can tell that it is the wrong one. Such a composite must call
/// [`clear_aggregate_entries`] instead of this function whenever anything was
/// relocated.
///
/// # Owned aggregates
///
/// The early return on "nothing borrowed" covers an
/// [`Owned`](index_result::RawAggregateResult::Owned) aggregate, such as a
/// `HybridMetric` result, by treating it as needing no work. That is true of
/// its *entries* — they are boxed children in the result's own allocation, which
/// no transition retags — but not necessarily of what those children hold: a
/// boxed child's own `data` can be index-backed and suspended, and re-narrowing
/// it would need the same recursive re-validation a leaf does. No composite
/// wired to this helper owns such an aggregate today, so the case is unreachable
/// rather than handled; a composite that acquires one must re-validate its
/// children itself before the cast.
#[must_use = "a cleared aggregate no longer describes a document; the caller \
              must rebuild it or abort the resume"]
pub fn rederive_aggregate_entries<'query, 'index, 'child, I>(
    result: &mut SuspendedIndexResult<'query>,
    children: impl IntoIterator<Item = &'child mut I>,
) -> RederiveOutcome
where
    I: RQEIterator<'index> + 'child,
{
    let doc_id = result.doc_id;
    let Some(aggregate) = result.as_aggregate() else {
        return RederiveOutcome::Rederived;
    };
    if aggregate.num_borrowed() == 0 {
        // Nothing to re-derive, and so no reason to touch a single child.
        //
        // "Nothing borrowed" is not the same as "nothing to validate" — see the
        // `# Owned aggregates` section above for the boundary this return
        // draws, and why no caller stands on the wrong side of it today.
        return RederiveOutcome::Rederived;
    }

    // A child at EOF hides its result, and one whose own resume carried it onto
    // a later document no longer backs the entry it was taken for; either way it
    // cannot answer for one. Each child's borrow ends with its `current()`, so
    // collecting the references up front is what lets the per-entry check run
    // before a single entry is overwritten.
    let live: Vec<&RSIndexResult<'index>> = children
        .into_iter()
        .filter_map(|child| child.current().map(|current| &*current))
        .filter(|current| current.doc_id == doc_id)
        .collect();

    let every_entry_answered_for = aggregate.borrowed_addresses().all(|entry| {
        live.iter()
            .any(|current| NonNull::from_ref(*current).cast::<()>() == entry)
    });
    if !every_entry_answered_for {
        return clear_aggregate_entries(result);
    }

    let aggregate = result
        .as_aggregate_mut()
        .expect("the result was an aggregate a handful of statements ago");
    let rederived: usize = live
        .iter()
        .map(|current| aggregate.rederive_borrowed(current))
        .sum();
    debug_assert_eq!(
        rederived,
        aggregate.num_borrowed(),
        "every entry was just shown to have a live child, so every entry was rewritten",
    );
    RederiveOutcome::Rederived
}

// --- Blanket bridges: concrete → dyn-safe -----------------------------------

/// Bridge concrete active iterators into the dyn-safe sibling.
///
/// Only `suspend` is bridged here — the read/skip surface is inherited from
/// the legacy [`RQEIterator`] supertrait, which the
/// concrete iterator already implements.
impl<'index, T: RQEIteratorBoxed<'index> + 'index> RQEDynIterator<'index> for T {
    #[inline(always)]
    fn suspend(self: Box<Self>) -> TypeErasedRQESuspendedIterator<'index> {
        let suspended = <T as RQEIteratorBoxed<'index>>::suspend(self);
        TypeErasedRQESuspendedIterator(
            suspended as Box<dyn RQEDynSuspendedIterator<'index> + 'index>,
        )
    }
}

/// Bridge concrete suspended iterators into the dyn-safe sibling.
impl<'query, S: RQESuspendedIterator<'query> + 'query> RQEDynSuspendedIterator<'query> for S {
    #[inline(always)]
    fn resume<'index>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<TypeErasedRQEIterator<'index>>, RQEIteratorError>
    where
        'query: 'index,
    {
        // This bridge is the *only* place the resumed iterator is type-erased:
        // the concrete impl hands back its `Box<Self::Resumed>`, which we wrap
        // into a `TypeErasedRQEIterator`. `Aborted` carries nothing, so it maps
        // straight through. (The already-erased forwarding impl on
        // `TypeErasedRQESuspendedIterator` deliberately double-boxes; see there.)
        Ok(
            match <S as RQESuspendedIterator<'query>>::resume(self, guard)? {
                ResumeOutcome::Ok(it) => ResumeOutcome::Ok(TypeErasedRQEIterator::new(it)),
                ResumeOutcome::Moved(it) => ResumeOutcome::Moved(TypeErasedRQEIterator::new(it)),
                ResumeOutcome::Aborted => ResumeOutcome::Aborted,
            },
        )
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        <S as RQESuspendedIterator<'query>>::last_doc_id(self)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        <S as RQESuspendedIterator<'query>>::num_estimated(self)
    }
}

// --- Forwarding impls on the wrappers themselves ----------------------------

/// Forwarding [`RQEIterator`] impl so [`TypeErasedRQEIterator`] can serve as the
/// `I` type parameter of composite iterators (which bound on
/// [`RQEIterator`] via the [`RQEIteratorBoxed`] supertrait).
impl<'index> RQEIterator<'index> for TypeErasedRQEIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.0.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.0.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.0.revalidate(spec)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.0.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.0.type_()
    }

    #[inline(always)]
    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        self.0.as_c_iterator()
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.0.intersection_sort_weight(prioritize_union_children)
    }
}

/// Forwarding [`RQEIteratorBoxed`] impl so [`TypeErasedRQEIterator`] also
/// participates in the new suspend/resume surface (its `Suspended`
/// counterpart is [`TypeErasedRQESuspendedIterator`]).
impl<'index> RQEIteratorBoxed<'index> for TypeErasedRQEIterator<'index> {
    type Suspended = TypeErasedRQESuspendedIterator<'index>;

    #[inline(always)]
    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let TypeErasedRQEIterator(inner) = *self;
        Box::new(<dyn RQEDynIterator<'index> as RQEDynIterator<'index>>::suspend(inner))
    }
}

/// Forwarding [`RQESuspendedIterator`] impl on [`TypeErasedRQESuspendedIterator`]
/// so the dyn-erased pair behaves like any other concrete iterator pair.
impl<'query> RQESuspendedIterator<'query> for TypeErasedRQESuspendedIterator<'query> {
    type Resumed<'index>
        = TypeErasedRQEIterator<'index>
    where
        'query: 'index;

    #[inline(always)]
    fn resume<'index>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<TypeErasedRQEIterator<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let TypeErasedRQESuspendedIterator(inner) = *self;
        // `Self::Resumed` is the already-erased `TypeErasedRQEIterator`, so the
        // concrete `ResumeOutcome<Box<Self::Resumed>>` shape forces a
        // (deliberate) double box here. This is a transient allocation only on
        // the resume path for an erased composite child; the hot path resumes
        // the concrete inner via the blanket bridge (single box).
        Ok(match inner.resume(guard)? {
            ResumeOutcome::Ok(it) => ResumeOutcome::Ok(Box::new(it)),
            ResumeOutcome::Moved(it) => ResumeOutcome::Moved(Box::new(it)),
            ResumeOutcome::Aborted => ResumeOutcome::Aborted,
        })
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }
}
