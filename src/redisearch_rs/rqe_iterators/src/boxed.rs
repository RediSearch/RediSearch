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
//! [`RQEIterator::revalidate`](super::RQEIterator::revalidate) design:
//!
//! | Concept              | Concrete (type-state preserved)   | Dyn-safe sibling                |
//! |----------------------|-----------------------------------|---------------------------------|
//! | Active iterator      | [`RQEIteratorBoxed`]              | [`RQEDynIterator`]              |
//! | Suspended iterator   | [`RQESuspendedIterator`]          | [`RQEDynSuspendedIterator`]     |
//! | Erasure wrapper type | [`BoxedRQEIterator`]              | [`BoxedRQESuspendedIterator`]   |
//!
//! Implementors only need to provide the *concrete* traits
//! ([`RQEIteratorBoxed`] / [`RQESuspendedIterator`]); blanket bridge impls
//! produce the corresponding [`RQEDynIterator`] / [`RQEDynSuspendedIterator`]
//! implementations automatically.
//!
//! The receiver shape (`self: Box<Self>`) is what unlocks object safety
//! while still letting the suspend/resume body reinterpret the heap
//! allocation byte-identically — see [`RQEIteratorBoxed::suspend`] for the
//! intended idiom.
//!
//! # R1 + R2 transitional shape
//!
//! During R1 and R2 the new active-iterator trait is a **subtrait** of the
//! legacy [`RQEIterator`](super::RQEIterator):
//!
//! ```text
//! trait RQEIteratorBoxed<'a>: RQEIterator<'a> + 'a {
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
//! In **R3** the legacy [`RQEIterator`](super::RQEIterator) trait is
//! deleted entirely; its method signatures (sans `revalidate`) are folded
//! directly into [`RQEIteratorBoxed`] / [`RQEDynIterator`], and
//! [`RQEIteratorBoxed`] is renamed back to `RQEIterator`. That matches the
//! end-state design described in the plan.

use ffi::{ValidateStatus, t_docId};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, c2rust,
};

/// Concrete-typed active iterator trait — the new shape of
/// [`RQEIterator`](super::RQEIterator).
///
/// Compared with the legacy trait it adds:
///
/// 1. [`suspend`](Self::suspend) consumes `self: Box<Self>` and returns
///    `Box<Self::Suspended>`. The intended implementation is a pure pointer
///    cast (`Box::from_raw(Box::into_raw(self) as *mut _)`) on `#[repr(C)]`,
///    layout-compatible `Active`/`Suspended` counterparts. This preserves
///    the box's heap address — composite aggregate
///    [`RawIndexResult`](index_result::RawIndexResult) pointers into
///    children's interiors stay valid across the suspend/resume cycle.
///
/// The [`Box<Self>`] receiver also makes this method object-safe, which is
/// what lets the [`RQEDynIterator`] sibling exist as a free blanket impl.
///
/// During R1–R2 this trait is a **subtrait** of
/// [`RQEIterator`](super::RQEIterator) so that the read/skip/rewind surface
/// is inherited without duplication. R3 folds those method signatures into
/// this trait directly and renames it back to `RQEIterator`.
pub trait RQEIteratorBoxed<'a>: RQEIterator<'a> + 'a {
    /// The suspended counterpart of this iterator. Carries no live
    /// references into the index and can therefore be held across a lock
    /// release/reacquire cycle.
    type Suspended: RQESuspendedIterator + 'static;

    /// Transition to the suspended state.
    ///
    /// Implementations should perform a pure pointer cast of the box:
    /// the active and suspended types are `#[repr(C)]` layout-compatible
    /// over [`SharedPtr`](ref_mode::SharedPtr) (a `#[repr(transparent)]`
    /// `NonNull`) fields, so the same heap allocation can be relabelled as
    /// the suspended type without reallocation. Preserving the heap address
    /// is what keeps composite aggregate-result pointers valid across the
    /// cycle.
    fn suspend(self: Box<Self>) -> Box<Self::Suspended>;

    /// Cascade the suspend signal to child iterators' C-side wrappers.
    ///
    /// Composite iterators must override this to call `cascade_suspend` on each
    /// child (so `CRQEIterator` children invoke their wrapped iterator's
    /// `Suspend` vtable entry, flipping its typestate). [`CRQEIterator`] itself
    /// overrides this to call its `Suspend` callback. Leaf iterators inherit
    /// the default no-op.
    ///
    /// Called by the FFI wrapper's `Suspend` callback **before**
    /// [`suspend`](Self::suspend) does its `Box<Self>` type-cast — without
    /// this, child wrappers stay Active across the parent's suspend/resume
    /// cycle, and the parent's resume cascade no-ops on them, leaving their
    /// internal state stale.
    fn cascade_suspend(&mut self) {}
}

/// Concrete-typed suspended iterator trait — counterpart of
/// [`RQEIteratorBoxed`].
///
/// Implementors are typically the `Raw…<Suspended, …>` instantiations of
/// the same `#[repr(C)]` struct used in active mode. The `'static` bound
/// reflects the absence of live index references: a suspended iterator
/// can be held across a lock release.
pub trait RQESuspendedIterator: 'static {
    /// The active counterpart this iterator resumes into, parameterised by
    /// the lifetime of the held read guard.
    type Resumed<'a>: RQEIteratorBoxed<'a>;

    /// Resume from the suspended state, re-acquiring references into the
    /// index and re-validating the iterator's state against any changes
    /// that happened while the iterator was suspended.
    ///
    /// Returns the active iterator alongside a [`ValidateStatus`]:
    ///
    /// - [`VALIDATE_OK`](ffi::ValidateStatus_VALIDATE_OK) — the iterator is
    ///   at the same position it was before suspend.
    /// - [`VALIDATE_MOVED`](ffi::ValidateStatus_VALIDATE_MOVED) — the
    ///   iterator's position moved forward (the previous `last_doc_id` was
    ///   deleted or otherwise no longer present); callers should query
    ///   [`current`](RQEIterator::current).
    /// - [`VALIDATE_ABORTED`](ffi::ValidateStatus_VALIDATE_ABORTED) — the
    ///   iterator's underlying state is unrecoverable; it should be
    ///   dropped.
    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus);

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
    /// uses it for display only.
    fn num_estimated(&self) -> usize;
}

/// Dyn-safe sibling of [`RQEIteratorBoxed`].
///
/// During R1–R2 this trait is a **subtrait** of
/// [`RQEIterator`](super::RQEIterator) — the read/skip/rewind surface is
/// reached via the supertrait, and only [`suspend`](Self::suspend) is new
/// on top. R3 folds the legacy iter methods directly into this trait.
///
/// Implementors should not write this trait by hand; the blanket
/// `impl<T: RQEIteratorBoxed<'a> + 'a> RQEDynIterator<'a> for T` below
/// produces it for every concrete iterator.
pub trait RQEDynIterator<'a>: RQEIterator<'a> + 'a {
    /// Type-erased counterpart of [`RQEIteratorBoxed::suspend`].
    fn suspend(self: Box<Self>) -> BoxedRQESuspendedIterator;
    /// Type-erased counterpart of [`RQEIteratorBoxed::cascade_suspend`].
    fn cascade_suspend(&mut self);
}

/// Dyn-safe sibling of [`RQESuspendedIterator`].
///
/// As with [`RQEDynIterator`], implementors don't write this directly — the
/// blanket bridge below produces it from any
/// `T: RQESuspendedIterator`.
pub trait RQEDynSuspendedIterator: 'static {
    /// Type-erased counterpart of [`RQESuspendedIterator::resume`].
    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (BoxedRQEIterator<'a>, ValidateStatus);

    fn last_doc_id(&self) -> t_docId;

    fn num_estimated(&self) -> usize;
}

/// Type-erased, active iterator.
///
/// Newtype around `Box<dyn RQEDynIterator<'a> + 'a>`. The wrapper itself
/// implements [`RQEIterator`] and [`RQEIteratorBoxed`] so composites can
/// take it as their `I` parameter without knowing it's holding a trait
/// object.
#[repr(transparent)]
pub struct BoxedRQEIterator<'a>(pub Box<dyn RQEDynIterator<'a> + 'a>);

/// Type-erased, suspended iterator.
///
/// Newtype around `Box<dyn RQEDynSuspendedIterator>`. Mirrors
/// [`BoxedRQEIterator`] in the suspended state.
#[repr(transparent)]
pub struct BoxedRQESuspendedIterator(pub Box<dyn RQEDynSuspendedIterator>);

impl<'a> BoxedRQEIterator<'a> {
    /// Wrap a concrete iterator into the type-erased wrapper.
    pub fn new<I: RQEIteratorBoxed<'a> + 'a>(iter: Box<I>) -> Self {
        Self(iter as Box<dyn RQEDynIterator<'a> + 'a>)
    }
}

/// Suspend a single child slot in place: read the value out, call its
/// [`RQEIteratorBoxed::suspend`] through the trait, and write the suspended
/// counterpart back into the same slot.
///
/// This is the composite-side primitive that lets `Vec<I>` storage hold
/// children whose `I::Suspended` byte representation has different invariants
/// from `I`'s — most importantly, dyn-erased children like [`BoxedRQEIterator`]
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
/// * `I` and `I::Suspended` must have the same size and alignment — guaranteed
///   for all `RQEIteratorBoxed` impls in this crate by their `#[repr(C)]`
///   layouts over `SharedPtr`/fat-pointer fields.
pub(crate) unsafe fn suspend_child_slot_in_place<'a, I>(slot: *mut I)
where
    I: RQEIteratorBoxed<'a> + 'a,
{
    // SAFETY: caller guarantees `slot` is exclusively owned and points to a
    // valid `I` value. `ptr::read` moves the value out; the slot bytes are
    // typed-but-moved-from until the matching `ptr::write` below.
    let active = unsafe { std::ptr::read(slot) };
    // Dispatches via the vtable for dyn-erased `I` (e.g. `BoxedRQEIterator`);
    // a whole-box cast at the leaf level for concrete `I`. Either way the
    // inner concrete iterator's heap allocation is preserved — only the
    // outer wrapper bytes may differ (and the wrapper's address doesn't
    // matter, see [`crate::interop::revalidate`] for the rationale).
    let suspended = *<I as RQEIteratorBoxed<'a>>::suspend(Box::new(active));
    // SAFETY: `I` and `I::Suspended` share size and alignment (see contract
    // above). The slot is uninitialised after the earlier `ptr::read`;
    // writing a valid `I::Suspended` reinitialises it.
    unsafe { std::ptr::write(slot as *mut I::Suspended, suspended) };
}

impl BoxedRQESuspendedIterator {
    /// Wrap a concrete suspended iterator into the type-erased wrapper.
    pub fn new<S: RQESuspendedIterator>(iter: Box<S>) -> Self {
        Self(iter as Box<dyn RQEDynSuspendedIterator>)
    }
}

// --- Blanket bridges: concrete → dyn-safe -----------------------------------

/// Bridge concrete active iterators into the dyn-safe sibling.
///
/// Only `suspend` is bridged here — the read/skip surface is inherited from
/// the legacy [`RQEIterator`](super::RQEIterator) supertrait, which the
/// concrete iterator already implements.
impl<'a, T: RQEIteratorBoxed<'a> + 'a> RQEDynIterator<'a> for T {
    fn suspend(self: Box<Self>) -> BoxedRQESuspendedIterator {
        let suspended = <T as RQEIteratorBoxed<'a>>::suspend(self);
        BoxedRQESuspendedIterator(suspended as Box<dyn RQEDynSuspendedIterator>)
    }

    fn cascade_suspend(&mut self) {
        <T as RQEIteratorBoxed<'a>>::cascade_suspend(self);
    }
}

/// Bridge concrete suspended iterators into the dyn-safe sibling.
impl<S: RQESuspendedIterator> RQEDynSuspendedIterator for S {
    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (BoxedRQEIterator<'a>, ValidateStatus) {
        let (active, status) = <S as RQESuspendedIterator>::resume(self, guard);
        (
            BoxedRQEIterator(active as Box<dyn RQEDynIterator<'a> + 'a>),
            status,
        )
    }

    fn last_doc_id(&self) -> t_docId {
        <S as RQESuspendedIterator>::last_doc_id(self)
    }

    fn num_estimated(&self) -> usize {
        <S as RQESuspendedIterator>::num_estimated(self)
    }
}

// --- Forwarding impls on the wrappers themselves ----------------------------

/// Forwarding [`RQEIterator`] impl so [`BoxedRQEIterator`] can serve as the
/// `I` type parameter of composite iterators (which bound on
/// [`RQEIterator`] via the [`RQEIteratorBoxed`] supertrait).
impl<'a> RQEIterator<'a> for BoxedRQEIterator<'a> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'a>> {
        self.0.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'a>>, RQEIteratorError> {
        self.0.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'a>>, RQEIteratorError> {
        self.0.skip_to(doc_id)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'a>, RQEIteratorError> {
        self.0.revalidate(spec)
    }

    fn rewind(&mut self) {
        self.0.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.0.at_eof()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.0.type_()
    }

    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        self.0.as_c_iterator()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.0.intersection_sort_weight(prioritize_union_children)
    }
}

/// Forwarding [`RQEIteratorBoxed`] impl so [`BoxedRQEIterator`] also
/// participates in the new suspend/resume surface (its `Suspended`
/// counterpart is [`BoxedRQESuspendedIterator`]).
impl<'a> RQEIteratorBoxed<'a> for BoxedRQEIterator<'a> {
    type Suspended = BoxedRQESuspendedIterator;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let BoxedRQEIterator(inner) = *self;
        Box::new(<dyn RQEDynIterator<'a> as RQEDynIterator<'a>>::suspend(
            inner,
        ))
    }

    fn cascade_suspend(&mut self) {
        self.0.cascade_suspend();
    }
}

/// Forwarding [`RQESuspendedIterator`] impl on [`BoxedRQESuspendedIterator`]
/// so the dyn-erased pair behaves like any other concrete iterator pair.
impl RQESuspendedIterator for BoxedRQESuspendedIterator {
    type Resumed<'a> = BoxedRQEIterator<'a>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let BoxedRQESuspendedIterator(inner) = *self;
        let (active, status) =
            <dyn RQEDynSuspendedIterator as RQEDynSuspendedIterator>::resume(inner, guard);
        (Box::new(active), status)
    }

    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }
}
