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
//! # Transitional shape
//!
//! During the first phrase of the revalidation work,
//! the new `RQEIteratorBoxed` trait is a **subtrait** of the legacy [`RQEIterator`]:
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
//! In the second phase of the revalidation work, the legacy [`RQEIterator`] trait will be
//! deleted entirely; its method signatures (sans `revalidate`) will be folded
//! directly into [`RQEIteratorBoxed`] / [`RQEDynIterator`], and
//! [`RQEIteratorBoxed`] will be renamed back to `RQEIterator`.

use ffi::{ValidateStatus, t_docId};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, c2rust,
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
pub trait RQEIteratorBoxed<'a>: RQEIterator<'a> + 'a {
    /// The suspended counterpart of this iterator. Carries no live
    /// references into the index and can therefore be held across a lock
    /// release/reacquire cycle.
    type Suspended: RQESuspendedIterator + 'static;

    /// Transition to the suspended state.
    fn suspend(self: Box<Self>) -> Box<Self::Suspended>;
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
    /// On success returns the active iterator alongside a [`ValidateStatus`]:
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
    ///
    /// Resume re-reads/seeks the index to restore position (mirroring the
    /// legacy [`RQEIterator::revalidate`]), so it can fail with an
    /// [`RQEIteratorError`] (e.g. [`IoError`](RQEIteratorError::IoError) or
    /// [`TimedOut`](RQEIteratorError::TimedOut)). On `Err` the suspended
    /// iterator is consumed and dropped.
    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> Result<(Box<Self::Resumed<'a>>, ValidateStatus), RQEIteratorError>;

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
    fn num_estimated(&self) -> usize {
        0
    }
}

/// Dyn-safe sibling of [`RQEIteratorBoxed`].
///
/// You shouldn't implement this trait by hand; the blanket
/// `impl<T: RQEIteratorBoxed<'a> + 'a> RQEDynIterator<'a> for T` below
/// produces it for every concrete iterator.
pub trait RQEDynIterator<'a>: RQEIterator<'a> + 'a {
    /// Type-erased counterpart of [`RQEIteratorBoxed::suspend`].
    fn suspend(self: Box<Self>) -> BoxedRQESuspendedIterator;
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
    ) -> Result<(BoxedRQEIterator<'a>, ValidateStatus), RQEIteratorError>;

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

impl BoxedRQESuspendedIterator {
    /// Wrap a concrete suspended iterator into the type-erased wrapper.
    pub fn new<S: RQESuspendedIterator>(iter: Box<S>) -> Self {
        Self(iter as Box<dyn RQEDynSuspendedIterator>)
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
///
/// Between the `ptr::read` and the matching `ptr::write` the slot is logically
/// uninitialised while the caller (and any composite that owns it) still
/// considers it live. `RQEIteratorBoxed::suspend` is a safe trait method that
/// may dispatch to arbitrary (including dyn) implementations and could panic;
/// if it did, unwinding past the uninitialised slot would let the owner drop a
/// moved-from value (double drop). To keep the window sound, a panic from
/// `suspend` is converted into a process abort rather than an unwind.
pub unsafe fn suspend_child_slot_in_place<'a, I>(slot: *mut I)
where
    I: RQEIteratorBoxed<'a> + 'a,
{
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
    // Slot reinitialised — disarm the abort guard.
    std::mem::forget(bomb);
}

// --- Blanket bridges: concrete → dyn-safe -----------------------------------

/// Bridge concrete active iterators into the dyn-safe sibling.
///
/// Only `suspend` is bridged here — the read/skip surface is inherited from
/// the legacy [`RQEIterator`] supertrait, which the
/// concrete iterator already implements.
impl<'a, T: RQEIteratorBoxed<'a> + 'a> RQEDynIterator<'a> for T {
    fn suspend(self: Box<Self>) -> BoxedRQESuspendedIterator {
        let suspended = <T as RQEIteratorBoxed<'a>>::suspend(self);
        BoxedRQESuspendedIterator(suspended as Box<dyn RQEDynSuspendedIterator>)
    }
}

/// Bridge concrete suspended iterators into the dyn-safe sibling.
impl<S: RQESuspendedIterator> RQEDynSuspendedIterator for S {
    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> Result<(BoxedRQEIterator<'a>, ValidateStatus), RQEIteratorError> {
        let (active, status) = <S as RQESuspendedIterator>::resume(self, guard)?;
        Ok((
            BoxedRQEIterator(active as Box<dyn RQEDynIterator<'a> + 'a>),
            status,
        ))
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
}

/// Forwarding [`RQESuspendedIterator`] impl on [`BoxedRQESuspendedIterator`]
/// so the dyn-erased pair behaves like any other concrete iterator pair.
impl RQESuspendedIterator for BoxedRQESuspendedIterator {
    type Resumed<'a> = BoxedRQEIterator<'a>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> Result<(Box<Self::Resumed<'a>>, ValidateStatus), RQEIteratorError> {
        let BoxedRQESuspendedIterator(inner) = *self;
        let (active, status) =
            <dyn RQEDynSuspendedIterator as RQEDynSuspendedIterator>::resume(inner, guard)?;
        Ok((Box::new(active), status))
    }

    fn last_doc_id(&self) -> t_docId {
        self.0.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.0.num_estimated()
    }
}
