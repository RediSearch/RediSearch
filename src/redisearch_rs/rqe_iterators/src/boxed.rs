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
//! This module defines the suspend/resume trait hierarchy that replaced the
//! legacy `revalidate` method design. An iterator is either *active* (holding
//! live references into the index) or *suspended* (a passive carrier that can
//! be held across an index read-lock release); each state has a concrete,
//! type-state-preserving trait and a dyn-safe sibling for type erasure:
//!
//! | Concept              | Concrete (type-state preserved)   | Dyn-safe sibling                    |
//! |----------------------|-----------------------------------|-------------------------------------|
//! | Active iterator      | [`RQEIterator`]                   | [`RQEDynIterator`]                  |
//! | Suspended iterator   | [`RQESuspendedIterator`]          | [`RQEDynSuspendedIterator`]         |
//! | Erasure wrapper type | [`TypeErasedRQEIterator`]         | [`TypeErasedRQESuspendedIterator`]  |
//!
//! Implementers only provide the *concrete* traits
//! ([`RQEIterator`] — defined in the crate root — and [`RQESuspendedIterator`]);
//! blanket bridge impls in this module produce the corresponding
//! [`RQEDynIterator`] / [`RQEDynSuspendedIterator`] implementations
//! automatically.
//!
//! The receiver shape (`self: Box<Self>`) is what lets the suspend/resume body
//! reinterpret the heap allocation byte-identically — see
//! [`RQEIterator::suspend`] for the intended idiom.
//!
//! # Object safety
//!
//! [`RQEIterator`] carries an associated `Suspended` type, so `dyn RQEIterator`
//! is not a legal type. [`RQEDynIterator`] is therefore the sole dyn-safe trait:
//! it redeclares the object-safe read/skip surface and replaces the concrete
//! `suspend` (`-> Box<Self::Suspended>`) with a type-erased one
//! (`-> TypeErasedRQESuspendedIterator`). Trait objects and dynamic children go
//! through `dyn RQEDynIterator`, never `dyn RQEIterator`.

use ffi::t_docId;
use index_result::RSIndexResult;

use crate::{IteratorType, RQEIterator, RQEIteratorError, ResumeOutcome, SkipToOutcome, c2rust};
use index_spec::IndexSpecReadGuard;

/// Concrete-typed suspended iterator trait — counterpart of
/// [`RQEIterator`].
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
///
/// It is also [`ProfilePrint`](crate::profile_print::ProfilePrint) — symmetric
/// with [`RQEIterator`] — so `FT.PROFILE` introspection can print a suspended
/// iterator's profile tree without resuming it (see
/// [`crate::interop::RQEIteratorWrapper`]'s `print_profile` callback). The
/// profile fields (counters, wall-time, child accessors) are stored on the
/// mode-independent `Raw…` structs, so the same `ProfilePrint` impl serves the
/// active and suspended counterparts.
pub trait RQESuspendedIterator<'query>: crate::profile_print::ProfilePrint {
    /// The active counterpart this iterator resumes into, parameterised by
    /// the lifetime of the freshly re-acquired read guard.
    ///
    /// `'query: 'index` is required because the retained query-pipeline
    /// borrows must outlive the (shorter) guard window the iterator is
    /// resumed into.
    type Resumed<'index>: RQEIterator<'index>
    where
        'query: 'index;

    /// Resume from the suspended state, re-acquiring references into the
    /// index and re-validating the iterator's state against any changes
    /// that happened while the iterator was suspended.
    ///
    /// Returns a [`ResumeOutcome`] describing how the position was restored:
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
    /// Resume re-reads/seeks the index to restore position, so it can fail with an
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

/// Dyn-safe sibling of [`RQEIterator`].
///
/// This is the **only** dyn-safe trait in the hierarchy. [`RQEIterator`] carries
/// an associated `Suspended` type, so `dyn RQEIterator` is impossible; instead,
/// `RQEDynIterator` redeclares the object-safe read/skip surface and pairs it
/// with a *type-erased* `suspend` (returning [`TypeErasedRQESuspendedIterator`]
/// rather than a concrete `Box<Self::Suspended>`). Trait objects and dynamic
/// children therefore go through `dyn RQEDynIterator`, never `dyn RQEIterator`.
///
/// You shouldn't implement this trait by hand; the blanket
/// `impl<T: RQEIterator<'index> + 'index> RQEDynIterator<'index> for T` below
/// produces it for every concrete iterator by forwarding to its
/// [`RQEIterator`] impl.
pub trait RQEDynIterator<'index>: crate::profile_print::ProfilePrint + 'index {
    /// Object-safe counterpart of [`RQEIterator::current`].
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>>;
    /// Object-safe counterpart of [`RQEIterator::read`].
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>;
    /// Object-safe counterpart of [`RQEIterator::skip_to`].
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>;
    /// Object-safe counterpart of [`RQEIterator::rewind`].
    fn rewind(&mut self);
    /// Object-safe counterpart of [`RQEIterator::num_estimated`].
    fn num_estimated(&self) -> usize;
    /// Object-safe counterpart of [`RQEIterator::last_doc_id`].
    fn last_doc_id(&self) -> t_docId;
    /// Object-safe counterpart of [`RQEIterator::at_eof`].
    fn at_eof(&self) -> bool;
    /// Object-safe counterpart of [`RQEIterator::type_`].
    fn type_(&self) -> IteratorType;
    /// Object-safe counterpart of [`RQEIterator::as_c_iterator`].
    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator>;
    /// Object-safe counterpart of [`RQEIterator::intersection_sort_weight`].
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64;

    /// Type-erased counterpart of [`RQEIterator::suspend`].
    fn suspend(self: Box<Self>) -> TypeErasedRQESuspendedIterator<'index>;
}

/// Dyn-safe sibling of [`RQESuspendedIterator`].
///
/// As with [`RQEDynIterator`], implementers don't write this directly — the
/// blanket bridge below produces it from any
/// `T: RQESuspendedIterator`.
///
/// It is [`ProfilePrint`](crate::profile_print::ProfilePrint) (via the
/// `RQESuspendedIterator: ProfilePrint` supertrait, forwarded by the blanket
/// bridge) so a type-erased suspended child can print its profile.
pub trait RQEDynSuspendedIterator<'query>: crate::profile_print::ProfilePrint {
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
/// implements [`RQEIterator`] so composites can
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
    pub fn new<I: RQEIterator<'index> + 'index>(iter: Box<I>) -> Self {
        Self(iter as Box<dyn RQEDynIterator<'index> + 'index>)
    }
}

impl<'query> TypeErasedRQESuspendedIterator<'query> {
    /// Wrap a concrete suspended iterator into the type-erased wrapper.
    pub fn new<S: RQESuspendedIterator<'query> + 'query>(iter: Box<S>) -> Self {
        Self(iter as Box<dyn RQEDynSuspendedIterator<'query> + 'query>)
    }
}

/// Suspend a single child slot in place: read the value out, call its
/// [`RQEIterator::suspend`] through the trait, and write the suspended
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
/// * `I` and `I::Suspended` must have the same size and alignment — guaranteed
///   for all `RQEIterator` impls in this crate by their `#[repr(C)]`
///   layouts over `SharedPtr`/fat-pointer fields.
///
/// Between the `ptr::read` and the matching `ptr::write` the slot is logically
/// uninitialised while the caller (and any composite that owns it) still
/// considers it live. `RQEIterator::suspend` is a safe trait method that
/// may dispatch to arbitrary (including dyn) implementations and could panic;
/// if it did, unwinding past the uninitialised slot would let the owner drop a
/// moved-from value (double drop). To keep the window sound, a panic from
/// `suspend` is converted into a process abort rather than an unwind.
pub unsafe fn suspend_child_slot_in_place<'index, I>(slot: *mut I)
where
    I: RQEIterator<'index> + 'index,
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
    // Dispatches via:
    // - the vtable for dyn-erased `I` (e.g. `TypeErasedRQEIterator`);
    // - a transmute at the leaf level for concrete `I`.
    // Either way the *inner* concrete iterator's heap allocation is preserved.
    // Only the outer wrapper bytes may differ (and the wrapper's address doesn't
    // matter, see [`crate::interop::revalidate`] for the rationale).
    let suspended = *<I as RQEIterator<'index>>::suspend(Box::new(active));
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
/// The read/skip surface is forwarded method-by-method to the concrete
/// iterator's [`RQEIterator`] impl, and `suspend` is type-erased into a
/// [`TypeErasedRQESuspendedIterator`]. Forwarding (rather than inheriting via a
/// supertrait) is what keeps `RQEDynIterator` free of `RQEIterator`'s associated
/// `Suspended` type, so `dyn RQEDynIterator` stays object-safe.
impl<'index, T: RQEIterator<'index> + 'index> RQEDynIterator<'index> for T {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        <T as RQEIterator<'index>>::current(self)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        <T as RQEIterator<'index>>::read(self)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        <T as RQEIterator<'index>>::skip_to(self, doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        <T as RQEIterator<'index>>::rewind(self)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        <T as RQEIterator<'index>>::num_estimated(self)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        <T as RQEIterator<'index>>::last_doc_id(self)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        <T as RQEIterator<'index>>::at_eof(self)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        <T as RQEIterator<'index>>::type_(self)
    }

    #[inline(always)]
    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        <T as RQEIterator<'index>>::as_c_iterator(self)
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        <T as RQEIterator<'index>>::intersection_sort_weight(self, prioritize_union_children)
    }

    #[inline(always)]
    fn suspend(self: Box<Self>) -> TypeErasedRQESuspendedIterator<'index> {
        let suspended = <T as RQEIterator<'index>>::suspend(self);
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
/// `I` type parameter of composite iterators (which bound on [`RQEIterator`]),
/// and participate in the suspend/resume surface (its `Suspended` counterpart
/// is [`TypeErasedRQESuspendedIterator`]).
impl<'index> RQEIterator<'index> for TypeErasedRQEIterator<'index> {
    type Suspended = TypeErasedRQESuspendedIterator<'index>;

    #[inline(always)]
    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let TypeErasedRQEIterator(inner) = *self;
        Box::new(<dyn RQEDynIterator<'index> as RQEDynIterator<'index>>::suspend(inner))
    }

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

/// Forwarding [`crate::profile_print::ProfilePrint`] impl so [`TypeErasedRQEIterator`] satisfies the
/// `ProfilePrint` bound (also a supertrait of [`RQEDynIterator`]).
///
/// Delegates to the erased concrete iterator's own `print_profile` through the
/// [`RQEDynIterator`] vtable, so `FT.PROFILE` reports the real iterator instead
/// of a generic placeholder.
impl crate::profile_print::ProfilePrint for TypeErasedRQEIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut crate::profile_print::ProfilePrintCtx<'_>,
    ) {
        self.0.print_profile(map, ctx);
    }
}

/// Forwarding [`crate::profile_print::ProfilePrint`] impl so
/// [`TypeErasedRQESuspendedIterator`] satisfies the `ProfilePrint` bound
/// required by the `RQESuspendedIterator: ProfilePrint` supertrait.
///
/// Delegates to the erased concrete suspended iterator's own `print_profile`
/// through the [`RQEDynSuspendedIterator`] vtable, so `FT.PROFILE` reports the
/// real iterator even when the tree is suspended at the unlock site.
impl crate::profile_print::ProfilePrint for TypeErasedRQESuspendedIterator<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut crate::profile_print::ProfilePrintCtx<'_>,
    ) {
        self.0.print_profile(map, ctx);
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
