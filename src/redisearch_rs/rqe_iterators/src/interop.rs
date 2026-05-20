/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem;

use ffi::{
    IteratorStatus, IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND,
    IteratorStatus_ITERATOR_OK, IteratorStatus_ITERATOR_TIMEOUT, QueryIterator, ValidateStatus,
    t_docId,
};
use index_result::RSIndexResult;

use crate::{RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, SkipToOutcome};

/// A wrapper around a Rust iterator — i.e. an implementer of the
/// [`RQEIteratorBoxed`] trait.
///
/// It allows existing C code to invoke the Rust iterator as if it were a C iterator,
/// conforming to the existing C iterator conventions.
///
/// # Typestate
///
/// The wrapper holds the inner iterator inside a [`WrapperState`] tagged enum: it is
/// either [`Active`](WrapperState::Active) (live references into the index — safe to
/// call read/skip/rewind/etc.) or [`Suspended`](WrapperState::Suspended) (references
/// dropped, being held across a lock release). Transitions are driven by the
/// [`suspend`] and [`revalidate`] C callbacks, invoked by C at well-defined points
/// (right before lock release and right after lock re-acquisition).
///
/// If a read/skip/etc. callback is invoked while
/// [`Suspended`](WrapperState::Suspended), the call panics — defense-in-depth that
/// catches missing `Suspend` wiring on the C side rather than silently dereferencing
/// a dangling pointer.
///
/// # The `'index` lifetime
///
/// The wrapper is parametrized over `'index`, the lifetime of the data the inner
/// iterator borrows from. Inside the FFI boundary, every `Rf`-dependent field is a
/// [`SharedPtr`](ref_mode::SharedPtr) `=` `#[repr(transparent)] NonNull<T>` — there
/// are no real Rust references with this lifetime, only raw pointers. The lifetime
/// is therefore phantom in practice; the C protocol's spec read-lock discipline
/// guarantees the wrapper is freed before its data goes away.
///
/// Accessors that recover an `&'a RQEIteratorWrapper<'index, I>` from a raw header
/// pointer fabricate a fresh `'index` on each call — see
/// [`ref_from_header_ptr`](Self::ref_from_header_ptr) /
/// [`mut_ref_from_header_ptr`](Self::mut_ref_from_header_ptr).
///
/// # Invariants
///
/// 1. It is always safe to cast a raw [`QueryIterator`] pointer returned by
///    [`boxed_new`](Self::boxed_new) or [`boxed_new_compound`](Self::boxed_new_compound)
///    to an [`RQEIteratorWrapper`] pointer when invoking one of the callbacks stored
///    in the header.
#[repr(C)]
pub struct RQEIteratorWrapper<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    // The iterator header.
    // It *must* appear first for C-Rust interoperability to work as expected.
    header: QueryIterator,
    state: WrapperState<'index, I>,
}

/// Active/Suspended typestate for [`RQEIteratorWrapper`].
///
/// `Box<I>` and `Box<I::Suspended>` are byte-identical (`NonNull<T>` under the hood),
/// so the storage cost is just the enum discriminant (~8 bytes per wrapper). The
/// [`Empty`](Self::Empty) variant is a transient tombstone used by
/// [`take_active`](Self::take_active) / [`take_suspended`](Self::take_suspended)
/// during state transitions; it should only be observable inside a single callback
/// body.
enum WrapperState<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    Active(Box<I>),
    Suspended(Box<I::Suspended>),
    /// Transient tombstone, populated only while inside a suspend / resume callback
    /// body. If a panic during a transition lets this variant escape, [`Drop`] still
    /// cleans up correctly (it carries no resources).
    Empty,
}

impl<'index, I> WrapperState<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    /// Borrow the active iterator. Panics if the wrapper is not in
    /// [`Active`](Self::Active) state.
    fn active_ref(&self) -> &I {
        match self {
            Self::Active(active) => active,
            Self::Suspended(_) => panic!(
                "RQEIteratorWrapper accessed while in Suspended state — the C side must \
                 call the iterator's Revalidate callback before any read/skip/rewind/etc. call"
            ),
            Self::Empty => panic!("RQEIteratorWrapper accessed mid-transition (Empty state)"),
        }
    }

    /// Mutably borrow the active iterator. Panics if the wrapper is not in
    /// [`Active`](Self::Active) state.
    fn active_mut(&mut self) -> &mut I {
        match self {
            Self::Active(active) => active,
            Self::Suspended(_) => panic!(
                "RQEIteratorWrapper accessed while in Suspended state — the C side must \
                 call the iterator's Revalidate callback before any read/skip/rewind/etc. call"
            ),
            Self::Empty => panic!("RQEIteratorWrapper accessed mid-transition (Empty state)"),
        }
    }

    /// Replace the state with [`Empty`](Self::Empty) and return the previously-active
    /// [`Box<I>`]. Panics if the wrapper is not in [`Active`](Self::Active) state.
    fn take_active(&mut self) -> Box<I> {
        match mem::replace(self, Self::Empty) {
            Self::Active(active) => active,
            other => {
                // Restore so a follow-up `Drop` cleans up correctly.
                *self = other;
                panic!("expected Active state, found something else");
            }
        }
    }

    /// Replace the state with [`Empty`](Self::Empty) and return the previously-
    /// suspended [`Box<I::Suspended>`]. Panics if the wrapper is not in
    /// [`Suspended`](Self::Suspended) state.
    fn take_suspended(&mut self) -> Box<I::Suspended> {
        match mem::replace(self, Self::Empty) {
            Self::Suspended(suspended) => suspended,
            other => {
                *self = other;
                panic!("expected Suspended state, found something else");
            }
        }
    }
}

impl<'index, I> RQEIteratorWrapper<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    /// Borrow the inner active iterator. Panics if the wrapper has been suspended.
    pub fn inner(&self) -> &I {
        self.state.active_ref()
    }

    /// Mutably borrow the inner active iterator. Panics if the wrapper has been
    /// suspended.
    pub fn inner_mut(&mut self) -> &mut I {
        self.state.active_mut()
    }

    /// Re-synchronize the C header's `current` pointer from the inner iterator's
    /// [`RQEIterator::current`].
    ///
    /// Call this after any operation that may invalidate the previously stored
    /// `header.current` (e.g. replacing the inner variant in-place). Panics if the
    /// wrapper has been suspended.
    pub fn sync_current(&mut self) {
        let current = self
            .state
            .active_mut()
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
            .unwrap_or(std::ptr::null_mut());
        self.header.current = current;
    }

    /// Synchronize the header's `atEOF`, `lastDocId`, and `current` fields from the
    /// active inner iterator. Panics if the wrapper is not in
    /// [`Active`](WrapperState::Active) state.
    fn sync_header_from_active(&mut self) {
        let (at_eof, last_doc_id, current) = {
            let active = self.state.active_mut();
            let current = active
                .current()
                .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
                .unwrap_or(std::ptr::null_mut());
            (active.at_eof(), active.last_doc_id(), current)
        };
        self.header.atEOF = at_eof;
        self.header.lastDocId = last_doc_id;
        self.header.current = current;
    }

    /// Convert a type-erased iterator "header" into a wrapper around a specific Rust
    /// iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via
    ///    [`boxed_new`](Self::boxed_new) or [`boxed_new_compound`](Self::boxed_new_compound).
    /// 2. The caller must ensure that the provided header matches the expected Rust
    ///    iterator type.
    /// 3. The caller must ensure that it has a unique handle over the provided header.
    /// 4. The fabricated `'index` lifetime in the returned reference must not outlive
    ///    the data the inner iterator references via raw pointers. In practice, the
    ///    C protocol's spec read-lock discipline guarantees this for the standard
    ///    FFI flow.
    pub const unsafe fn mut_ref_from_header_ptr<'a>(
        base: *mut QueryIterator,
    ) -> &'a mut RQEIteratorWrapper<'index, I> {
        debug_assert!(!base.is_null());

        // SAFETY: Guaranteed by 1 + 2.
        let wrapper = unsafe { base.cast::<RQEIteratorWrapper<'index, I>>().as_mut() };

        if cfg!(debug_assertions) {
            wrapper.expect("Unexpected null pointer!")
        } else {
            // SAFETY: Guaranteed by 1.
            unsafe { wrapper.unwrap_unchecked() }
        }
    }

    /// Convert a type-erased iterator "header" into a wrapper around a specific Rust
    /// iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via
    ///    [`boxed_new`](Self::boxed_new) or [`boxed_new_compound`](Self::boxed_new_compound).
    /// 2. The caller must ensure that the provided header matches the expected Rust
    ///    iterator type.
    /// 3. The fabricated `'index` lifetime in the returned reference must not outlive
    ///    the data the inner iterator references via raw pointers. In practice, the
    ///    C protocol's spec read-lock discipline guarantees this for the standard
    ///    FFI flow.
    pub const unsafe fn ref_from_header_ptr<'a>(
        base: *const QueryIterator,
    ) -> &'a RQEIteratorWrapper<'index, I> {
        debug_assert!(!base.is_null());
        // SAFETY: Guaranteed by 1 + 2.
        unsafe {
            base.cast::<RQEIteratorWrapper<'index, I>>()
                .as_ref()
                .expect("Null pointer!")
        }
    }
}

impl<'index, I> RQEIteratorWrapper<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    /// Heap-allocate a wrapper with the given `ProfileChildren` callback.
    pub fn boxed_new_inner(
        inner: I,
        profile_children: Option<unsafe extern "C" fn(*mut QueryIterator) -> *mut QueryIterator>,
    ) -> *mut QueryIterator {
        let mut wrapper = Box::new(Self {
            header: QueryIterator {
                type_: inner.type_(),
                atEOF: inner.at_eof(),
                lastDocId: inner.last_doc_id(),
                current: std::ptr::null_mut(),
                NumEstimated: Some(num_estimated::<'index, I>),
                Read: Some(read::<'index, I>),
                SkipTo: Some(skip_to::<'index, I>),
                Revalidate: Some(revalidate::<'index, I>),
                Suspend: Some(suspend::<'index, I>),
                Free: Some(free_iterator::<'index, I>),
                Rewind: Some(rewind::<'index, I>),
                ProfileChildren: profile_children,
            },
            state: WrapperState::Active(Box::new(inner)),
        });
        if let Some(current) = wrapper
            .inner_mut()
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
        {
            wrapper.header.current = current;
        }
        Box::into_raw(wrapper) as *mut QueryIterator
    }

    /// Create a new C-compatible wrapper around a Rust iterator.
    ///
    /// The wrapper is placed on the heap. The `ProfileChildren` C callback is set to
    /// `None` — use [`boxed_new_compound`](Self::boxed_new_compound) for compound
    /// iterators that need the callback.
    pub fn boxed_new(inner: I) -> *mut QueryIterator {
        Self::boxed_new_inner(inner, None)
    }
}

/// Profiling support for Rust compound iterators exposed to C via [`RQEIteratorWrapper`].
///
/// # Why this exists
///
/// C code accesses compound iterator internals (children, structure) via type-specific
/// casts like `RQEIteratorWrapper::<Intersection<CRQEIterator>>::ref_from_header_ptr`.
/// This is used by the query optimizer, profile printing, and iterator mutation code.
///
/// For these casts to remain valid after profiling, the wrapper's type parameter must
/// stay the same. For example, `Intersection<CRQEIterator>` must remain
/// `Intersection<CRQEIterator>` after its children are profiled — not become
/// `Intersection<Box<dyn RQEIterator>>`.
///
/// This trait provides that guarantee: [`profile_children`](Self::profile_children)
/// returns `Self`, so the type is preserved through profiling. Each child is profiled
/// via [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled),
/// which returns `CRQEIterator` — preserving the child type parameter too.
///
/// This trait will be removed when the C code that accesses iterator internals
/// (profile printing, optimizer, mutation) is ported to Rust.
pub trait ProfileChildren<'index>: RQEIteratorBoxed<'index> + Sized + 'index {
    /// Profile all children, preserving the concrete iterator type.
    fn profile_children(self) -> Self;
}

impl<'index, I> RQEIteratorWrapper<'index, I>
where
    I: ProfileChildren<'index>,
{
    /// Create a new C-compatible wrapper around a compound Rust iterator.
    ///
    /// Sets the `ProfileChildren` C callback to [`rust_profile_children`], which
    /// calls [`ProfileChildren::profile_children`] to preserve the concrete type
    /// through profiling.
    pub fn boxed_new_compound(inner: I) -> *mut QueryIterator {
        debug_assert!(
            !inner.type_().is_leaf(),
            "boxed_new_compound should only be used for compound iterators"
        );
        let profile_children =
            Some(rust_profile_children::<'index, I> as unsafe extern "C" fn(_) -> _);
        Self::boxed_new_inner(inner, profile_children)
    }
}

extern "C" fn read<'index, I>(base: *mut QueryIterator) -> IteratorStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) };
    match wrapper.state.active_mut().read() {
        Ok(Some(result)) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_OK
        }
        Err(RQEIteratorError::TimedOut) => IteratorStatus_ITERATOR_TIMEOUT,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            wrapper.header.atEOF = true;
            IteratorStatus_ITERATOR_EOF
        }
    }
}

extern "C" fn skip_to<'index, I>(base: *mut QueryIterator, doc_id: t_docId) -> IteratorStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) };
    match wrapper.state.active_mut().skip_to(doc_id) {
        Ok(Some(SkipToOutcome::Found(result))) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_OK
        }
        Ok(Some(SkipToOutcome::NotFound(result))) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_NOTFOUND
        }
        Err(RQEIteratorError::TimedOut) => IteratorStatus_ITERATOR_TIMEOUT,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            wrapper.header.atEOF = true;
            IteratorStatus_ITERATOR_EOF
        }
    }
}

/// `Suspend` C callback. Transitions the wrapper from
/// [`Active`](WrapperState::Active) to [`Suspended`](WrapperState::Suspended).
///
/// Called by C immediately before the spec read lock is released. Panics if the
/// wrapper is not in [`Active`](WrapperState::Active) state (defense-in-depth — the
/// C protocol guarantees suspend is called exactly once per Active→Suspended
/// transition).
///
/// After this returns, `header.current` is nulled out: the previous result pointer
/// almost certainly pointed into the inverted-index data that may be invalidated
/// during the lock release / re-acquire cycle.
extern "C" fn suspend<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) };
    let active = wrapper.state.take_active();
    let suspended = <I as RQEIteratorBoxed>::suspend(active);
    wrapper.state = WrapperState::Suspended(suspended);
    // The pointer was almost certainly aimed at inverted-index data that may now
    // be invalidated. The C side must not dereference `header.current` while the
    // iterator is suspended; null it out so any stray read fails loudly.
    wrapper.header.current = std::ptr::null_mut();
}

/// `Revalidate` C callback. Drives the iterator's resume path: takes the
/// [`Suspended`](WrapperState::Suspended) box out, calls
/// [`RQESuspendedIterator::resume`] to re-acquire references into the index, and
/// stores the resulting [`Active`](WrapperState::Active) box back.
///
/// # The internal lifetime cast
///
/// `resume` returns `Box<<I::Suspended>::Resumed<'guard>>` where `'guard` is the
/// guard's borrow lifetime. The wrapper's [`WrapperState::Active`] variant holds
/// `Box<I>` where `I: RQEIteratorBoxed<'index>` — a different lifetime than
/// `'guard`. The trait surface does not pin
/// `<I::Suspended>::Resumed<'a> = I` (we deliberately keep the trait small), so
/// the assignment requires one localized unsafe cast.
///
/// The cast is sound because every iterator pair in this crate satisfies the
/// round-trip identity *structurally* (the active/suspended counterparts are
/// `Foo<Active<'a>, …>` / `Foo<Suspended, …>` over a single `#[repr(C)]` struct,
/// so `<<Foo<Active<'a>, …> as RQEIteratorBoxed<'a>>::Suspended as RQESuspendedIterator>::Resumed<'b>`
/// is byte-identical to `Foo<Active<'b>, …>`). The lifetime widening from `'guard`
/// to `'index` is the same `'static`-lie pattern documented on
/// [`RQEIteratorWrapper`] — all `Rf`-dependent fields are
/// [`SharedPtr`](ref_mode::SharedPtr) `=` `#[repr(transparent)] NonNull<T>`, so
/// no real reference is being widened.
extern "C" fn revalidate<'index, I>(base: *mut QueryIterator, spec: *mut ffi::IndexSpec) -> ValidateStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    debug_assert!(!spec.is_null());

    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) };

    // SAFETY: spec is a valid pointer (guaranteed by C caller).
    let spec_ref = unsafe { &*spec };

    // SAFETY:
    // - C has already acquired the read lock (see handleSpecLockAndRevalidate in result_processor.c).
    // - from_locked() returns ManuallyDrop to prevent lock release on drop;
    //   C is responsible for lock lifecycle via RedisSearchCtx_UnlockSpec.
    let guard = unsafe { index_spec::IndexSpecReadGuard::from_locked(spec_ref) };

    let suspended = wrapper.state.take_suspended();
    let (resumed, status) =
        <I::Suspended as RQESuspendedIterator>::resume(suspended, &guard);
    // SAFETY: see the doc-comment above. `Box<<I::Suspended>::Resumed<'guard>>`
    // is layout-identical to `Box<I>`: both heap-allocate the same `#[repr(C)]`
    // struct; the lifetime parameter is phantom in every `Rf`-dependent field.
    // Box::from_raw reuses the same heap allocation, so any external pointers
    // into the iterator's interior (composite aggregate results) stay valid.
    let active: Box<I> = unsafe { Box::from_raw(Box::into_raw(resumed) as *mut I) };
    wrapper.state = WrapperState::Active(active);
    wrapper.sync_header_from_active();
    status
}

extern "C" fn rewind<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) };
    wrapper.state.active_mut().rewind();
    wrapper.sync_header_from_active();
}

extern "C" fn num_estimated<'index, I>(base: *const QueryIterator) -> usize
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(base) };
    wrapper.state.active_ref().num_estimated()
}

/// [`ProfileChildren`] callback for composite Rust iterators wrapped in
/// [`RQEIteratorWrapper`].
///
/// Only set on non-leaf iterators via
/// [`boxed_new_compound`](RQEIteratorWrapper::boxed_new_compound). Consumes the
/// wrapper, calls [`ProfileChildren::profile_children`] on the inner iterator, and
/// re-wraps the result via `boxed_new_inner` with `ProfileChildren` set to `None`
/// — profiling is a one-shot pass.
///
/// Panics if the wrapper is not in [`Active`](WrapperState::Active) state —
/// profiling is only invoked on a freshly-constructed iterator tree, which is
/// always active.
extern "C" fn rust_profile_children<'index, I>(base: *mut QueryIterator) -> *mut QueryIterator
where
    I: ProfileChildren<'index>,
{
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Callbacks are guaranteed to get a header pointer created by
    // `RQEIteratorWrapper::boxed_new_compound`, which uses `Box::into_raw`.
    let it = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<'index, I>) };
    let inner = match it.state {
        WrapperState::Active(active) => *active,
        WrapperState::Suspended(_) => panic!(
            "rust_profile_children invoked while iterator was Suspended — profiling must run \
             while the iterator is active"
        ),
        WrapperState::Empty => {
            panic!("rust_profile_children invoked on a wrapper mid-transition (Empty state)")
        }
    };
    let profiled = inner.profile_children();
    // Re-wrap with `boxed_new_inner` instead of `boxed_new_compound`: profiling is
    // a one-shot pass, so the result's children are already profiled and the
    // `ProfileChildren` callback would never be called again.
    RQEIteratorWrapper::boxed_new_inner(profiled, None)
}

extern "C" fn free_iterator<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    if !base.is_null() {
        debug_assert!(base.is_aligned());
        // SAFETY: Callbacks are guaranteed to get a header pointer created by
        //  `RQEIteratorWrapper::boxed_new` or `boxed_new_compound`,
        //  which (internally) use `Box::into_raw` to return a raw header pointer.
        let _ = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<'index, I>) };
    }
}
