/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    mem,
    ptr::NonNull,
    sync::atomic::{AtomicBool, Ordering},
};

use ffi::{
    IteratorStatus, IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND,
    IteratorStatus_ITERATOR_OK, IteratorStatus_ITERATOR_STOPPED, QueryIterator, ValidateStatus,
    ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
    ValidateStatus_VALIDATE_TIMEOUT,
};
use index_result::RSIndexResult;
use rqe_core::DocId;

use crate::{
    RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome, SkipToOutcome,
    profile_print::ProfilePrint, score_bound::MinCompetitiveScore,
};

/// Exposes a Rust iterator to C through the C iterator vtable.
///
/// # Suspension
///
/// The iterator is either active, borrowing into the index, or suspended, with those
/// borrows dropped while the spec lock is released. The [`suspend`] and [`revalidate`]
/// callbacks switch between the two. Reading from a suspended wrapper panics rather
/// than read index data that may have been freed.
///
/// # The `'index` lifetime
///
/// The iterator's index borrows are raw pointers ([`SharedPtr`](ref_mode::SharedPtr)),
/// so the compiler does not check `'index`: the spec read lock keeps the data alive,
/// and [`ref_from_header_ptr`](Self::ref_from_header_ptr) and
/// [`mut_ref_from_header_ptr`](Self::mut_ref_from_header_ptr) pick it freely.
///
/// # Invariants
///
/// 1. It is always safe to cast a raw [`QueryIterator`] pointer returned by
///    [`boxed_new`](Self::boxed_new) or [`boxed_new_compound`](Self::boxed_new_compound)
///    to an [`RQEIteratorWrapper`] pointer when invoking one of the callbacks stored
///    in the header.
/// 2. The inner iterator never moves, so every iterator must resume into the
///    allocation it was suspended in: C keeps pointers into it across lock cycles,
///    such as the slot [`own_key_ref`](crate::metric::own_key_ref) hands out.
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

/// Borrow of whichever form of the iterator a [`RQEIteratorWrapper`] currently
/// holds, returned by [`RQEIteratorWrapper::state`].
pub(crate) enum InnerState<'a, A, S> {
    /// The wrapper holds the active iterator, valid for reads and positioning.
    Active(&'a A),
    /// The wrapper holds the suspended iterator, which holds no index references.
    Suspended(&'a S),
}

/// Whether an [`RQEIteratorWrapper`]'s iterator is active or suspended.
enum WrapperState<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    Active(Box<I>),
    Suspended(Box<I::Suspended>),
    /// No iterator: mid-transition, or after a failed resume consumed it.
    Empty,
}

impl<'index, I> WrapperState<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    /// Borrow the active iterator. Panics otherwise.
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

    /// Mutably borrow the active iterator. Panics otherwise.
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

    /// Take the active iterator, leaving [`Empty`](Self::Empty). Panics otherwise.
    fn take_active(&mut self) -> Box<I> {
        match mem::replace(self, Self::Empty) {
            Self::Active(active) => active,
            other => {
                // Restored so that freeing the wrapper still drops it.
                *self = other;
                panic!("expected Active state, found something else");
            }
        }
    }

    /// Take the suspended iterator, leaving [`Empty`](Self::Empty). Panics otherwise.
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
    /// Borrow the iterator. Panics if it is suspended.
    pub fn inner(&self) -> &I {
        self.state.active_ref()
    }

    /// Mutably borrow the iterator. Panics if it is suspended.
    pub fn inner_mut(&mut self) -> &mut I {
        self.state.active_mut()
    }

    /// Borrow whichever form of the iterator is live, for callbacks C may invoke
    /// while the iterator is suspended, such as `FT.PROFILE` printing.
    ///
    /// Only read query-owned data through the suspended form: it holds no index
    /// references. Reading and positioning go through [`inner_mut`](Self::inner_mut),
    /// which panics on a suspended wrapper.
    pub(crate) fn state(&self) -> InnerState<'_, I, I::Suspended> {
        match &self.state {
            WrapperState::Active(active) => InnerState::Active(active),
            WrapperState::Suspended(suspended) => InnerState::Suspended(suspended),
            WrapperState::Empty => {
                panic!("RQEIteratorWrapper accessed mid-transition (Empty state)")
            }
        }
    }

    /// Point the header's `current` at the iterator's
    /// [`current`](crate::RQEIterator::current) result, after a change that may have moved
    /// it, such as replacing the iterator's variant in place. Panics if it is suspended.
    pub fn sync_current(&mut self) {
        let current = self
            .state
            .active_mut()
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
            .unwrap_or(std::ptr::null_mut());
        self.header.current = current;
    }

    /// Copy `atEOF`, `lastDocId` and `current` from the iterator into the header. Panics if
    /// it is suspended.
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
    /// 4. `'index` does not outlive the index data the iterator points into.
    pub const unsafe fn mut_ref_from_header_ptr<'a>(
        base: NonNull<QueryIterator>,
    ) -> &'a mut RQEIteratorWrapper<'index, I> {
        // SAFETY: 1 + 2 make `base` a live, aligned wrapper of this type; 3 makes the borrow
        // unique.
        unsafe { base.cast::<RQEIteratorWrapper<'index, I>>().as_mut() }
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
    /// 3. `'index` does not outlive the index data the iterator points into.
    pub const unsafe fn ref_from_header_ptr<'a>(
        base: NonNull<QueryIterator>,
    ) -> &'a RQEIteratorWrapper<'index, I> {
        // SAFETY: 1 + 2 make `base` a live, aligned wrapper of this type.
        unsafe { base.cast::<RQEIteratorWrapper<'index, I>>().as_ref() }
    }
}

impl<'index, I> RQEIteratorWrapper<'index, I>
where
    I: RQEIteratorBoxed<'index> + ProfilePrint + 'index,
    I::Suspended: RQESuspendedIterator<'index, Resumed<'index> = I>,
{
    /// Heap-allocate a wrapper with the given `ProfileChildren` callback.
    pub fn boxed_new_inner(
        inner: I,
        profile_children: Option<unsafe extern "C" fn(*mut QueryIterator) -> *mut QueryIterator>,
    ) -> NonNull<QueryIterator> {
        let wrapper = Box::new(Self {
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
                PrintProfile: Some(print_profile::<I>),
                ReportsScoreBounds: Some(reports_score_bounds::<I>),
                CurrentMaxScore: Some(current_max_score::<I>),
                CurrentBlockBound: Some(current_block_bound::<I>),
                ReadWithThreshold: Some(read_with_threshold::<I>),
                IntersectionSortWeight: Some(intersection_sort_weight::<I>),
            },
            state: WrapperState::Active(Box::new(inner)),
        });
        // Derive the self-referential `current` pointer only *after* the box is
        // stable behind a raw pointer. `sync_current` reads `inner` and writes
        // `header.current` through a single `&mut self`, so the borrow never
        // escapes across `Box::into_raw`'s `Unique` retag (which would pop it).
        let mut raw = NonNull::new(Box::into_raw(wrapper)).expect("Box pointers are non-null");
        // SAFETY: `raw` was just produced by `Box::into_raw`: initialized, aligned,
        // and — since nothing else has seen it yet — uniquely ours to borrow mutably.
        unsafe {
            raw.as_mut().sync_current();
        }
        raw.cast::<QueryIterator>()
    }

    /// Create a new C-compatible wrapper around a Rust iterator.
    ///
    /// The wrapper is placed on the heap. The `ProfileChildren` C callback is set to
    /// `None` — use [`boxed_new_compound`](Self::boxed_new_compound) for compound
    /// iterators that need the callback.
    pub fn boxed_new(inner: I) -> NonNull<QueryIterator> {
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
/// # How profiling flows
///
/// All Rust compound iterators are generic over their child type, and are
/// exposed to C as `RQEIteratorWrapper<Compound<CRQEIterator>>`.
/// Profiling must unwrap the *parent* compound iterator from its
/// [`RQEIteratorWrapper`], profile each [`CRQEIterator`](crate::c2rust::CRQEIterator)
/// child, then re-wrap the parent. Without this unwrap step,
/// [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled)
/// would only wrap the root in a [`Profile`](crate::profile::Profile) node —
/// children would not be recursively profiled, because `CRQEIterator` is
/// type-erased and has no way to reach a compound iterator's children without
/// recovering the concrete Rust type.
///
/// Concretely:
///
/// 1. The tree's root is profiled with [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled)
/// 2. `into_profiled` calls [`CRQEIterator::profile_children`](crate::c2rust::CRQEIterator::profile_children),
///    which invokes the C vtable `ProfileChildren` callback
/// 3. For Rust compound iterators, that callback is [`rust_profile_children`],
///    which unwraps the parent [`RQEIteratorWrapper`] (via `Box::from_raw`),
///    extracts the inner compound iterator, and calls this trait's
///    [`profile_children`](Self::profile_children)
/// 4. The implementation profiles each child via
///    [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled)
///    (preserving [`CRQEIterator`](crate::c2rust::CRQEIterator)) and returns the
///    same compound type
/// 5. The result is re-wrapped in a fresh [`RQEIteratorWrapper`] with the same type
///    parameter
///
/// This trait will be removed when the C code that accesses iterator internals
/// (profile printing, optimizer, mutation) is ported to Rust.
pub trait ProfileChildren<'index>: RQEIteratorBoxed<'index> + Sized + 'index {
    /// Profile all children, preserving the concrete iterator type.
    fn profile_children(self) -> Self;
}

impl<'index, I> RQEIteratorWrapper<'index, I>
where
    I: ProfileChildren<'index> + ProfilePrint,
    I::Suspended: RQESuspendedIterator<'index, Resumed<'index> = I>,
{
    /// Create a new C-compatible wrapper around a compound Rust iterator.
    ///
    /// Sets the `ProfileChildren` C callback to [`rust_profile_children`], which
    /// calls [`ProfileChildren::profile_children`] to preserve the concrete type
    /// through profiling.
    pub fn boxed_new_compound(inner: I) -> NonNull<QueryIterator> {
        debug_assert!(
            !inner.type_().is_leaf(),
            "boxed_new_compound should only be used for compound iterators"
        );
        let profile_children =
            Some(rust_profile_children::<'index, I> as unsafe extern "C" fn(_) -> _);
        Self::boxed_new_inner(inner, profile_children)
    }
}

/// Borrow the wrapper behind the header pointer a vtable callback receives.
///
/// A null `base` fails a debug assertion that reports the calling callback;
/// release builds do not check.
///
/// # Safety
///
/// 1. `base` satisfies invariant 1. in [`RQEIteratorWrapper`] for `I`, which
///    rules out null.
/// 2. The callback holds the wrapper exclusively for as long as the returned
///    reference is used.
/// 3. `'index` does not outlive the index data the iterator points into.
#[inline(always)]
#[track_caller]
unsafe fn wrapper_from_callback<'a, 'index, I>(
    base: *mut QueryIterator,
) -> &'a mut RQEIteratorWrapper<'index, I>
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(!base.is_null());
    // SAFETY: Guaranteed by 1., which rules out null.
    let base = unsafe { NonNull::new_unchecked(base) };
    // SAFETY: Guaranteed by 1., 2. and 3.
    unsafe { RQEIteratorWrapper::<'index, I>::mut_ref_from_header_ptr(base) }
}

extern "C" fn read<'index, I>(base: *mut QueryIterator) -> IteratorStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(base.is_aligned());
    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };
    let outcome = wrapper.state.active_mut().read();
    publish_read(&mut wrapper.header, outcome)
}

extern "C" fn read_with_threshold<'index, I: RQEIteratorBoxed<'index> + 'index>(
    base: *mut QueryIterator,
    min_competitive_score: f64,
) -> IteratorStatus {
    debug_assert!(base.is_aligned());
    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };
    // A threshold that does not apply could not drop anything, so a plain read is what it asks
    // for.
    let outcome = match MinCompetitiveScore::new(min_competitive_score) {
        Some(min_competitive_score) => wrapper
            .state
            .active_mut()
            .read_with_threshold(min_competitive_score),
        None => wrapper.state.active_mut().read(),
    };
    publish_read(&mut wrapper.header, outcome)
}

/// Report the outcome of a read in the C header, as `Read` and `ReadWithThreshold` require.
fn publish_read(
    header: &mut QueryIterator,
    outcome: Result<Option<&mut RSIndexResult<'_>>, RQEIteratorError>,
) -> IteratorStatus {
    match outcome {
        Ok(Some(result)) => {
            header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_OK
        }
        Err(RQEIteratorError::Stopped) => IteratorStatus_ITERATOR_STOPPED,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            header.atEOF = true;
            header.current = std::ptr::null_mut();
            IteratorStatus_ITERATOR_EOF
        }
    }
}

extern "C" fn skip_to<'index, I>(base: *mut QueryIterator, doc_id: DocId) -> IteratorStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(base.is_aligned());
    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };
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
        Err(RQEIteratorError::Stopped) => IteratorStatus_ITERATOR_STOPPED,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            wrapper.header.atEOF = true;
            wrapper.header.current = std::ptr::null_mut();
            IteratorStatus_ITERATOR_EOF
        }
    }
}

/// Debug switch behind `FT.DEBUG MOCK_REVALIDATE_TIMEOUT`: when set, every revalidation reports a
/// timeout without touching the iterator underneath.
///
/// A revalidation timeout is otherwise hard to stage from a test. It needs a deadline that expires
/// between two cursor reads *and* an index change that makes a composite re-seek its children, and
/// the blocked-client deadline that tests can fire on demand is the same flag the result processor
/// checks right after revalidating - so it hides the very case under test. This switch stands in
/// for the deadline so the flow tests can exercise what C does with the status.
static MOCK_REVALIDATE_TIMEOUT: AtomicBool = AtomicBool::new(false);

/// Set the [`MOCK_REVALIDATE_TIMEOUT`] debug switch.
pub fn set_mock_revalidate_timeout(enabled: bool) {
    MOCK_REVALIDATE_TIMEOUT.store(enabled, Ordering::Relaxed);
}

/// Read the [`MOCK_REVALIDATE_TIMEOUT`] debug switch.
pub fn mock_revalidate_timeout() -> bool {
    MOCK_REVALIDATE_TIMEOUT.load(Ordering::Relaxed)
}

/// `Suspend` C callback, called right before the spec read lock is released. Panics if the
/// iterator is already suspended.
extern "C" fn suspend<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(base.is_aligned());
    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };
    let active = wrapper.state.take_active();
    let suspended = <I as RQEIteratorBoxed>::suspend(active);
    wrapper.state = WrapperState::Suspended(suspended);
    // The result may point into index data freed while the lock is released, so a stray read
    // must fail loudly.
    wrapper.header.current = std::ptr::null_mut();
}

/// `Revalidate` C callback, called right after the spec read lock is re-acquired: resumes the
/// iterator and reports the outcome.
extern "C" fn revalidate<'index, I>(
    base: *mut QueryIterator,
    spec: *mut ffi::IndexSpec,
) -> ValidateStatus
where
    I: RQEIteratorBoxed<'index> + 'index,
    I::Suspended: RQESuspendedIterator<'index, Resumed<'index> = I>,
{
    debug_assert!(base.is_aligned());
    debug_assert!(!spec.is_null());

    // Revalidation runs once per query resume, never in the read loop, so the load costs nothing
    // worth measuring.
    if MOCK_REVALIDATE_TIMEOUT.load(Ordering::Relaxed) {
        return ValidateStatus_VALIDATE_TIMEOUT;
    }

    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };

    if matches!(wrapper.state, WrapperState::Active(_)) {
        // Not every unlock site calls `Suspend`, and the borrows may have gone stale while the
        // lock was released: run the whole cycle rather than trust them.
        let active = wrapper.state.take_active();
        wrapper.state = WrapperState::Suspended(<I as RQEIteratorBoxed>::suspend(active));
    }

    // SAFETY: spec is a valid pointer (guaranteed by C caller).
    let spec_ref = unsafe { &*spec };

    // SAFETY:
    // 1. `spec_ref` is a valid pointer to an `IndexSpec`.
    // 2. C has already acquired the read lock through `IndexSpec_LockRead`
    //    (see handleSpecLockAndRevalidate in result_processor.c).
    // 3. C releases it again through `IndexSpec_Unlock`.
    // 4. `IndexSpec_LockRead` pauses rehashing on the keys dict.
    // 5. `missing.indexes` is left fully rehashed on insert (see indexer.c).
    let guard: std::mem::ManuallyDrop<index_spec::IndexSpecReadGuard<'index>> =
        unsafe { index_spec::IndexSpecReadGuard::from_locked(spec_ref) };

    let suspended = wrapper.state.take_suspended();
    let suspended_at: *const I::Suspended = &*suspended;
    // On an abort or an error, `resume` consumed the iterator: the wrapper stays empty until C
    // frees it.
    let outcome = <I::Suspended as RQESuspendedIterator>::resume(suspended, &guard);
    let (resumed, status) = match outcome {
        Ok(ResumeOutcome::Ok(resumed)) => (resumed, ValidateStatus_VALIDATE_OK),
        Ok(ResumeOutcome::Moved(resumed)) => (resumed, ValidateStatus_VALIDATE_MOVED),
        Ok(ResumeOutcome::Aborted) => return ValidateStatus_VALIDATE_ABORTED,
        // Both errors leave the iterator in an indeterminate state, so C frees it either way. They
        // are reported apart because they say different things about the result set: a timeout
        // makes it partial and has to reach the client, an I/O error kills this subtree in a query
        // that still has time left.
        Err(RQEIteratorError::Stopped) => return ValidateStatus_VALIDATE_TIMEOUT,
        Err(RQEIteratorError::IoError(_)) => return ValidateStatus_VALIDATE_ABORTED,
    };
    debug_assert_resumed_in_place(suspended_at, &*resumed);
    wrapper.state = WrapperState::Active(resumed);
    wrapper.sync_header_from_active();
    status
}

/// Assert invariant 2. of [`RQEIteratorWrapper`]: `resumed` lives where the iterator was
/// suspended.
#[track_caller]
fn debug_assert_resumed_in_place<S, I>(suspended_at: *const S, resumed: &I) {
    debug_assert!(
        std::ptr::addr_eq(suspended_at, resumed),
        "the iterator resumed into a new allocation, leaving C's pointers into it dangling"
    );
}

extern "C" fn rewind<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(base.is_aligned());
    // SAFETY: 1. by invariant 1. of [`RQEIteratorWrapper`]. 2. C runs one callback at a time on
    // an iterator it owns. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe { wrapper_from_callback::<'_, 'index, I>(base) };
    wrapper.state.active_mut().rewind();
    wrapper.sync_header_from_active();
}

/// `NumEstimated` C callback. Returns the inner iterator's estimate in whichever
/// typestate it is in, so FT.PROFILE introspection works after the iterator has
/// been suspended at the unlock site.
extern "C" fn num_estimated<'index, I>(base: *const QueryIterator) -> usize
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    debug_assert!(base.is_aligned());
    // SAFETY: 1. and 2. by invariant 1. of [`RQEIteratorWrapper`]. 3. C frees the iterator
    // before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    match wrapper.state() {
        InnerState::Active(it) => it.num_estimated(),
        InnerState::Suspended(it) => it.num_estimated(),
    }
}

extern "C" fn reports_score_bounds<'index, I: RQEIteratorBoxed<'index> + 'index>(
    base: *const QueryIterator,
) -> bool {
    debug_assert!(base.is_aligned());
    // SAFETY: 1. and 2. by invariant 1. of [`RQEIteratorWrapper`]. 3. C frees the iterator
    // before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    wrapper.state.active_ref().reports_score_bounds()
}

extern "C" fn current_max_score<'index, I: RQEIteratorBoxed<'index> + 'index>(
    base: *const QueryIterator,
) -> f64 {
    debug_assert!(base.is_aligned());
    // SAFETY: 1. and 2. by invariant 1. of [`RQEIteratorWrapper`]. 3. C frees the iterator
    // before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    wrapper.state.active_ref().current_max_score()
}

/// Panics while the iterator is suspended, like [`num_estimated`]: an intersection reads its
/// children's weights only while it is being built, and a child being built into a tree is active.
extern "C" fn intersection_sort_weight<'index, I: RQEIteratorBoxed<'index> + 'index>(
    base: *const QueryIterator,
    prioritize_union_children: bool,
) -> f64 {
    debug_assert!(base.is_aligned());
    // SAFETY: 1. and 2. by invariant 1. of [`RQEIteratorWrapper`]. 3. C frees the iterator
    // before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    wrapper
        .state
        .active_ref()
        .intersection_sort_weight(prioritize_union_children)
}

extern "C" fn current_block_bound<'index, I: RQEIteratorBoxed<'index> + 'index>(
    base: *const QueryIterator,
    last_doc_id: *mut DocId,
    max_score: *mut f64,
) -> bool {
    debug_assert!(base.is_aligned());
    // SAFETY: 1. and 2. by invariant 1. of [`RQEIteratorWrapper`]. 3. C frees the iterator
    // before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    let Some(block) = wrapper.state.active_ref().current_block_bound() else {
        return false;
    };
    debug_assert!(last_doc_id.is_aligned() && max_score.is_aligned());
    // SAFETY: The `CurrentBlockBound` contract has the caller pass valid, writable outputs.
    unsafe { last_doc_id.write(block.last_doc_id) };
    // SAFETY: As above.
    unsafe { max_score.write(block.max_score) };
    true
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
/// Panics if the iterator is suspended.
pub(crate) extern "C" fn rust_profile_children<'index, I>(
    base: *mut QueryIterator,
) -> *mut QueryIterator
where
    I: ProfileChildren<'index> + ProfilePrint,
    I::Suspended: RQESuspendedIterator<'index, Resumed<'index> = I>,
{
    debug_assert!(base.is_aligned());
    debug_assert!(!base.is_null());
    // SAFETY: As a header-stored callback (invariant 1. in [`RQEIteratorWrapper`]),
    // `base` is a live header, so it is non-null.
    let base = unsafe { NonNull::new_unchecked(base) };
    // SAFETY:
    // 1. As a header-stored callback (invariant 1. in [`RQEIteratorWrapper`]),
    //    `base` is an aligned, live header, uniquely owned until it is consumed
    //    into the `Box` below.
    // 2. `SkipTo` is `Copy`, so reading it through a shared reference leaves
    //    `base` intact for that `Box::from_raw`.
    let had_no_skip_to = unsafe { (*base.as_ptr()).SkipTo.is_none() };
    // SAFETY: Callbacks are guaranteed to get a header pointer created by
    // `RQEIteratorWrapper::boxed_new_compound`, so it owns a `Box` allocation of
    // the wrapper around `I`.
    let it = unsafe { Box::from_raw(base.cast::<RQEIteratorWrapper<'index, I>>().as_ptr()) };
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
    let ptr = RQEIteratorWrapper::boxed_new_inner(profiled, None);
    if had_no_skip_to {
        // Preserve a root-only iterator's cleared `SkipTo` (top_k is the only
        // user today) across this rebox.
        // SAFETY: `ptr` was just produced above and has no other alias yet.
        unsafe { patch_vtable(ptr, |h| h.SkipTo = None) };
    }
    ptr.as_ptr()
}

extern "C" fn free_iterator<'index, I>(base: *mut QueryIterator)
where
    I: RQEIteratorBoxed<'index> + 'index,
{
    if !base.is_null() {
        debug_assert!(base.is_aligned());
        // SAFETY: Callbacks are guaranteed to get a header pointer created by
        //  `RQEIteratorWrapper::boxed_new` or `boxed_new_compound`, so it owns a
        //  `Box` allocation of the wrapper around `I`.
        let _ = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<'index, I>) };
    }
}

/// `PrintProfile` vtable callback.
///
/// # Safety
///
/// - `base` must be a valid pointer to a [`QueryIterator`] created by
///   [`RQEIteratorWrapper::boxed_new`] or [`RQEIteratorWrapper::boxed_new_compound`]
///   with inner type `I`.
/// - `map` must be a valid pointer to a [`redis_reply::MapBuilder`].
/// - `ctx` must be a valid pointer to a [`ProfilePrintCtx`](crate::profile_print::ProfilePrintCtx).
unsafe extern "C" fn print_profile<'index, I: ProfilePrint + RQEIteratorBoxed<'index> + 'index>(
    base: *const QueryIterator,
    map: *mut ffi::RsMapBuilder,
    ctx: *mut ffi::RsProfilePrintCtx,
) {
    debug_assert!(!map.is_null());
    debug_assert!(!ctx.is_null());
    // SAFETY: 1. and 2. `base` was created by `boxed_new`/`boxed_new_compound` with inner type
    // `I`, per this function's contract. 3. C frees the iterator before the index data it borrows.
    let wrapper = unsafe {
        RQEIteratorWrapper::<'index, I>::ref_from_header_ptr(
            NonNull::new(base.cast_mut()).expect("`base` must not be null"),
        )
    };
    // SAFETY: map is a valid &mut MapBuilder per precondition.
    let map = unsafe { &mut *(map as *mut redis_reply::MapBuilder<'_>) };
    // SAFETY: ctx is a valid &mut ProfilePrintCtx per precondition.
    let ctx = unsafe { &mut *(ctx as *mut crate::profile_print::ProfilePrintCtx<'_>) };
    // `FT.PROFILE` prints the tree after the root iterator was suspended at the
    // unlock site.
    match wrapper.state() {
        InnerState::Active(it) => it.print_profile(map, ctx),
        InnerState::Suspended(it) => it.print_profile(map, ctx),
    }
}

/// Apply a post-construction mutation to the vtable of a freshly-boxed iterator.
///
/// This is an escape hatch for clearing or overriding vtable entries that
/// `boxed_new` / `boxed_new_compound` set unconditionally. The closure receives
/// a mutable reference to the [`QueryIterator`] header and may set any field.
///
/// # Safety
///
/// 1. `ptr` is a valid [`NonNull<QueryIterator>`](NonNull) produced by a
///    [`RQEIteratorWrapper`] constructor in this module.
/// 2. No other alias to `ptr` is live for the duration of `f`.
pub unsafe fn patch_vtable(mut ptr: NonNull<QueryIterator>, f: impl FnOnce(&mut QueryIterator)) {
    // SAFETY: upheld by the caller
    unsafe { f(ptr.as_mut()) }
}
