/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{
    IteratorStatus, IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND,
    IteratorStatus_ITERATOR_OK, IteratorStatus_ITERATOR_TIMEOUT, QueryIterator, ValidateStatus,
    ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
    t_docId,
};
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

#[repr(C)]
/// A wrapper around a Rust iterator—i.e. an implementer of the [`RQEIterator`] trait.
///
/// It allows existing C code to invoke the Rust iterator
/// as if it were a C iterator, conforming to the existing C iterator conventions.
///
/// # Invariants
///
/// 1. It is always safe to cast a raw [`QueryIterator`] pointer returned by
///    [`RQEIteratorWrapper::boxed_new`] or [`RQEIteratorWrapper::boxed_new_compound`]
///    to an [`RQEIteratorWrapper`] pointer when invoking one of the callbacks stored in the header.
pub struct RQEIteratorWrapper<E> {
    // The iterator header.
    // It *must* appear first for C-Rust interoperability to work as expected.
    header: QueryIterator,
    pub inner: E,
}

impl<'index, I> RQEIteratorWrapper<I>
where
    I: RQEIterator<'index> + 'index,
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
                NumEstimated: Some(num_estimated::<I>),
                Read: Some(read::<I>),
                SkipTo: Some(skip_to::<I>),
                Revalidate: Some(revalidate::<I>),
                Free: Some(free_iterator::<I>),
                Rewind: Some(rewind::<I>),
                ProfileChildren: profile_children,
            },
            inner,
        });
        if let Some(current) = wrapper
            .inner
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
        {
            wrapper.header.current = current;
        }
        Box::into_raw(wrapper) as *mut QueryIterator
    }
}

impl<'index, I> RQEIteratorWrapper<I>
where
    I: RQEIterator<'index> + 'index,
{
    /// Create a new C-compatible wrapper around a Rust iterator.
    ///
    /// The wrapper is placed on the heap. The `ProfileChildren` C callback is
    /// set to `None` — use [`boxed_new_compound`](Self::boxed_new_compound) for
    /// compound iterators that need the callback.
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
/// 1. C calls `Profile_AddIters` → [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled)
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
pub trait ProfileChildren<'index>: RQEIterator<'index> + Sized + 'index {
    /// Profile all children, preserving the concrete iterator type.
    fn profile_children(self) -> Self;
}

impl<'index, I> RQEIteratorWrapper<I>
where
    I: ProfileChildren<'index>,
{
    /// Create a new C-compatible wrapper around a compound Rust iterator.
    ///
    /// Sets the `ProfileChildren` C callback to [`rust_profile_children`],
    /// which calls [`ProfileChildren::profile_children`]
    /// to preserve the concrete type through profiling.
    pub fn boxed_new_compound(inner: I) -> *mut QueryIterator {
        debug_assert!(
            !inner.type_().is_leaf(),
            "boxed_new_compound should only be used for compound iterators"
        );
        let profile_children = Some(rust_profile_children::<I> as unsafe extern "C" fn(_) -> _);
        Self::boxed_new_inner(inner, profile_children)
    }
}

impl<'index, I> RQEIteratorWrapper<I>
where
    I: RQEIterator<'index> + 'index,
{
    /// Re-synchronize the C header's `current` pointer from the inner
    /// iterator's [`RQEIterator::current`].
    ///
    /// Call this after any operation that may invalidate the previously stored
    /// `header.current` (e.g. replacing the inner variant in-place).
    pub fn sync_current(&mut self) {
        self.header.current = self
            .inner
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
            .unwrap_or(std::ptr::null_mut());
    }

    /// Convert a type-erased iterator "header" into a wrapper around a specific Rust iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via
    ///    [`RQEIteratorWrapper::boxed_new`] or [`RQEIteratorWrapper::boxed_new_compound`].
    /// 2. The caller must ensure that the provided header matches the expected Rust iterator type.
    /// 3. The caller must ensure that it has a unique handle over the provided header.
    pub const unsafe fn mut_ref_from_header_ptr(
        base: *mut QueryIterator,
    ) -> &'index mut RQEIteratorWrapper<I> {
        debug_assert!(!base.is_null());

        // SAFETY: Guaranteed by 1 + 2.
        let wrapper = unsafe { base.cast::<RQEIteratorWrapper<I>>().as_mut() };

        if cfg!(debug_assertions) {
            wrapper.expect("Unexpected null pointer!")
        } else {
            // SAFETY: Guaranteed by 1.
            unsafe { wrapper.unwrap_unchecked() }
        }
    }

    /// Convert a type-erased iterator "header into a wrapper around a specific Rust iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via
    ///    [`RQEIteratorWrapper::boxed_new`] or [`RQEIteratorWrapper::boxed_new_compound`].
    /// 2. The caller must ensure that the provided header matches the expected Rust iterator type.
    pub const unsafe fn ref_from_header_ptr(
        base: *const QueryIterator,
    ) -> &'index RQEIteratorWrapper<I> {
        debug_assert!(!base.is_null());
        // SAFETY: Guaranteed by 1 + 2.
        unsafe {
            base.cast::<RQEIteratorWrapper<I>>()
                .as_ref()
                .expect("Null pointer!")
        }
    }
}

extern "C" fn read<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
) -> IteratorStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    match wrapper.inner.read() {
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

extern "C" fn skip_to<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
    doc_id: t_docId,
) -> IteratorStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    match wrapper.inner.skip_to(doc_id) {
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

extern "C" fn revalidate<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
    spec: *mut ffi::IndexSpec,
) -> ValidateStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    debug_assert!(!spec.is_null());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    // SAFETY: The caller guarantees `spec` is a valid, non-null pointer to an `IndexSpec`
    // while the spec read lock is held.
    let spec = unsafe { NonNull::new_unchecked(spec) };
    // SAFETY: `spec` points to a valid `IndexSpec` while the spec read lock is held,
    // satisfying the safety requirements of `RQEIterator::revalidate`.
    match unsafe { wrapper.inner.revalidate(spec) } {
        Ok(RQEValidateStatus::Ok) => ValidateStatus_VALIDATE_OK,
        Ok(RQEValidateStatus::Moved { current }) => {
            if let Some(result) = current {
                wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
                wrapper.header.lastDocId = result.doc_id;
            } else {
                wrapper.header.atEOF = true;
            }
            ValidateStatus_VALIDATE_MOVED
        }
        Ok(RQEValidateStatus::Aborted) => ValidateStatus_VALIDATE_ABORTED,
        Err(_) => ValidateStatus_VALIDATE_ABORTED,
    }
}

extern "C" fn rewind<'index, I: RQEIterator<'index> + 'index>(base: *mut QueryIterator) {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    wrapper.inner.rewind();
    wrapper.header.lastDocId = wrapper.inner.last_doc_id();
    wrapper.header.atEOF = wrapper.inner.at_eof();
    wrapper.header.current = wrapper
        .inner
        .current()
        .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
        .unwrap_or(std::ptr::null_mut());
}

extern "C" fn num_estimated<'index, I: RQEIterator<'index> + 'index>(
    base: *const QueryIterator,
) -> usize {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::ref_from_header_ptr(base) };
    wrapper.inner.num_estimated()
}

/// [`ProfileChildren`] callback for composite Rust iterators wrapped in
/// [`RQEIteratorWrapper`].
///
/// Only set on non-leaf iterators via [`boxed_new_compound`](RQEIteratorWrapper::boxed_new_compound).
/// Consumes the wrapper, calls [`ProfileChildren::profile_children`]
/// on the inner iterator, and re-wraps the result via `boxed_new_inner` with
/// `ProfileChildren` set to `None` — profiling is a one-shot pass.
extern "C" fn rust_profile_children<'index, I: ProfileChildren<'index>>(
    base: *mut QueryIterator,
) -> *mut QueryIterator {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Callbacks are guaranteed to get a header pointer created by
    // `RQEIteratorWrapper::boxed_new_compound`, which uses `Box::into_raw`.
    let it = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<I>) };
    let profiled = it.inner.profile_children();
    // Re-wrap with `boxed_new_inner` instead of `boxed_new_compound`: profiling is
    // a one-shot pass, so the result's children are already profiled and the
    // `ProfileChildren` callback would never be called again.
    RQEIteratorWrapper::boxed_new_inner(profiled, None)
}

extern "C" fn free_iterator<'index, I: RQEIterator<'index> + 'index>(base: *mut QueryIterator) {
    if !base.is_null() {
        debug_assert!(base.is_aligned());
        // SAFETY: Callbacks are guaranteed to get a header pointer created by
        //  `RQEIteratorWrapper::boxed_new` or `boxed_new_compound`,
        //  which (internally) use `Box::into_raw` to return a raw header pointer.
        let _ = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<I>) };
    }
}
