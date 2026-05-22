/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test utilities for rqe_iterators.
//!
//! This module provides utilities for testing iterators, including contexts
//! for setting up test environments.

#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod mock_context;
#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod test_context;

pub use mock_context::MockContext;
pub use test_context::{GlobalGuard, TestContext};

/// Drive a suspend/resume cycle on `it` under the given lock guard.
///
/// Mirrors the production FFI `revalidate` callback (see
/// `rqe_iterators::interop::revalidate`): box-suspend → resume → cast back to
/// the same active type. Tests use this to exercise the canonical
/// suspend/resume path during the in-progress migration away from
/// `RQEIterator::revalidate`.
///
/// # Safety contract
///
/// The cast `Box<T::Suspended::Resumed<'a>> → Box<T>` relies on the
/// layout-compatibility guarantee documented on
/// [`rqe_iterators::RQEIterator::suspend`]: active and suspended forms
/// of an iterator are `#[repr(C)]` over `SharedPtr` fields, so the heap
/// allocation can be relabelled between modes without copying or
/// reallocating. All iterators in `rqe_iterators` honour this contract, and
/// the FFI wrapper uses the same cast (see
/// `rqe_iterators::interop::revalidate`).
pub fn revalidate_via_resume<'borrow, 'spec, T>(
    mut it: Box<T>,
    spec: &'borrow index_spec::IndexSpecReadGuard<'spec>,
) -> (Box<T>, ffi::ValidateStatus)
where
    T: rqe_iterators::RQEIterator<'spec> + 'spec,
{
    // Cascade-suspend first: this flips the typestate of any nested
    // trait-object children (e.g. `BoxedRQEIterator` wrapping a `Box<dyn
    // RQEDynIterator>`) by going through their vtable. Without this,
    // `suspend`'s whole-box cast would relabel composite bytes as
    // `Suspended` without actually transitioning the dyn-erased children,
    // causing the suspended children to keep active vtables and produce
    // UB on subsequent resume calls. Mirrors what the FFI wrapper does
    // via `it->Suspend(it)` before each lock release.
    it.cascade_suspend();
    let suspended = <T as rqe_iterators::RQEIterator<'spec>>::suspend(it);
    let (resumed, status) =
        <T::Suspended as rqe_iterators::RQESuspendedIterator>::resume(suspended, spec);
    // SAFETY: `T` and `<T::Suspended as RQESuspendedIterator>::Resumed<'_>`
    // are layout-identical by the `RQEIterator::suspend` contract.
    // The cast also coerces the resumed iterator's lifetime back to the
    // input's `'spec`; that's sound because iterator types are covariant in
    // their lifetime parameter (the lifetime is purely phantom via
    // `ref_mode::Active<'a>`), so widening from the resume's chosen `'a`
    // (which the compiler picks from the borrow lifetime) to `'spec` (which
    // is at least as long) doesn't extend any real borrow.
    let active: Box<T> = unsafe { Box::from_raw(Box::into_raw(resumed) as *mut T) };
    (active, status)
}
