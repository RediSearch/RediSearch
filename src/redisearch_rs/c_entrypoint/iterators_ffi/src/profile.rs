/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::QueryIterator;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    profile::{Profile, ProfileCounters},
};
use std::ptr::NonNull;

type ProfileIteratorImpl = Profile<'static, CRQEIterator>;

/// Create a new profile iterator.
///
/// # Safety
///
/// 1. `child` must be a valid non-null pointer to an implementation of the C query iterator API.
/// 2. `child` must not be aliased.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewProfileIterator(child: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!child.is_null(), "child must not be null");
    // SAFETY: 1.
    let child = unsafe { NonNull::new_unchecked(child) };
    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child) };
    RQEIteratorWrapper::boxed_new(Profile::new(child))
}

/// Get the child iterator from a profile iterator.
///
/// The returned pointer borrows from the iterator — it is valid as long as
/// the iterator is alive. The C caller only reads through this pointer.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer created by [`NewProfileIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileIterator_GetChild(
    it: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!it.is_null());
    debug_assert_eq!(
        // SAFETY: guaranteed by 1.
        unsafe { (*it).type_ },
        IteratorType::Profile,
        "Expected a profile iterator"
    );
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    let child: &QueryIterator = wrapper.inner.child();
    std::ptr::from_ref(child)
}

/// Get the profile counters from a profile iterator.
///
/// The returned pointer borrows from the iterator — it is valid as long as
/// the iterator is alive. The C caller only reads through this pointer.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer created by [`NewProfileIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileIterator_GetCounters(
    it: *const QueryIterator,
) -> *const ProfileCounters {
    debug_assert!(!it.is_null());
    debug_assert_eq!(
        // SAFETY: guaranteed by 1.
        unsafe { (*it).type_ },
        IteratorType::Profile,
        "Expected a profile iterator"
    );
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    let counters: &ProfileCounters = wrapper.inner.counters();
    std::ptr::from_ref(counters)
}

/// Get the accumulated wall time in nanoseconds from a profile iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer created by [`NewProfileIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileIterator_GetWallTimeNs(it: *const QueryIterator) -> u64 {
    debug_assert!(!it.is_null());
    debug_assert_eq!(
        // SAFETY: guaranteed by 1.
        unsafe { (*it).type_ },
        IteratorType::Profile,
        "Expected a profile iterator"
    );
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    wrapper.inner.wall_time_ns()
}

/// Profile-wrap an iterator and its entire subtree.
///
/// Wraps the iterator as a [`CRQEIterator`], calls
/// [`CRQEIterator::into_profiled`](rqe_iterators::c2rust::CRQEIterator::into_profiled)
/// (which recursively profiles all descendants), then returns the result
/// as a `QueryIterator*`.
///
/// # Safety
///
/// 1. `iter` must be a valid non-null pointer to an implementation of the C query iterator API.
/// 2. `iter` must not be aliased.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IntoProfiled(iter: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!iter.is_null(), "iter must not be null");
    // SAFETY: guaranteed by 1.
    let iter = unsafe { NonNull::new_unchecked(iter) };
    // SAFETY: guaranteed by 1 + 2.
    let iter = unsafe { CRQEIterator::new(iter) };
    iter.into_profiled().into_raw().as_ptr()
}
