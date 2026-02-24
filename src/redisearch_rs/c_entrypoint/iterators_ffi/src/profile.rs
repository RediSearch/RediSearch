/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IteratorType_PROFILE_ITERATOR, QueryIterator};
use rqe_iterators::profile::{Profile, ProfileCounters};
use rqe_iterators_interop::RQEIteratorWrapper;
use std::ffi::c_int;
use std::ptr::NonNull;

use crate::c2rust::CRQEIterator;

type ProfileIteratorImpl = Profile<'static, CRQEIterator>;

#[unsafe(no_mangle)]
/// Create a new profile iterator.
///
/// # Safety
///
/// 1. `child` must be a valid non-null pointer to an implementation of the C query iterator API.
/// 2. `child` must not be aliased.
pub unsafe extern "C" fn NewProfileIterator(child: *mut QueryIterator) -> *mut QueryIterator {
    let child = NonNull::new(child)
        .expect("Trying to create a profile iterator using a NULL child iterator pointer");
    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child) };
    RQEIteratorWrapper::boxed_new(IteratorType_PROFILE_ITERATOR, Profile::new(child))
}

/// Get the child iterator from a profile iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer created by [`NewProfileIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileIterator_GetChild(it: *mut QueryIterator) -> *mut QueryIterator {
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    let child: &QueryIterator = &*wrapper.inner.child();
    std::ptr::from_ref(child).cast_mut()
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
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    wrapper.inner.counters()
}

/// Get the accumulated wall time in nanoseconds from a profile iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer created by [`NewProfileIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileIterator_GetWallTimeNs(it: *const QueryIterator) -> u64 {
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { RQEIteratorWrapper::<ProfileIteratorImpl>::ref_from_header_ptr(it) };
    wrapper.inner.wall_time_ns()
}

/// Get the read count from profile counters.
///
/// # Safety
///
/// 1. `c` must be a valid non-null pointer obtained from [`ProfileIterator_GetCounters`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileCounters_GetReadCount(c: *const ProfileCounters) -> usize {
    // SAFETY: guaranteed by 1.
    let counters = unsafe { &*c };
    counters.read
}

/// Get the skip-to count from profile counters.
///
/// # Safety
///
/// 1. `c` must be a valid non-null pointer obtained from [`ProfileIterator_GetCounters`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileCounters_GetSkipToCount(c: *const ProfileCounters) -> usize {
    // SAFETY: guaranteed by 1.
    let counters = unsafe { &*c };
    counters.skip_to
}

/// Get the EOF flag from profile counters.
///
/// # Safety
///
/// 1. `c` must be a valid non-null pointer obtained from [`ProfileIterator_GetCounters`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ProfileCounters_GetEof(c: *const ProfileCounters) -> c_int {
    // SAFETY: guaranteed by 1.
    let counters = unsafe { &*c };
    counters.eof as c_int
}
