/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{IteratorType, QueryIterator};
use rqe_iterators::{
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator},
};

use crate::empty::NewEmptyIterator;

/// Free the C-allocated `its` array using the Redis allocator.
///
/// # Safety
///
/// `its` must have been allocated with `rm_malloc` / `RedisModule_Alloc`.
unsafe fn free_iterators_array(its: *mut *mut QueryIterator) {
    // SAFETY: Redis allocator must be initialized before this is called.
    let free_fn = unsafe { ffi::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: `its` was allocated via the Redis allocator; the caller guarantees this.
    unsafe { free_fn(its.cast::<std::ffi::c_void>()) };
}

/// Create a new intersection iterator.
///
/// Takes ownership of both the `its` array and all child iterators it contains.
/// Applies reduction rules before
/// constructing the iterator:
///
/// 0. No children → empty iterator.
/// 1. Strip wildcard children. All wildcards → return the last one.
/// 2. Any empty child → free all, return empty iterator.
/// 3. Exactly one real child → return it directly.
/// 4. Two or more real children → build a full intersection.
///
/// # Safety
///
/// 1. `its` must be a valid non-null pointer to an array of `num` `QueryIterator*` values,
///    allocated with the Redis allocator (`rm_malloc`). Ownership is transferred to this function.
/// 2. Every non-null pointer in `its` must be a valid `QueryIterator` whose callbacks are set.
/// 3. Null entries in `its` are treated as empty iterators.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewIntersectionIterator(
    its: *mut *mut QueryIterator,
    num: usize,
    max_slop: i32,
    in_order: bool,
    weight: f64,
) -> *mut QueryIterator {
    // Rule 0 – no children at all.
    if num == 0 {
        if !its.is_null() {
            // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
            unsafe { free_iterators_array(its) };
        }
        return NewEmptyIterator();
    }

    // SAFETY: `its` is valid for `num` elements and `num > 0`.
    let slice: &[*mut QueryIterator] = unsafe { std::slice::from_raw_parts(its, num) };

    // Wrap each raw pointer into a `CRQEIterator`. From this point, Rust Drop manages lifetimes.
    // NULL pointers are replaced by an empty iterator so they flow through is_empty() naturally.
    let children: Vec<CRQEIterator> = slice
        .iter()
        .map(|&ptr| {
            let ptr = if ptr.is_null() {
                // NULL ≡ empty iterator
                NewEmptyIterator()
            } else {
                ptr
            };
            // SAFETY: ptr is non-null (either original or NewEmptyIterator()).
            // Each pointer is valid and uniquely owned per caller's contract.
            let ptr = unsafe { NonNull::new_unchecked(ptr) };
            // SAFETY: each pointer is valid, non-null, and uniquely owned.
            unsafe { CRQEIterator::new(ptr) }
        })
        .collect();
    let max_slop = if max_slop < 0 {
        None
    } else {
        Some(max_slop as u32)
    };
    // SAFETY: `ffi::RSGlobalConfig` is the global config instance, read-only here.
    let prioritize_union_children = unsafe { ffi::RSGlobalConfig.prioritizeIntersectUnionChildren };
    let wrapper = match new_intersection_iterator(children) {
        NewIntersectionIterator::Empty => NewEmptyIterator(),
        NewIntersectionIterator::Single(child) => child.into_raw().as_ptr(),
        NewIntersectionIterator::Proceed(cs) => {
            let intersection = Intersection::new_with_slop_order(
                cs,
                weight,
                prioritize_union_children,
                max_slop,
                in_order,
            );
            RQEIteratorWrapper::boxed_new_compound(intersection)
        }
    };

    // Free the `its` array (iterators are now owned by the intersection).
    // SAFETY: `its` is a valid Redis-allocated pointer per the function's safety contract.
    unsafe { free_iterators_array(its) };
    wrapper
}

/// Returns the number of child iterators held by the intersection iterator.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewIntersectionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetIntersectionIteratorNumChildren(header: *const QueryIterator) -> usize {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: safe thanks to 1
        unsafe { (*header).type_ },
        IteratorType::Intersect,
        "Expected an INTERSECT_ITERATOR"
    );
    // SAFETY: safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<Intersection<CRQEIterator>>::ref_from_header_ptr(header) };
    wrapper.inner.num_children()
}

/// Returns a non-owning raw pointer to the child at `idx`.
///
/// The returned pointer is valid as long as the intersection iterator is alive and no
/// structural modifications are made (e.g. via [`AddIntersectionIteratorChild`]).
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewIntersectionIterator`].
/// 2. `idx` must be less than [`GetIntersectionIteratorNumChildren`]`(header)`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetIntersectionIteratorChild(
    header: *const QueryIterator,
    idx: usize,
) -> *const QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: safe thanks to 1
        unsafe { (*header).type_ },
        IteratorType::Intersect,
        "Expected an INTERSECT_ITERATOR"
    );
    // SAFETY: safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<Intersection<CRQEIterator>>::ref_from_header_ptr(header) };
    // SAFETY: safe thanks to 2
    wrapper.inner.child_at(idx).as_ref() as *const QueryIterator
}

/// Append a new child iterator to the intersection.
///
/// Transfers ownership of `child` to the intersection. Updates the estimated result count
/// if the new child has a lower estimate than the current minimum.
///
/// # Note
///
/// Unlike the constructor, this method does **not** re-sort the child list after insertion.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewIntersectionIterator`].
/// 2. `child` must be a valid non-null pointer to a `QueryIterator`, not aliased.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn AddIntersectionIteratorChild(
    header: *mut QueryIterator,
    child: *mut QueryIterator,
) {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: safe thanks to 1
        unsafe { (*header).type_ },
        IteratorType::Intersect,
        "Expected an INTERSECT_ITERATOR"
    );
    debug_assert!(!child.is_null());
    // SAFETY: safe thanks to 1
    let wrapper = unsafe {
        RQEIteratorWrapper::<Intersection<CRQEIterator>>::mut_ref_from_header_ptr(header)
    };
    // SAFETY: safe thanks to 2; both `new_unchecked` and `CRQEIterator::new` are
    // justified by the same contract point.
    #[expect(clippy::multiple_unsafe_ops_per_block)]
    let child = unsafe { CRQEIterator::new(NonNull::new_unchecked(child)) };
    wrapper.inner.push_child(child);
}
