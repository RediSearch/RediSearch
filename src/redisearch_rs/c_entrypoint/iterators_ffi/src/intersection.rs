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
    c2rust::CRQEIterator, interop::RQEIteratorWrapper, intersection::Intersection,
};

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
/// Delegates reduction to the C `IntersectionIteratorReducer` before constructing the iterator.
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
    let mut num_filtered = num;
    // SAFETY: `its` is a valid Redis-allocated array of `num` QueryIterator* per caller's
    // contract. The C reducer frees `its` on short-circuit (non-NULL return).
    let reduced = unsafe { ffi::IntersectionIteratorReducer(its, &mut num_filtered) };
    if !reduced.is_null() {
        return reduced; // `its` already freed by the C reducer
    }

    // NULL returned from the reducer: num_filtered >= 2, its[0..num_filtered] are valid non-wildcard iterators.
    // SAFETY: `its` is still valid and `num_filtered` elements are initialised non-null pointers.
    let slice = unsafe { std::slice::from_raw_parts(its, num_filtered) };
    let children: Vec<CRQEIterator> = slice
        .iter()
        .map(|&ptr| {
            // SAFETY: each pointer is valid, non-null, and uniquely owned.
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
    let intersection = Intersection::new_with_slop_order(
        children,
        weight,
        prioritize_union_children,
        max_slop,
        in_order,
    );
    let wrapper = RQEIteratorWrapper::boxed_new(intersection);

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
/// structural modifications are made (e.g. via [`AddIntersectionIteratorChild`] or
/// [`ForEachIntersectionChildMut`]).
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

/// Apply `callback` to each child iterator slot, passing a mutable `QueryIterator**`.
///
/// This is designed for use with `Profile_AddIters`, which replaces each child with a
/// profile-wrapping iterator in place. The callback receives a pointer to the raw pointer
/// stored inside each [`CRQEIterator`] child, allowing it to update (replace) that pointer.
///
/// This is safe because [`CRQEIterator`] is `#[repr(transparent)]` over
/// `NonNull<QueryIterator>`, which has the same memory layout as `*mut QueryIterator`.
/// The callback's in-place mutation of the slot directly updates the `CRQEIterator`'s
/// internal pointer, so the intersection will subsequently own (and free) the new iterator.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewIntersectionIterator`].
/// 2. `callback` must be a valid function pointer.
/// 3. The callback must replace `*slot` with a valid non-null `QueryIterator*` that takes
///    ownership of the original iterator (i.e. `NewProfileIterator` semantics).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ForEachIntersectionChildMut(
    header: *mut QueryIterator,
    callback: unsafe extern "C" fn(*mut *mut QueryIterator),
) {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: safe thanks to 1
        unsafe { (*header).type_ },
        IteratorType::Intersect,
        "Expected an INTERSECT_ITERATOR"
    );
    // SAFETY: safe thanks to 1
    let wrapper = unsafe {
        RQEIteratorWrapper::<Intersection<CRQEIterator>>::mut_ref_from_header_ptr(header)
    };

    for child in wrapper.inner.children_mut() {
        // SAFETY:
        // - `CRQEIterator` is `#[repr(transparent)]` over `NonNull<QueryIterator>`, which has
        //   the same layout as `*mut QueryIterator`.
        // - Casting `*mut CRQEIterator` to `*mut *mut QueryIterator` gives the callback direct
        //   write access to the raw pointer stored inside the `CRQEIterator`.
        // - After the callback (e.g. `Profile_AddIters`) writes a new pointer into the slot,
        //   the `CRQEIterator` owns it and will free it correctly on drop. Safe per contract 3.
        let slot = child as *mut CRQEIterator as *mut *mut QueryIterator;
        // SAFETY: safe thanks to 2 and 3.
        unsafe { callback(slot) };
    }
}
