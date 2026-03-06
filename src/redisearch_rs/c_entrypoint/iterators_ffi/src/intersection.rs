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
    IteratorType_EMPTY_ITERATOR, IteratorType_INTERSECT_ITERATOR,
    IteratorType_INV_IDX_WILDCARD_ITERATOR, IteratorType_UNION_ITERATOR,
    IteratorType_WILDCARD_ITERATOR, QueryIterator, UnionIterator,
};
use rqe_iterators::intersection::Intersection;
use rqe_iterators_interop::RQEIteratorWrapper;

use crate::empty::NewEmptyIterator;
use rqe_iterators::c2rust::CRQEIterator;

/// Returns `true` if the iterator is a wildcard iterator of any kind.
///
/// Mirrors the C `IsWildcardIterator()` function from `wildcard_iterator.c`.
const fn is_wildcard_iterator(it: &QueryIterator) -> bool {
    it.type_ == IteratorType_WILDCARD_ITERATOR || it.type_ == IteratorType_INV_IDX_WILDCARD_ITERATOR
}

/// Free the C-allocated `its` array using the Redis allocator.
///
/// # Safety
///
/// `its` must have been allocated with `rm_malloc` / `RedisModule_Alloc`.
unsafe fn free_its_array(its: *mut *mut QueryIterator) {
    // SAFETY: Redis allocator must be initialized before this is called.
    let free_fn = unsafe { ffi::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: `its` was allocated via the Redis allocator; the caller guarantees this.
    unsafe { free_fn(its.cast::<std::ffi::c_void>()) };
}

/// Free a `QueryIterator` by calling its `Free` callback.
///
/// # Safety
///
/// `it` must be a valid, non-null pointer to a `QueryIterator` whose `Free`
/// callback is set and safe to call.
unsafe fn free_iterator(it: NonNull<QueryIterator>) {
    // SAFETY: guaranteed by the caller.
    let free_fn = unsafe { it.as_ref().Free.expect("Free callback is NULL") };
    // SAFETY: guaranteed by the caller.
    unsafe { free_fn(it.as_ptr()) };
}

/// Classify the raw child pointers into wildcards and non-wildcards.
///
/// Returns `(kept, last_wildcard)` where:
/// - `kept`: non-wildcard entries in declaration order. A `None` slot means the original
///   pointer was `NULL` (treated as an empty iterator by the reducer).
/// - `last_wildcard`: the preserved last wildcard iterator. `Some` only when *all* children
///   were wildcards; `None` otherwise.
///
/// All "discarded" wildcard iterators are freed inside this function.
///
/// # Safety
///
/// 1. Every non-null pointer in `slice` must be a valid, fully-initialised `QueryIterator`.
/// 2. Each such pointer must be uniquely owned by the caller (no aliasing), so that this
///    function may call `free_iterator` on the ones it discards.
unsafe fn classify_children(
    slice: &[*mut QueryIterator],
) -> (
    Vec<Option<NonNull<QueryIterator>>>,
    Option<NonNull<QueryIterator>>,
) {
    let mut kept: Vec<Option<NonNull<QueryIterator>>> = Vec::with_capacity(slice.len());
    // The last wildcard seen: kept alive only when all children are wildcards.
    let mut last_wildcard: Option<NonNull<QueryIterator>> = None;
    let mut all_wildcards = true;

    for &raw_ptr in slice {
        if let Some(it) = NonNull::new(raw_ptr) {
            // SAFETY: non-null pointers are valid per safety contract.
            if unsafe { is_wildcard_iterator(it.as_ref()) } {
                // Replace last_wildcard; free the evicted one.
                if let Some(prev) = last_wildcard.replace(it) {
                    // SAFETY: conditions (1) and (2) — `prev` is valid and uniquely owned.
                    unsafe { free_iterator(prev) };
                }
            } else {
                // Non-wildcard: any saved wildcard is now not needed.
                if all_wildcards {
                    if let Some(wc) = last_wildcard.take() {
                        // SAFETY: conditions (1) and (2) — `wc` is valid and uniquely owned.
                        unsafe { free_iterator(wc) };
                    }
                    all_wildcards = false;
                }
                kept.push(Some(it));
            }
        } else {
            // NULL ≡ empty iterator: still a non-wildcard entry.
            if all_wildcards {
                if let Some(wc) = last_wildcard.take() {
                    // SAFETY: conditions (1) and (2) — `wc` is valid and uniquely owned.
                    unsafe { free_iterator(wc) };
                }
                all_wildcards = false;
            }
            kept.push(None); // NULL sentinel
        }
    }

    if !all_wildcards {
        // There are real children: the saved last_wildcard is not needed.
        if let Some(wc) = last_wildcard.take() {
            // SAFETY: conditions (1) and (2) — `wc` is valid and uniquely owned.
            unsafe { free_iterator(wc) };
        }
        (kept, None)
    } else {
        // All children were wildcards: pass the last one back to the caller.
        (kept, last_wildcard)
    }
}

/// Compute the sort weight for one child iterator, mirroring the C `iteratorFactor()` helper.
///
/// The sort key applied in [`NewIntersectionIterator`] is `NumEstimated(it) * iterator_factor(it)`.
/// A *lower* key means the child acts as the pivot (is iterated first), which minimises
/// the total number of `SkipTo` calls made during query execution.
///
/// - `INTERSECT_ITERATOR` → factor `1/n` (de-prioritise deep nested intersections)
/// - `UNION_ITERATOR` when `prioritizeIntersectUnionChildren` is set → factor `n`
///   (de-prioritise wide unions so that cheaper iterators lead)
/// - everything else → factor `1.0`
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a fully-initialised `QueryIterator`.
unsafe fn iterator_factor(it: NonNull<QueryIterator>) -> f64 {
    // SAFETY: guaranteed by the caller.
    let qi = unsafe { it.as_ref() };
    if qi.type_ == IteratorType_INTERSECT_ITERATOR {
        // SAFETY: `GetIntersectionIteratorNumChildren` is safe for any INTERSECT_ITERATOR.
        let n = unsafe { GetIntersectionIteratorNumChildren(it.as_ptr()) };
        if n == 0 { 1.0 } else { 1.0 / n as f64 }
    } else if qi.type_ == IteratorType_UNION_ITERATOR {
        // SAFETY: A UNION_ITERATOR typed pointer always points to a `UnionIterator` whose
        // `base` field is a `QueryIterator`.  The cast is therefore valid.
        let ui = unsafe { &*it.as_ptr().cast::<UnionIterator>() };
        // SAFETY: `RSGlobalConfig` is a valid extern C global, initialized before any query runs.
        if unsafe { ffi::RSGlobalConfig.prioritizeIntersectUnionChildren } {
            ui.num as f64
        } else {
            1.0
        }
    } else {
        1.0
    }
}

/// Compute the sort key used to order children inside the intersection.
///
/// Mirrors `NumEstimated(it) * iteratorFactor(it)` from the original C implementation.
///
/// # Safety
///
/// 1. `it` must be a valid, non-null pointer to a fully-initialised `QueryIterator`.
unsafe fn sort_key(it: NonNull<QueryIterator>) -> f64 {
    // SAFETY: guaranteed by the caller.
    let qi = unsafe { it.as_ref() };
    // SAFETY: NumEstimated is non-null for any well-formed QueryIterator.
    let num_estimated_fn = unsafe { qi.NumEstimated.unwrap_unchecked() };
    // SAFETY: `it` is a valid, non-null `QueryIterator` per the caller's contract.
    let num_est = unsafe { num_estimated_fn(it.as_ptr()) } as f64;
    // SAFETY: guaranteed by the caller.
    num_est * unsafe { iterator_factor(it) }
}

/// Outcome of [`intersection_iterator_reducer`].
enum ReducerOutcome {
    /// Short-circuit: return this pointer to the caller immediately.
    ///
    /// The reducer has already freed `its`.
    Return(*mut QueryIterator),
    /// Proceed to build a real intersection from these (≥ 2) children.
    ///
    /// The caller is responsible for freeing `its` after constructing the iterator.
    Proceed(Vec<NonNull<QueryIterator>>),
}

/// Reduce the raw child list before constructing an intersection iterator.
///
/// Mirrors the C `IntersectionIteratorReducer` function:
///
/// 0. If `num == 0`, return an empty iterator immediately.
/// 1. Remove wildcard iterators (they cannot further constrain an intersection).
///    If *all* children were wildcards, return the last one.
/// 2. If any child is `NULL` or an `EMPTY_ITERATOR`, free the rest and return an empty iterator.
/// 3. If exactly one non-wildcard child remains, return it directly.
/// 4. Otherwise signal the caller to proceed with building a real intersection.
///
/// When [`ReducerOutcome::Return`] is produced `its` has already been freed.
/// When [`ReducerOutcome::Proceed`] is produced the caller must free `its`.
///
/// # Safety
///
/// - When `num > 0`, `its` must be a valid pointer to an array of `num` `QueryIterator*` values.
/// - Every non-null pointer in `its` must be a valid `QueryIterator` whose callbacks are set.
/// - `its` (when non-null) must have been allocated with the Redis allocator (`rm_malloc`).
unsafe fn intersection_iterator_reducer(
    its: *mut *mut QueryIterator,
    num: usize,
) -> ReducerOutcome {
    // Rule 0 – no children at all.
    if num == 0 {
        if !its.is_null() {
            // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
            unsafe { free_its_array(its) };
        }
        return ReducerOutcome::Return(NewEmptyIterator());
    }

    // SAFETY: `its` is valid for `num` elements and `num > 0`.
    let slice: &[*mut QueryIterator] = unsafe { std::slice::from_raw_parts(its, num) };

    // Classify children into wildcards (freed internally) and non-wildcards.
    // SAFETY: non-null entries in `slice` are valid per the caller's contract.
    let (kept, last_wildcard) = unsafe { classify_children(slice) };

    // Rule 1 – all children were wildcards: return the preserved last wildcard.
    if let Some(wc) = last_wildcard {
        // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
        unsafe { free_its_array(its) };
        return ReducerOutcome::Return(wc.as_ptr());
    }

    // `classify_children` returns `last_wildcard=None` only when `kept` is non-empty
    // (non-wildcard children were found). Guard defensively in case the slice was empty.
    if kept.is_empty() {
        // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
        unsafe { free_its_array(its) };
        return ReducerOutcome::Return(NewEmptyIterator());
    }

    // Rule 2 – scan for a NULL (≡ empty) or EMPTY_ITERATOR child.
    let mut empty_idx: Option<usize> = None;
    for (i, opt_it) in kept.iter().enumerate() {
        match opt_it {
            None => {
                empty_idx = Some(i);
                break;
            }
            Some(it) => {
                // SAFETY: non-null entries are valid per the caller's contract.
                if unsafe { it.as_ref() }.type_ == IteratorType_EMPTY_ITERATOR {
                    empty_idx = Some(i);
                    break;
                }
            }
        }
    }

    if let Some(idx) = empty_idx {
        // Free all non-empty children; preserve the empty iterator to return to the caller.
        let mut empty_it: Option<NonNull<QueryIterator>> = None;
        for (i, opt_it) in kept.into_iter().enumerate() {
            if i == idx {
                empty_it = opt_it; // `None` = NULL slot, `Some` = EMPTY_ITERATOR.
            } else if let Some(it) = opt_it {
                // SAFETY: pointers in `kept` are valid per `classify_children`'s contract.
                unsafe { free_iterator(it) };
            }
        }
        // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
        unsafe { free_its_array(its) };
        return ReducerOutcome::Return(match empty_it {
            Some(it) => it.as_ptr(),
            None => NewEmptyIterator(),
        });
    }

    // All entries are `Some` at this point (no NULLs, no empty iterators).
    let kept_valid: Vec<NonNull<QueryIterator>> =
        kept.into_iter().map(|opt| opt.unwrap()).collect();

    // Rule 3 – single child: no point wrapping it in an intersection.
    if kept_valid.len() == 1 {
        // SAFETY: `its` is a valid Redis-allocated pointer per the caller's contract.
        unsafe { free_its_array(its) };
        return ReducerOutcome::Return(kept_valid[0].as_ptr());
    }

    // Rule 4 – two or more real children: let the caller build the intersection.
    ReducerOutcome::Proceed(kept_valid)
}

/// Create a new intersection iterator.
///
/// Takes ownership of both the `its` array and all child iterators it contains.
/// Delegates reduction to [`intersection_iterator_reducer`] (mirroring the C
/// `IntersectionIteratorReducer` helper) before constructing the iterator.
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
    // SAFETY: non-null entries in `its` are valid per caller's contract.
    let mut kept_valid = match unsafe { intersection_iterator_reducer(its, num) } {
        ReducerOutcome::Return(ptr) => return ptr,
        ReducerOutcome::Proceed(v) => v,
    };

    // Sort children by the `iteratorFactor` heuristic before wrapping them, mirroring the
    // original C `NewIntersectionIterator` behaviour.  Sorting is skipped when `in_order` is
    // set because the child order is then semantically significant for proximity checks.
    if !in_order {
        kept_valid.sort_by(|&a, &b| {
            // SAFETY: `sort_key` condition (1) — all pointers in `kept_valid` are valid
            // non-null `QueryIterator` pointers, as guaranteed by the caller of this function.
            let ka = unsafe { sort_key(a) };
            // SAFETY: Same as ka
            let kb = unsafe { sort_key(b) };
            ka.total_cmp(&kb)
        });
    }

    // Build the Rust intersection from the sorted non-wildcard children.
    // Use `new_presorted` so that a second sort is not applied inside the constructor.
    let children: Vec<CRQEIterator> = kept_valid
        .into_iter()
        .map(|ptr| {
            // SAFETY: each pointer is valid, non-null, and non-aliased.
            unsafe { CRQEIterator::new(ptr) }
        })
        .collect();

    let max_slop = if max_slop < 0 { None } else { Some(max_slop) };
    let intersection = Intersection::new_presorted(children, max_slop, in_order).weight(weight);
    let wrapper = RQEIteratorWrapper::boxed_new(IteratorType_INTERSECT_ITERATOR, intersection);

    // SAFETY: `its` is a valid Redis-allocated pointer per the function's safety contract.
    unsafe { free_its_array(its) };
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
        IteratorType_INTERSECT_ITERATOR,
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
        IteratorType_INTERSECT_ITERATOR,
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
/// Mirrors the C `AddIntersectIterator` function from `query_optimizer.c`.
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
        IteratorType_INTERSECT_ITERATOR,
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
        IteratorType_INTERSECT_ITERATOR,
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
