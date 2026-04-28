/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`UnionTrimmed`].

use crate::utils::{MockVec, create_mock_3, drain_doc_ids};
use rqe_iterators::{IteratorType, RQEIterator, UnionTrimmed};

/// Helper: create a vec of boxed MockVec children from doc_id slices.
/// Each child's `num_estimated` equals the length of its slice.
fn make_children(slices: &[&[u64]]) -> Vec<Box<dyn RQEIterator<'static>>> {
    slices
        .iter()
        .map(|ids| MockVec::new_boxed(ids.to_vec()))
        .collect()
}

// =============================================================================
// new() requires at least 3 children
// =============================================================================

#[test]
#[should_panic(expected = "UnionTrimmed requires at least 3 children, got 0")]
fn new_panics_on_empty() {
    UnionTrimmed::<Box<dyn RQEIterator<'static>>>::new(vec![], usize::MAX, true);
}

#[test]
#[should_panic(expected = "UnionTrimmed requires at least 3 children, got 1")]
fn new_panics_on_single_child() {
    let children = make_children(&[&[1, 2, 3]]);
    UnionTrimmed::new(children, usize::MAX, true);
}

#[test]
#[should_panic(expected = "UnionTrimmed requires at least 3 children, got 2")]
fn new_panics_on_two_children() {
    let children = make_children(&[&[1, 2], &[3, 4]]);
    UnionTrimmed::new(children, usize::MAX, true);
}

// =============================================================================
// Basic read tests
// =============================================================================

/// Three children — verifies full reverse-order drain.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn three_children_reverse_order() {
    let (children, _data) = create_mock_3([1], [2], [3]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    let r = union.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3, "last child read first");
    let r = union.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 2, "middle child read second");
    let r = union.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 1, "first child read last");
    assert!(union.read().unwrap().is_none());
}

/// Four children with multiple docs each — full reverse drain.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn four_children_reverse_order() {
    let children = make_children(&[&[1, 2], &[3, 4], &[5, 6], &[7, 8]]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [7, 8, 5, 6, 3, 4, 1, 2]);
}

// =============================================================================
// skip_to panics (unsorted mode)
// =============================================================================

#[test]
#[should_panic(expected = "skip_to is not supported on UnionTrimmed")]
fn skip_to_panics() {
    let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);
    let _ = union.skip_to(2);
}

// =============================================================================
// Rewind
// =============================================================================

#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn rewind_restores_full_iteration() {
    let (children, _data) = create_mock_3([1], [2], [3]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    // Drain everything.
    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [3, 2, 1]);
    assert!(union.at_eof());

    // Rewind and read again.
    union.rewind();
    assert!(!union.at_eof());
    assert_eq!(union.last_doc_id(), 0);

    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [3, 2, 1]);
}

// =============================================================================
// num_estimated
// =============================================================================

#[test]
fn num_estimated_sums_children() {
    let (children, _data) = create_mock_3([1, 2], [3, 4, 5], [6]);
    let union = UnionTrimmed::new(children, usize::MAX, true);
    assert_eq!(union.num_estimated(), 6);
}

// =============================================================================
// Exhausted children are skipped
// =============================================================================

/// When the last child is empty, it is skipped and the next child is read.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn skips_exhausted_children() {
    let (children, _data) = create_mock_3([10, 20], [], [30]);
    // children[1] is empty, so it is skipped.
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [30, 10, 20]);
}

// =============================================================================
// Accessor helpers
// =============================================================================

#[test]
fn accessors_work() {
    let (children, _data) = create_mock_3([1], [2], [3]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    assert_eq!(union.num_children_total(), 3);
    assert_eq!(union.child_at(0).unwrap().num_estimated(), 1);
    assert_eq!(union.child_at(1).unwrap().num_estimated(), 1);
    assert_eq!(union.child_at(2).unwrap().num_estimated(), 1);
    assert_eq!(union.children_mut().count(), 3);
}

// =============================================================================
// Trimming tests
// =============================================================================

/// Ascending trim: 5 children with num_estimated [3, 3, 3, 3, 3], limit=5.
/// Scanning from child[1]: child[1]=3, child[2]=3 → total=6 > 5 → keep=3.
/// All 5 children stay alive, but only the first 3 are read.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn new_asc_basic() {
    let children = make_children(&[
        &[1, 2, 3],
        &[4, 5, 6],
        &[7, 8, 9],
        &[10, 11, 12],
        &[13, 14, 15],
    ]);
    let mut union = UnionTrimmed::new(children, 5, true);
    // All 5 children are still owned.
    assert_eq!(union.num_children_total(), 5);
    // Only first 3 are active — reads in reverse: child[2], child[1], child[0].
    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [7, 8, 9, 4, 5, 6, 1, 2, 3]);
}

/// Ascending trim: limit large enough to keep all.
#[test]
fn new_asc_keeps_all_when_limit_large() {
    let children = make_children(&[&[1], &[2], &[3], &[4]]);
    let union = UnionTrimmed::new(children, 100, true);
    assert_eq!(union.num_children_total(), 4);
}

/// Descending trim: 5 children with num_estimated [3, 3, 3, 3, 3], limit=5.
/// Scanning from child[3] backward: child[3]=3, child[2]=3 → total=6 > 5 → skip=2.
/// Active window is children[2..5], all 5 stay alive.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn new_desc_basic() {
    let children = make_children(&[
        &[1, 2, 3],
        &[4, 5, 6],
        &[7, 8, 9],
        &[10, 11, 12],
        &[13, 14, 15],
    ]);
    let mut union = UnionTrimmed::new(children, 5, false);
    assert_eq!(union.num_children_total(), 5);
    // Active window [2..5], reads in reverse: child[4], child[3], child[2].
    let docs = drain_doc_ids(&mut union);
    assert_eq!(docs, [13, 14, 15, 10, 11, 12, 7, 8, 9]);
}

/// Descending trim: limit large enough to keep all.
#[test]
fn new_desc_keeps_all_when_limit_large() {
    let children = make_children(&[&[1], &[2], &[3], &[4]]);
    let union = UnionTrimmed::new(children, 100, false);
    assert_eq!(union.num_children_total(), 4);
}

/// Ascending: verify the kept children are the correct prefix.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn new_asc_keeps_correct_prefix() {
    // Children: A=[1], B=[2,3], C=[4,5,6], D=[7,8,9,10]
    // Scanning from B: B.est=2, C.est=3 → total=5 > limit=4 → keep=3 (A,B,C).
    let children = make_children(&[&[1], &[2, 3], &[4, 5, 6], &[7, 8, 9, 10]]);
    let mut union = UnionTrimmed::new(children, 4, true);
    assert_eq!(union.num_children_total(), 4, "all children stay alive");
    // Reads in reverse within active [0..3): C then B then A.
    let r = union.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 4);
}

/// Descending: verify the kept children are the correct suffix.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn new_desc_keeps_correct_suffix() {
    // Children: A=[1], B=[2,3], C=[4,5,6], D=[7,8,9,10]
    // Scanning from C backward: C.est=3, B.est=2 → total=5 > limit=4 → skip=1.
    // Active window [1..4) = B,C,D.
    let children = make_children(&[&[1], &[2, 3], &[4, 5, 6], &[7, 8, 9, 10]]);
    let mut union = UnionTrimmed::new(children, 4, false);
    assert_eq!(union.num_children_total(), 4, "all children stay alive");
    // Reads in reverse within active [1..4): D then C then B.
    let r = union.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 7);
}

// =============================================================================
// current()
// =============================================================================

/// `current` returns `Some` after a successful read.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn current_some_after_read() {
    let children = make_children(&[&[1], &[2], &[42]]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    union.read().unwrap();
    let cur = union.current().unwrap();
    assert_eq!(cur.doc_id, 42);
}

/// `current` returns `None` after all children are exhausted.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn current_none_after_exhaustion() {
    let (children, _data) = create_mock_3([1], [2], [3]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    drain_doc_ids(&mut union);
    assert!(union.current().is_none());
}

// =============================================================================
// revalidate()
// =============================================================================

/// `revalidate` panics.
#[test]
#[should_panic(expected = "revalidate is not supported on UnionTrimmed")]
fn revalidate_panics() {
    let (children, _data) = create_mock_3([1], [2], [3]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    // SAFETY: test-only call with valid context
    let _ = unsafe { union.revalidate(mock_ctx.spec()) };
}

// =============================================================================
// type_()
// =============================================================================

/// `type_` returns `IteratorType::Union`.
#[test]
fn type_is_union() {
    let children = make_children(&[&[1], &[2], &[3]]);
    let union = UnionTrimmed::new(children, usize::MAX, true);
    assert_eq!(union.type_(), IteratorType::Union);
}

// =============================================================================
// Trimming tests
// =============================================================================

/// child_at can access trimmed-away children (no dangling).
#[test]
fn child_at_accesses_trimmed_children() {
    let children = make_children(&[&[1], &[2], &[3], &[4], &[5]]);
    let union = UnionTrimmed::new(children, 1, true);
    // Child[4] is outside the active window but still accessible.
    assert_eq!(union.child_at(4).unwrap().num_estimated(), 1);
}

// =============================================================================
// num_children_active
// =============================================================================

/// `num_children_active` tracks the remaining active window across reads.
///
/// The cursor moves to the next child only when the current child is
/// exhausted, so `num_children_active` decreases one step behind: reading
/// the last doc from a child doesn't move the cursor until the *next*
/// read discovers EOF on that child.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn num_children_active_shrinks_as_children_exhaust() {
    // 4 children with 2 docs each, asc trim with large limit → active window [0..4).
    let children = make_children(&[&[1, 2], &[3, 4], &[5, 6], &[7, 8]]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);

    assert_eq!(union.num_children_total(), 4);
    assert_eq!(union.num_children_active(), 4, "all active before reads");

    // Read first doc of child[3] — cursor still at 3.
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 4);

    // Read second doc of child[3] — child[3] not yet detected as exhausted.
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 4);

    // Next read: child[3] returns None → cursor moves to 2, reads child[2].
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 3);

    // Read second doc of child[2].
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 3);

    // child[2] exhausted → cursor moves to 1.
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 2);

    // Read second doc of child[1].
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 2);

    // child[1] exhausted → cursor moves to 0.
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 1);

    // Read second doc of child[0].
    union.read().unwrap().unwrap();
    assert_eq!(union.num_children_active(), 1);

    // child[0] exhausted → EOF.
    assert!(union.read().unwrap().is_none());
    assert_eq!(union.num_children_active(), 0, "none active at EOF");

    // Rewind restores full active window.
    union.rewind();
    assert_eq!(union.num_children_active(), 4, "all active after rewind");
}

/// When trimming reduces the active window, `num_children_active` reflects
/// only the kept children, not the total.
#[test]
fn num_children_active_respects_trim() {
    // 5 children each with est=1. Asc trim with limit=1:
    // scan from child[1]: child[1].est=1 → total=1 ≤ 1, child[2].est=1 → total=2 > 1 → keep=3.
    let children = make_children(&[&[1], &[2], &[3], &[4], &[5]]);
    let union = UnionTrimmed::new(children, 1, true);

    assert_eq!(union.num_children_total(), 5);
    assert_eq!(union.num_children_active(), 3);
}

// =============================================================================
// intersection_sort_weight
// =============================================================================

/// When `prioritize_union_children` is false, weight is always 1.0.
#[test]
fn intersection_sort_weight_without_priority() {
    let children = make_children(&[&[1], &[2], &[3], &[4]]);
    let union = UnionTrimmed::new(children, usize::MAX, true);
    assert_eq!(union.intersection_sort_weight(false), 1.0);
}

/// When `prioritize_union_children` is true, weight equals the active window
/// size (not the total number of children).
#[test]
fn intersection_sort_weight_with_priority() {
    // 5 children, trim keeps 3 → weight should be 3.0.
    let children = make_children(&[&[1], &[2], &[3], &[4], &[5]]);
    let union = UnionTrimmed::new(children, 1, true);
    assert_eq!(union.num_children_active(), 3);
    assert_eq!(union.intersection_sort_weight(true), 3.0);
}

/// Weight shrinks as children exhaust during reads.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn intersection_sort_weight_decreases_after_reads() {
    let children = make_children(&[&[1, 2], &[3, 4], &[5, 6]]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);
    assert_eq!(union.intersection_sort_weight(true), 3.0);

    // Drain child[2] (2 docs) then child[1] starts.
    union.read().unwrap(); // child[2] doc 5
    union.read().unwrap(); // child[2] doc 6
    union.read().unwrap(); // child[2] EOF → cursor moves to 1, reads child[1] doc 3
    assert_eq!(union.intersection_sort_weight(true), 2.0);
}

/// At EOF, weight is 1.0.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn intersection_sort_weight_at_eof() {
    let children = make_children(&[&[1], &[2], &[3]]);
    let mut union = UnionTrimmed::new(children, usize::MAX, true);
    drain_doc_ids(&mut union);
    assert_eq!(union.intersection_sort_weight(true), 1.0);
}

// =============================================================================
// into_trimmed
// =============================================================================

/// `into_trimmed` re-trims with new parameters, preserving all children
/// including those previously trimmed away.
#[test]
#[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
fn into_trimmed_reuses_all_children() {
    // 5 children, each with est=3. Asc trim with limit=5:
    // scan from child[1]: child[1]=3, child[2]=3 → total=6 > 5 → keep=3.
    // Active window [0..3), so children[3] and [4] are trimmed away.
    let children = make_children(&[
        &[1, 2, 3],
        &[4, 5, 6],
        &[7, 8, 9],
        &[10, 11, 12],
        &[13, 14, 15],
    ]);
    let union = UnionTrimmed::new(children, 5, true);
    assert_eq!(union.num_children_total(), 5);

    // Re-trim with a large limit so all 5 children become active again,
    // including children[3] and [4] that were previously trimmed away.
    let mut union2 = union.into_trimmed(usize::MAX, true).unwrap();
    assert_eq!(union2.num_children_total(), 5, "all children carried over");
    let docs = drain_doc_ids(&mut union2);
    assert_eq!(
        docs,
        [13, 14, 15, 10, 11, 12, 7, 8, 9, 4, 5, 6, 1, 2, 3],
        "previously trimmed children are now active"
    );
}
