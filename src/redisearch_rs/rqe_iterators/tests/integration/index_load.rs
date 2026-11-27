/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use index_result::RSIndexResult;
use query_types::QueryNodeType;
use rlookup::{RLookupKey, RLookupKeyHandle};
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome, TypeErasedRQEIterator,
    c2rust::CRQEIterator,
    index_load,
    index_load::IndexLoad,
    score_bound::{BlockBound, MinCompetitiveScore},
    union_opaque::build_union,
};
use rqe_iterators_test_utils::{
    ContractChecker, MockContext, ResumeOutcomeExt, revalidate_via_resume,
};
use rstest::rstest;

use crate::utils::{
    Mock, MockIteratorError, MockRevalidateResult,
    blocked::{Block, Blocked},
};

/// A stand-in for the key pipeline construction resolves: the decorator only
/// stores a reference to it, never reads it.
struct FakeKey(Box<ffi::RLookupKey>);

impl FakeKey {
    fn new() -> Self {
        // SAFETY: `RLookupKey` is a plain C struct, for which all-zero is a
        // valid value (null pointers, zero indices and flags).
        Self(Box::new(unsafe { std::mem::zeroed() }))
    }

    fn as_ptr<'index>(&mut self) -> *mut RLookupKey<'index> {
        (&raw mut *self.0).cast()
    }
}

/// Bind `key` into `it`'s key slot, as pipeline construction does.
fn bind<'index, I: RQEIterator<'index>>(it: &mut IndexLoad<'index, I>, key: &mut FakeKey) {
    // SAFETY: the slot is the decorator's live heap cell, and nothing reads it
    // while it is written.
    unsafe { *it.key_slot() = key.as_ptr() };
}

/// The single `(key, value)` metric `result` carries.
fn sole_metric(result: &RSIndexResult<'_>) -> (*const ffi::RLookupKey, f64) {
    let metrics = result.metrics_ref();
    assert_eq!(metrics.len(), 1, "expected exactly one metric");
    let entry = metrics.get(0).unwrap();
    let key = entry.key().expect("the metric must carry the bound key");
    (key as *const _, entry.value())
}

fn fake_key_ptr(key: &FakeKey) -> *const ffi::RLookupKey {
    &*key.0 as *const _
}

#[test]
fn reports_the_childs_type() {
    let it = IndexLoad::new(Mock::new_with_values([1], [1.0]));
    assert_eq!(it.type_(), IteratorType::Mock);
}

#[test]
fn read_attaches_each_hits_value() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([2, 5, 9], [20.0, 50.0, 90.0]));
    bind(&mut it, &mut key);
    let mut it = ContractChecker::new(it);

    for (doc_id, value) in [(2, 20.0), (5, 50.0), (9, 90.0)] {
        let result = it.read().unwrap().unwrap();
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(sole_metric(result), (fake_key_ptr(&key), value));
    }
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn skip_to_attaches_found_and_not_found_hits() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([2, 5, 9], [20.0, 50.0, 90.0]));
    bind(&mut it, &mut key);
    let mut it = ContractChecker::new(it);

    let Some(SkipToOutcome::Found(result)) = it.skip_to(5).unwrap() else {
        panic!("doc 5 is in the child");
    };
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 50.0));

    // A parent keeps a `NotFound` hit as its next candidate.
    let Some(SkipToOutcome::NotFound(result)) = it.skip_to(7).unwrap() else {
        panic!("doc 9 follows doc 7");
    };
    assert_eq!(result.doc_id, 9);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 90.0));

    assert!(it.skip_to(10).unwrap().is_none());
}

#[test]
fn unbound_key_pushes_nothing() {
    let mut it = ContractChecker::new(IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0])));

    let result = it.read().unwrap().unwrap();
    assert!(result.metrics_ref().is_empty());
    let Some(SkipToOutcome::Found(result)) = it.skip_to(2).unwrap() else {
        panic!("doc 2 is in the child");
    };
    assert!(result.metrics_ref().is_empty());
}

#[test]
fn non_numeric_hit_pushes_nothing() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new([1]));
    bind(&mut it, &mut key);

    let result = it.read().unwrap().unwrap();
    assert!(result.metrics_ref().is_empty());
}

#[test]
fn metrics_are_replaced_not_accumulated() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0]));
    bind(&mut it, &mut key);

    // A metric left on the shared result by an earlier hit must not leak into
    // the next one.
    let result = it.read().unwrap().unwrap();
    result.metrics_mut().push_without_key(-1.0);
    let result = it.read().unwrap().unwrap();
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 2.0));
}

#[test]
fn value_comes_from_the_aggregate_child_that_matched() {
    let mut key = FakeKey::new();
    // The shape `build_numeric_filter_iterator` returns: a quick-exit union of
    // one iterator per range-tree leaf.
    let leaves = vec![
        CRQEIterator::from_rust_leaf(Mock::new_with_values([1, 3], [10.0, 30.0])),
        CRQEIterator::from_rust_leaf(Mock::new_with_values([2], [20.0])),
    ];
    let union = build_union(leaves, true, 20, QueryNodeType::Numeric, 1.0);
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    let mut it = IndexLoad::new(unsafe { CRQEIterator::new(union) });
    bind(&mut it, &mut key);

    for (doc_id, value) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        let result = it.read().unwrap().unwrap();
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(sole_metric(result), (fake_key_ptr(&key), value));
    }
}

#[test]
fn forwards_to_the_child() {
    let mut it = ContractChecker::new(IndexLoad::new(Mock::new_with_values([3, 4], [3.0, 4.0])));
    assert_eq!(it.num_estimated(), 2);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 3);
    assert_eq!(it.last_doc_id(), 3);
    assert_eq!(it.current().unwrap().doc_id, 3);

    it.rewind();
    assert_eq!(it.last_doc_id(), 0);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 3);
}

#[test]
fn revalidate_moved_reattaches() {
    let mock_ctx = MockContext::new(0, 0);
    let mut key = FakeKey::new();
    let child = Mock::new_with_values([1, 2, 3], [1.0, 2.0, 3.0]);
    let mut data = child.data();
    let mut it = IndexLoad::new(child);
    bind(&mut it, &mut key);

    assert!(it.read().unwrap().is_some());
    data.set_revalidate_result(MockRevalidateResult::Move);
    let RQEValidateStatus::Moved {
        current: Some(result),
    } = it.revalidate(&mock_ctx.spec_read()).unwrap()
    else {
        panic!("the child moved onto doc 2");
    };
    assert_eq!(result.doc_id, 2);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 2.0));
}

#[test]
fn resume_moved_reattaches() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut key = FakeKey::new();
    let child = Mock::new_with_values([1, 2, 3], [1.0, 2.0, 3.0]);
    let mut data = child.data();
    let mut it = Box::new(IndexLoad::new(child));
    bind(&mut it, &mut key);

    // A parent takes the metrics of the hit it consumed.
    let result = it.read().unwrap().unwrap();
    drop(std::mem::take(result.metrics_mut()));

    data.set_revalidate_result(MockRevalidateResult::Move);
    let ResumeOutcome::Moved(mut it) = it.suspend().resume(&guard).unwrap() else {
        panic!("the child moved onto doc 2");
    };
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 2);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 2.0));
}

#[test]
fn resume_ok_keeps_position_and_allocation() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0])));
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let address = &*it as *const _ as usize;

    let suspended = it.suspend();
    assert_eq!(&*suspended as *const _ as usize, address);
    let ResumeOutcome::Ok(mut it) = suspended.resume(&guard).unwrap() else {
        panic!("the child did not move");
    };
    assert_eq!(&*it as *const _ as usize, address);
    assert_eq!(it.current().unwrap().doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
}

/// A type-erased child swaps its vtable on each transition, which only a
/// transition through its own `suspend`/`resume` gets right.
#[test]
fn type_erased_child_round_trips_in_place() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut key = FakeKey::new();
    let child = Mock::new_with_values([1, 2, 3], [1.0, 2.0, 3.0]);
    let mut data = child.data();
    let mut it = Box::new(IndexLoad::new(TypeErasedRQEIterator::new(Box::new(child))));
    bind(&mut it, &mut key);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let address = &*it as *const _ as usize;

    let ResumeOutcome::Ok(mut it) = it.suspend().resume(&guard).unwrap() else {
        panic!("the child did not move");
    };
    assert_eq!(&*it as *const _ as usize, address);
    assert_eq!(it.current().unwrap().doc_id, 1);

    data.set_revalidate_result(MockRevalidateResult::Move);
    let ResumeOutcome::Moved(mut it) = it.suspend().resume(&guard).unwrap() else {
        panic!("the child moved onto doc 2");
    };
    assert_eq!(&*it as *const _ as usize, address);
    assert_eq!(
        sole_metric(it.current().unwrap()),
        (fake_key_ptr(&key), 2.0)
    );
}

#[test]
fn resume_aborted_frees_the_decorator() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let child = Mock::new_with_values([1], [1.0]);
    child
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };
    let mut it = Box::new(IndexLoad::new(child));
    // SAFETY: (1) `handle` outlives `it`, no handle was attached before, and
    // nothing else borrows `handle` while `it` is dropped.
    unsafe { it.set_handle(Some(NonNull::from(&mut handle))) };

    assert!(matches!(
        it.suspend().resume(&guard).unwrap(),
        ResumeOutcome::Aborted
    ));
    assert!(
        !handle.is_valid,
        "freeing the decorator invalidates its handle"
    );
}

#[test]
fn resume_error_frees_the_decorator() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let child = Mock::new_with_values([1], [1.0]);
    child
        .data()
        .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));
    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };
    let mut it = Box::new(IndexLoad::new(child));
    // SAFETY: (1) as in `resume_aborted_frees_the_decorator`.
    unsafe { it.set_handle(Some(NonNull::from(&mut handle))) };

    assert!(matches!(
        it.suspend().resume(&guard),
        Err(RQEIteratorError::Stopped)
    ));
    assert!(
        !handle.is_valid,
        "freeing the decorator invalidates its handle"
    );
}

#[test]
fn forwards_score_bounds_to_the_child() {
    let it = IndexLoad::new(Blocked::new(vec![
        Block::tight(&[(1, 3.0), (2, 5.0)]),
        Block::loose(&[(7, 1.0)], 4.0),
    ]));
    let mut it = ContractChecker::new(it);
    assert!(it.reports_score_bounds());

    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(it.current_max_score(), 3.0);
    assert_eq!(
        it.current_block_bound(),
        Some(BlockBound {
            last_doc_id: 2,
            max_score: 5.0
        })
    );

    let threshold = MinCompetitiveScore::new(4.5).expect("a finite threshold applies");
    assert_eq!(
        it.read_with_threshold(threshold).unwrap().unwrap().doc_id,
        2
    );
    assert!(it.read_with_threshold(threshold).unwrap().is_none());
}

/// A C-ABI parent sees through the decorator to its child's sort weight: here
/// the weight a full union reports when union children are prioritized.
#[test]
fn intersection_sort_weight_through_the_c_abi() {
    let leaves = vec![
        CRQEIterator::from_rust_leaf(Mock::new([1])),
        CRQEIterator::from_rust_leaf(Mock::new([2])),
        CRQEIterator::from_rust_leaf(Mock::new([3])),
    ];
    let union = build_union(leaves, false, 20, QueryNodeType::Union, 1.0);
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    let child = unsafe { CRQEIterator::new(union) };
    let decorator = IndexLoad::new(child);
    // SAFETY: (1)–(4) `boxed_new` returns a valid, owning iterator handle.
    let it = unsafe { CRQEIterator::new(index_load::boxed_new(decorator)) };

    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(it.intersection_sort_weight(true), 3.0);
    assert_eq!(it.intersection_sort_weight(false), 1.0);
}

/// The handle pipeline construction writes through is bound right after the
/// decorator is built, but profiling re-boxes compound iterators before the
/// key is written. The slot must survive that move.
#[test]
fn key_slot_survives_profiling() {
    let mut key = FakeKey::new();
    let mut decorator = IndexLoad::new(CRQEIterator::from_rust_leaf(Mock::new_with_values(
        [4],
        [40.0],
    )));
    let mut handle = RLookupKeyHandle {
        key_ptr: decorator.key_slot(),
        is_valid: true,
    };
    // SAFETY: (1) `handle` outlives the iterator, no handle was attached
    // before, and nothing else borrows `handle` while the iterator is dropped.
    unsafe { decorator.set_handle(Some(NonNull::from(&mut handle))) };
    let header = index_load::boxed_new(decorator);

    // SAFETY: (1)–(4) `header` is a valid, owning iterator handle.
    let mut profiled = unsafe { CRQEIterator::new(header) }.into_profiled();

    assert!(
        handle.is_valid,
        "profiling moves the decorator, it does not free it"
    );
    // SAFETY: the handle is valid, so `key_ptr` still points at the live slot.
    unsafe { *handle.key_ptr = key.as_ptr() };

    let result = profiled.read().unwrap().unwrap();
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 40.0));

    drop(profiled);
    assert!(!handle.is_valid);
}

#[test]
fn read_with_threshold_attaches() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([1], [5.0]));
    bind(&mut it, &mut key);

    let threshold = MinCompetitiveScore::new(1.0).expect("a finite threshold applies");
    let result = it.read_with_threshold(threshold).unwrap().unwrap();
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 5.0));
}

/// A full union over a loading child and a sibling that aborts or moves on
/// revalidation stays on its document. The value it took from the loading child
/// when it read that document must survive the rebuild, although the loading
/// child itself reports no move and pushes nothing again.
fn union_over_loading_child(
    key: &mut FakeKey,
    sibling_outcome: MockRevalidateResult,
) -> CRQEIterator {
    let mut loading = IndexLoad::new(CRQEIterator::from_rust_leaf(Mock::new_with_values(
        [1, 2],
        [10.0, 20.0],
    )));
    bind(&mut loading, key);
    let sibling = Mock::new([1, 3]);
    sibling.data().set_revalidate_result(sibling_outcome);
    // SAFETY: (1)–(4) `boxed_new` returns a valid, owning iterator handle.
    let loading = unsafe { CRQEIterator::new(index_load::boxed_new(loading)) };
    let children = vec![loading, CRQEIterator::from_rust_leaf(sibling)];
    let union = build_union(children, false, 20, QueryNodeType::Union, 1.0);
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    unsafe { CRQEIterator::new(union) }
}

#[rstest]
fn union_keeps_the_value_when_a_sibling_changes_on_revalidate(
    #[values(MockRevalidateResult::Abort, MockRevalidateResult::Move)]
    sibling_outcome: MockRevalidateResult,
) {
    let mock_ctx = MockContext::new(0, 0);
    let mut key = FakeKey::new();
    let mut it = union_over_loading_child(&mut key, sibling_outcome);

    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 10.0));

    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "the union stays on doc 1"
    );
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 10.0));

    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 2);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 20.0));
}

#[rstest]
fn union_keeps_the_value_when_a_sibling_changes_on_resume(
    #[values(MockRevalidateResult::Abort, MockRevalidateResult::Move)]
    sibling_outcome: MockRevalidateResult,
) {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut key = FakeKey::new();
    let mut it = union_over_loading_child(&mut key, sibling_outcome);

    let result = it.read().unwrap().unwrap();
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 10.0));

    let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
        .unwrap()
        .expect_ok();
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(sole_metric(result), (fake_key_ptr(&key), 10.0));
}
