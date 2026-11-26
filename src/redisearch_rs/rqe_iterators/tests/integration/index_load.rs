/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use index_result::{RSIndexResult, YieldedData, YieldedValues};
use query_types::QueryNodeType;
use rlookup::RLookupKeyHandle;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome, TypeErasedRQEIterator,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    index_load,
    index_load::{HitNumber, IndexLoad, KeyCell, ValueSource},
    score_bound::{BlockBound, MinCompetitiveScore},
    union_opaque::{build_union, is_union_wrapper},
};
use rqe_iterators_test_utils::{
    ContractChecker, FakeKey, MockContext, ResumeOutcomeExt, revalidate_via_resume,
};
use rstest::rstest;

use crate::utils::{
    Mock, MockIteratorError, MockRevalidateResult,
    blocked::{Block, Blocked},
};

/// Bind `key` into `it`'s key slot, as pipeline construction does.
fn bind<'index, I: RQEIterator<'index>, V: ValueSource>(
    it: &mut IndexLoad<'index, I, V>,
    key: &mut FakeKey,
) {
    // SAFETY: the slot is the decorator's live heap cell, and nothing reads it
    // while it is written.
    unsafe { *it.key_slot().as_ptr() = key.as_ptr() };
}

/// A key cell tracked by `handle`, as evaluation binds one.
fn cell_tracked_by(handle: &mut RLookupKeyHandle<'static>) -> KeyCell<'static> {
    let mut cell = KeyCell::new();
    handle.key_ptr = cell.slot().as_ptr();
    // SAFETY: (1) `handle` outlives the cell, no handle was attached before,
    // and nothing else borrows `handle` while the cell is dropped.
    unsafe { cell.set_handle(NonNull::from(handle)) };
    cell
}

/// The single `(key, value)` metric `result` carries.
fn sole_yielded_value(result: &RSIndexResult<'_>) -> (*const ffi::RLookupKey, f64) {
    let values = result.yielded_values();
    assert_eq!(values.len(), 1, "expected exactly one yielded value");
    let entry = values.get(0).unwrap();
    let key = entry
        .key()
        .expect("the yielded value must carry the bound key");
    (
        key as *const _,
        entry.value().as_number().expect("a numeric value"),
    )
}

#[test]
fn reports_the_childs_type() {
    let it = IndexLoad::new(Mock::new_with_values([1], [1.0])).unwrap();
    assert_eq!(it.type_(), IteratorType::Mock);
}

#[test]
fn refuses_a_child_whose_type_it_may_not_report() {
    let refused = IndexLoad::new(IdListSorted::new(vec![1]))
        .map(|_| ())
        .map_err(|refused| refused.type_());
    assert_eq!(refused, Err(IteratorType::IdListSorted));
}

#[test]
fn read_attaches_each_hits_value() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([2, 5, 9], [20.0, 50.0, 90.0])).unwrap();
    bind(&mut it, &mut key);
    let mut it = ContractChecker::new(it);

    for (doc_id, value) in [(2, 20.0), (5, 50.0), (9, 90.0)] {
        let result = it.read().unwrap().unwrap();
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), value));
    }
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn skip_to_attaches_found_and_not_found_hits() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([2, 5, 9], [20.0, 50.0, 90.0])).unwrap();
    bind(&mut it, &mut key);
    let mut it = ContractChecker::new(it);

    let Some(SkipToOutcome::Found(result)) = it.skip_to(5).unwrap() else {
        panic!("doc 5 is in the child");
    };
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 50.0));

    // A parent keeps a `NotFound` hit as its next candidate.
    let Some(SkipToOutcome::NotFound(result)) = it.skip_to(7).unwrap() else {
        panic!("doc 9 follows doc 7");
    };
    assert_eq!(result.doc_id, 9);
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 90.0));

    assert!(it.skip_to(10).unwrap().is_none());
}

#[test]
fn unbound_key_pushes_nothing() {
    let mut it =
        ContractChecker::new(IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0])).unwrap());

    let result = it.read().unwrap().unwrap();
    assert!(result.yielded_values().is_empty());
    let Some(SkipToOutcome::Found(result)) = it.skip_to(2).unwrap() else {
        panic!("doc 2 is in the child");
    };
    assert!(result.yielded_values().is_empty());
}

#[test]
fn non_numeric_hit_pushes_nothing() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new([1])).unwrap();
    bind(&mut it, &mut key);

    let result = it.read().unwrap().unwrap();
    assert!(result.yielded_values().is_empty());
}

#[test]
fn yielded_values_are_replaced_not_accumulated() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0])).unwrap();
    bind(&mut it, &mut key);

    // A yielded value left on the shared result by an earlier hit must not leak into
    // the next one.
    let result = it.read().unwrap().unwrap();
    result.yielded_values_mut().push_without_key(-1.0);
    let result = it.read().unwrap().unwrap();
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 2.0));
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
    let mut it = IndexLoad::new(unsafe { CRQEIterator::new(union) }).unwrap();
    bind(&mut it, &mut key);

    for (doc_id, value) in [(1, 10.0), (2, 20.0), (3, 30.0)] {
        let result = it.read().unwrap().unwrap();
        assert_eq!(result.doc_id, doc_id);
        assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), value));
    }
}

#[test]
fn forwards_to_the_child() {
    let mut it =
        ContractChecker::new(IndexLoad::new(Mock::new_with_values([3, 4], [3.0, 4.0])).unwrap());
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
    let mut it = IndexLoad::new(child).unwrap();
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
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 2.0));
}

#[test]
fn resume_moved_reattaches() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut key = FakeKey::new();
    let child = Mock::new_with_values([1, 2, 3], [1.0, 2.0, 3.0]);
    let mut data = child.data();
    let mut it = Box::new(IndexLoad::new(child).unwrap());
    bind(&mut it, &mut key);

    // A parent takes the yielded values of the hit it consumed.
    let result = it.read().unwrap().unwrap();
    drop(std::mem::take(result.yielded_values_mut()));

    data.set_revalidate_result(MockRevalidateResult::Move);
    let ResumeOutcome::Moved(mut it) = it.suspend().resume(&guard).unwrap() else {
        panic!("the child moved onto doc 2");
    };
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 2);
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 2.0));
}

#[test]
fn resume_ok_keeps_position_and_allocation() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(IndexLoad::new(Mock::new_with_values([1, 2], [1.0, 2.0])).unwrap());
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
    let key = cell_tracked_by(&mut handle);
    let it = Box::new(IndexLoad::with_source(child, key, HitNumber).unwrap());

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
    let key = cell_tracked_by(&mut handle);
    let it = Box::new(IndexLoad::with_source(child, key, HitNumber).unwrap());

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
    ]))
    .unwrap();
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
    let decorator = IndexLoad::new(child).unwrap();
    // SAFETY: (1)–(4) `boxed_new` returns a valid, owning iterator handle.
    let it = unsafe { CRQEIterator::new(index_load::boxed_new(decorator)) };

    assert_eq!(it.type_(), IteratorType::Union);
    assert_eq!(it.intersection_sort_weight(true), 3.0);
    assert_eq!(it.intersection_sort_weight(false), 1.0);
}

/// A decorator reporting `Union` is not mistaken for the union it wraps by the
/// one C-ABI entry point that casts on that tag.
#[test]
fn a_decorated_union_is_not_a_union_wrapper() {
    let leaves = vec![
        CRQEIterator::from_rust_leaf(Mock::new([1])),
        CRQEIterator::from_rust_leaf(Mock::new([2])),
    ];
    let union = build_union(leaves, false, 20, QueryNodeType::Union, 1.0);
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    let union = unsafe { CRQEIterator::new(union) };
    assert!(is_union_wrapper(&union));

    let decorator = IndexLoad::new(union).unwrap();
    // SAFETY: (1)–(4) `boxed_new` returns a valid, owning iterator handle.
    let it = unsafe { CRQEIterator::new(index_load::boxed_new(decorator)) };
    assert_eq!(it.type_(), IteratorType::Union);
    assert!(!is_union_wrapper(&it));
}

/// The handle pipeline construction writes through is bound right after the
/// decorator is built, but profiling re-boxes compound iterators before the
/// key is written. The slot must survive that move.
#[test]
fn key_slot_survives_profiling() {
    let mut key = FakeKey::new();
    let mut handle = RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    };
    let decorator = IndexLoad::with_source(
        CRQEIterator::from_rust_leaf(Mock::new_with_values([4], [40.0])),
        cell_tracked_by(&mut handle),
        HitNumber,
    )
    .unwrap();
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
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 40.0));

    drop(profiled);
    assert!(!handle.is_valid);
}

#[test]
fn read_with_threshold_attaches() {
    let mut key = FakeKey::new();
    let mut it = IndexLoad::new(Mock::new_with_values([1], [5.0])).unwrap();
    bind(&mut it, &mut key);

    let threshold = MinCompetitiveScore::new(1.0).expect("a finite threshold applies");
    let result = it.read_with_threshold(threshold).unwrap().unwrap();
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 5.0));
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
    )))
    .unwrap();
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
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 10.0));

    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert!(
        matches!(status, RQEValidateStatus::Ok),
        "the union stays on doc 1"
    );
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 10.0));

    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 2);
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 20.0));
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
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 10.0));

    let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
        .unwrap()
        .expect_ok();
    let result = it.current().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(sole_yielded_value(result), (key.as_yielded_key(), 10.0));
}

#[test]
fn string_value_holds_its_bytes_and_no_number() {
    let value = YieldedData::String(index_load::shared_string(b"tag\xff".to_vec()));

    let YieldedData::String(string) = &value else {
        unreachable!("built as a string");
    };
    assert_eq!(string.as_bytes(), b"tag\xff");
    assert_eq!(value.as_number(), None);
}

#[test]
fn string_value_clone_shares_the_allocation() {
    let string = index_load::shared_string(b"red".to_vec());
    let clone = string.clone();

    assert_eq!(string.as_ptr(), clone.as_ptr());
    assert_eq!(index_load::shared_value_refcount(&string), Some(2));
    drop(clone);
    assert_eq!(index_load::shared_value_refcount(&string), Some(1));
}

#[test]
fn string_values_compare_by_content() {
    let red = YieldedData::String(index_load::shared_string(b"red".to_vec()));
    let other_red = YieldedData::String(index_load::shared_string(b"red".to_vec()));
    let blue = YieldedData::String(index_load::shared_string(b"blue".to_vec()));

    assert_eq!(
        red, other_red,
        "separate allocations of one value are equal"
    );
    assert_ne!(red, blue);
    assert_ne!(red, YieldedData::Number(0.0));
}

#[test]
fn yielded_values_clone_shares_string_values() {
    let string = index_load::shared_string(b"red".to_vec());
    let mut v = YieldedValues::new();
    v.push_without_key(YieldedData::String(string.clone()));

    let cloned = v.clone();
    assert_eq!(v, cloned);
    assert_eq!(index_load::shared_value_refcount(&string), Some(3));
    drop(cloned);
    v.reset();
    assert_eq!(index_load::shared_value_refcount(&string), Some(1));
}
