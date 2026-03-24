/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the UnionFlat iterator variants.
//!
//! These tests use [`Mock`] iterators which provide observability features
//! like `read_count()` and `revalidate_count()` that allow us to verify not just
//! correctness but also efficiency optimizations like the ReuseResults optimization.

use crate::utils::{Mock, MockData, MockRevalidateResult, MockVec};
use ffi::t_docId;
use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome, UnionFullFlat, UnionQuickFlat};
use rstest_reuse::{apply, template};

/// Test cases for union tests: (num_children, base_result_set).
#[template]
#[rstest::rstest]
#[case::c2_small(2, &[1u64, 2, 3, 40, 50])]
#[case::c2_medium(2, &[5u64, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345])]
#[case::c2_large(2, &[9u64, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130])]
#[case::c5_small(5, &[1u64, 2, 3, 40, 50])]
#[case::c5_medium(5, &[5u64, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345])]
#[case::c5_large(5, &[9u64, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130])]
#[case::c10_small(10, &[1u64, 2, 3, 40, 50])]
#[case::c10_medium(10, &[5u64, 6, 7, 24, 25, 46, 47, 48, 49, 50, 51, 234, 2345])]
#[case::c10_large(10, &[9u64, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130])]
fn union_cases(#[case] num_children: usize, #[case] base_result_set: &[u64]) {}

/// Type alias for the union variant used in these tests.
/// Using `UnionFullFlat` which aggregates all matching children (not quick exit)
/// and uses flat array iteration (not heap).
type Union<I> = UnionFullFlat<'static, I>;

fn create_mock_1<const N: usize>(ids: [t_docId; N]) -> (Box<dyn RQEIterator<'static>>, MockData) {
    let c = Mock::<N>::new(ids);
    let d = c.data();
    (Box::new(c), d)
}

fn create_mock_2<const N1: usize, const N2: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let d1 = c1.data();
    let d2 = c2.data();
    (vec![Box::new(c1), Box::new(c2)], vec![d1, d2])
}

fn create_mock_3<const N1: usize, const N2: usize, const N3: usize>(
    ids1: [t_docId; N1],
    ids2: [t_docId; N2],
    ids3: [t_docId; N3],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<MockData>) {
    let c1 = Mock::<N1>::new(ids1);
    let c2 = Mock::<N2>::new(ids2);
    let c3 = Mock::<N3>::new(ids3);
    let d1 = c1.data();
    let d2 = c2.data();
    let d3 = c3.data();
    (
        vec![Box::new(c1), Box::new(c2), Box::new(c3)],
        vec![d1, d2, d3],
    )
}

fn create_union_children(
    num_children: usize,
    base_result_set: &[t_docId],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<t_docId>) {
    let mut children: Vec<Box<dyn RQEIterator<'static>>> = Vec::with_capacity(num_children);
    let mut all_ids = Vec::new();
    let mut next_unique_id: t_docId = 10000;

    for i in 0..num_children {
        let mut child_ids = Vec::new();

        for (j, &id) in base_result_set.iter().enumerate() {
            if j % num_children == i || j % 2 == 0 {
                child_ids.push(id);
            }
        }

        for _ in 0..50 {
            child_ids.push(next_unique_id);
            next_unique_id += 1;
        }

        child_ids.sort();
        child_ids.dedup();
        all_ids.extend(child_ids.iter().copied());
        children.push(MockVec::new_boxed(child_ids));
    }

    all_ids.sort();
    all_ids.dedup();
    (children, all_ids)
}

// =============================================================================
// Read tests
// =============================================================================

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn read(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
    let (children, expected) = create_union_children(num_children, base_result_set);

    let mut union_iter = Union::new(children);
    let mut count = 0;
    while let Ok(Some(result)) = union_iter.read() {
        assert_eq!(
            result.doc_id, expected[count],
            "num_children={num_children}, expected doc_id={}, got={}",
            expected[count], result.doc_id
        );
        assert_eq!(
            union_iter.last_doc_id(),
            expected[count],
            "num_children={num_children}, last_doc_id mismatch"
        );
        assert!(
            !union_iter.at_eof(),
            "num_children={num_children}, should not be at EOF yet"
        );
        count += 1;
    }

    assert!(
        union_iter.at_eof(),
        "num_children={num_children}, should be at EOF"
    );
    assert!(
        matches!(union_iter.read(), Ok(None)),
        "num_children={num_children}, reading after EOF should return None"
    );
    assert_eq!(
        count,
        expected.len(),
        "num_children={num_children}, expected {} documents, got {}",
        expected.len(),
        count
    );
}

// =============================================================================
// SkipTo tests
// =============================================================================

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn skip_to(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
    let (children, expected) = create_union_children(num_children, base_result_set);
    let mut union_iter = Union::new(children);

    for &id in &expected {
        union_iter.rewind();
        let outcome = union_iter.skip_to(id).expect("skip_to failed");
        match outcome {
            Some(SkipToOutcome::Found(result)) => {
                assert_eq!(
                    result.doc_id, id,
                    "num_children={num_children}, skip_to({id}) should find {id}"
                );
            }
            Some(SkipToOutcome::NotFound(_)) => {}
            None => panic!("num_children={num_children}, skip_to({id}) should not return None"),
        }
    }

    union_iter.rewind();
    let last_id = *expected.last().unwrap();
    assert!(
        matches!(union_iter.skip_to(last_id + 1000), Ok(None)),
        "num_children={num_children}, skip beyond last should be EOF"
    );
    assert!(
        union_iter.at_eof(),
        "num_children={num_children}, should be at EOF after skip beyond"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn skip_to_edge_cases() {
    // Quick mode - child already at target doc_id
    {
        let (children, data) = create_mock_2([50, 200], [100, 250]);
        let mut quick_iter = UnionQuickFlat::new(children);
        quick_iter.read().expect("read failed").unwrap();
        assert_eq!(quick_iter.last_doc_id(), 50);
        let outcome = quick_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
        assert_eq!(quick_iter.last_doc_id(), 100);
        assert_eq!(data[0].read_count(), 2);
        assert_eq!(data[1].read_count(), 1, "reused cached position");
    }

    // Quick mode - child exhausts during skip_to
    {
        let (children, _data) = create_mock_2([10, 30], [20, 200]);
        let mut quick_iter = UnionQuickFlat::new(children);
        quick_iter.read().expect("read failed").unwrap();
        let outcome = quick_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
        assert_eq!(quick_iter.last_doc_id(), 200);
    }

    // Full mode - child at EOF before skip_to
    {
        let (children, data) = create_mock_2([10, 20], [15, 50, 100]);
        let mut full_iter = UnionFullFlat::new(children);

        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 10);
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 15);
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 20);

        let outcome = full_iter.skip_to(30).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
        assert_eq!(full_iter.last_doc_id(), 50);

        let outcome = full_iter.read().expect("read failed").unwrap();
        assert_eq!(outcome.doc_id, 100);
        assert!(matches!(full_iter.read(), Ok(None)));
        assert!(full_iter.at_eof());

        assert_eq!(data[0].read_count(), 3);
        assert_eq!(data[1].read_count(), 4);
    }

    // Full mode - child already at target doc_id
    {
        let (children, data) = create_mock_2([50, 150, 200], [100, 150, 250]);
        let mut full_iter = UnionFullFlat::new(children);
        full_iter.read().expect("read failed").unwrap();
        assert_eq!(full_iter.last_doc_id(), 50);

        let outcome = full_iter.skip_to(100).expect("skip_to failed");
        assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
        assert_eq!(full_iter.last_doc_id(), 100);

        assert_eq!(data[0].read_count(), 2);
        assert_eq!(data[1].read_count(), 1, "already at target");
    }
}

// =============================================================================
// Rewind tests
// =============================================================================

#[apply(union_cases)]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn rewind(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
    let (children, expected) = create_union_children(num_children, base_result_set);
    let mut union_iter = Union::new(children);

    for i in 0..5 {
        for j in 0..=i.min(expected.len() - 1) {
            let result = union_iter.read().expect("read failed");
            assert!(
                result.is_some(),
                "num_children={num_children}, read({j}) in iteration {i} failed"
            );
            let result = result.unwrap();
            assert_eq!(
                result.doc_id, expected[j],
                "num_children={num_children}, wrong doc_id"
            );
        }
        union_iter.rewind();
        assert_eq!(
            union_iter.last_doc_id(),
            0,
            "num_children={num_children}, last_doc_id should be 0 after rewind"
        );
        assert!(
            !union_iter.at_eof(),
            "num_children={num_children}, should not be at EOF after rewind"
        );
    }
}

// =============================================================================
// Edge case tests
// =============================================================================

#[test]
fn edge_case_no_children() {
    let children: Vec<Box<dyn RQEIterator<'static>>> = vec![];
    let mut union_iter = Union::new(children);

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(union_iter.num_estimated(), 0);
    assert!(matches!(union_iter.skip_to(1), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_single_child() {
    let (child, child_data) = create_mock_1([10, 20, 30, 40, 50]);
    let mut union_iter = Union::new(vec![child]);

    let expected = [10, 20, 30, 40, 50];
    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some());
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(child_data.read_count(), 6);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_disjoint_children() {
    let (children, data) = create_mock_3([1, 2, 3], [10, 20, 30], [100, 200, 300]);

    let mut union_iter = Union::new(children);
    let expected = [1, 2, 3, 10, 20, 30, 100, 200, 300];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());

    assert_eq!(data[0].read_count(), 4);
    assert_eq!(data[1].read_count(), 4);
    assert_eq!(data[2].read_count(), 4);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_overlapping_children() {
    let (children, data) = create_mock_3(
        [1, 2, 5, 10, 15, 20],
        [2, 5, 8, 10, 18, 20],
        [3, 5, 10, 12, 20, 25],
    );

    let mut union_iter = Union::new(children);
    // Union: 1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25
    let expected = [1, 2, 3, 5, 8, 10, 12, 15, 18, 20, 25];

    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }

    assert!(matches!(union_iter.read(), Ok(None)));
    assert_eq!(data[0].read_count(), 7);
    assert_eq!(data[1].read_count(), 7);
    assert_eq!(data[2].read_count(), 7);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_exact_match() {
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

    let outcome = union_iter.skip_to(30).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::Found(result)) => {
            assert_eq!(result.doc_id, 30);
        }
        _ => panic!("Expected Found(30)"),
    }
    assert_eq!(union_iter.last_doc_id(), 30);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_not_found() {
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

    let outcome = union_iter.skip_to(22).expect("skip_to failed");
    match outcome {
        Some(SkipToOutcome::NotFound(result)) => {
            assert_eq!(result.doc_id, 25);
        }
        _ => panic!("Expected NotFound landing on 25"),
    }
    assert_eq!(union_iter.last_doc_id(), 25);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_past_eof() {
    let (children, _) = create_mock_2([10, 20, 30], [15, 25, 35]);
    let mut union_iter = Union::new(children);

    assert!(matches!(union_iter.skip_to(100), Ok(None)));
    assert!(union_iter.at_eof());

    union_iter.rewind();
    assert!(!union_iter.at_eof());
    let result = union_iter.read().expect("read failed");
    assert!(result.is_some());
    assert_eq!(result.unwrap().doc_id, 10);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_interleaved_read_and_skip_to() {
    let (children, _) = create_mock_2(
        [10, 20, 30, 40, 50, 60, 70, 80],
        [15, 25, 35, 45, 55, 65, 75, 85],
    );
    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let outcome = union_iter.skip_to(35).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(union_iter.last_doc_id(), 35);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 40);

    let outcome = union_iter.skip_to(70).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    assert_eq!(union_iter.last_doc_id(), 70);
}

#[test]
fn edge_case_num_estimated_is_sum() {
    let (children, _) = create_mock_3(
        [1, 2, 3, 4, 5],
        [10, 20, 30, 40, 50],
        [100, 200, 300, 400, 500],
    );
    let union_iter = Union::new(children);

    assert_eq!(union_iter.num_estimated(), 15);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_empty_children_mixed_with_non_empty() {
    let empty_child: Mock<'static, 0> = Mock::new([]);
    let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);
    let child2: Mock<'static, 3> = Mock::new([15, 25, 35]);

    let children: Vec<Box<dyn RQEIterator<'static>>> =
        vec![Box::new(empty_child), Box::new(child1), Box::new(child2)];
    let mut union_iter = Union::new(children);

    let expected = vec![10, 15, 20, 25, 30, 35];
    for &expected_id in &expected {
        let result = union_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_all_children_empty() {
    let empty1: Mock<'static, 0> = Mock::new([]);
    let empty2: Mock<'static, 0> = Mock::new([]);
    let empty3: Mock<'static, 0> = Mock::new([]);

    let children: Vec<Box<dyn RQEIterator<'static>>> =
        vec![Box::new(empty1), Box::new(empty2), Box::new(empty3)];
    let mut union_iter = Union::new(children);
    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
    assert_eq!(union_iter.num_estimated(), 0);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_child_already_past_target() {
    let (children, data) = create_mock_2([10, 50, 100], [20, 60, 110]);
    let mut union_iter = Union::new(children);

    union_iter.read().expect("read failed");
    union_iter.read().expect("read failed");

    let outcome = union_iter.skip_to(40).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
    assert_eq!(union_iter.last_doc_id(), 50);

    assert_eq!(data[0].read_count(), 2);
    assert_eq!(data[1].read_count(), 2);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_exhausts_some_children() {
    let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 100]);
    let mut union_iter = Union::new(children);

    let outcome = union_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
    assert_eq!(union_iter.last_doc_id(), 100);

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_skip_to_exhausts_all_children() {
    let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 35]);
    let mut union_iter = Union::new(children);

    let outcome = union_iter.skip_to(1000).expect("skip_to failed");
    assert!(outcome.is_none());
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_initialize_with_empty_children() {
    let empty1: Mock<'static, 0> = Mock::new([]);
    let child1: Mock<'static, 2> = Mock::new([10, 20]);
    let empty2: Mock<'static, 0> = Mock::new([]);
    let child2: Mock<'static, 2> = Mock::new([15, 25]);
    let empty3: Mock<'static, 0> = Mock::new([]);

    let children: Vec<Box<dyn RQEIterator<'static>>> = vec![
        Box::new(empty1),
        Box::new(child1),
        Box::new(empty2),
        Box::new(child2),
        Box::new(empty3),
    ];
    let mut quick_iter = UnionQuickFlat::new(children);

    let expected = vec![10, 15, 20, 25];
    for &expected_id in &expected {
        let result = quick_iter.read().expect("read failed");
        assert!(result.is_some(), "Expected doc {expected_id}");
        assert_eq!(result.unwrap().doc_id, expected_id);
    }
    assert!(matches!(quick_iter.read(), Ok(None)));
    assert!(quick_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn edge_case_misbehaving_child_returns_none_during_init() {
    let mock1: Mock<'static, 3> = Mock::new([10, 30, 50]);
    let mock2: Mock<'static, 3> = Mock::new([20, 40, 60]);

    let mut data1 = mock1.data();
    data1.set_force_read_none(true);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 40);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 60);

    assert!(matches!(union_iter.read(), Ok(None)));
    assert!(union_iter.at_eof());
}

// =============================================================================
// Revalidate tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_ok() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 15);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_moved() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
        "Expected Moved with current, got {:?}",
        status
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_after_eof() {
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 2> = Mock::new([15, 25]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));
    assert!(union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_single_child_aborts() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);
    let child2: Mock<'static, 5> = Mock::new([12, 22, 32, 42, 52]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        !matches!(status, RQEValidateStatus::Aborted),
        "Union should not abort when only one child aborts"
    );
    assert!(!union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_all_children_abort() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        matches!(status, RQEValidateStatus::Aborted),
        "Union should abort when all children abort"
    );
    assert!(
        union_iter.at_eof(),
        "Union should be at EOF after all children abort"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_child_moves_to_eof() {
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 15);
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));

    assert!(!union_iter.at_eof());
    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 25);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_mixed_ok_moved_abort() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([15, 25, 35, 45, 55]);
    let child2: Mock<'static, 5> = Mock::new([12, 22, 32, 42, 52]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child2
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1), Box::new(child2)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(
        !matches!(status, RQEValidateStatus::Aborted),
        "Union should not abort when some children are still Ok"
    );
    assert!(!union_iter.at_eof());
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_all_children_move_to_eof() {
    let child0: Mock<'static, 2> = Mock::new([10, 20]);
    let child1: Mock<'static, 2> = Mock::new([15, 25]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());

    let status = union_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_updates_to_new_minimum() {
    let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
    let child1: Mock<'static, 5> = Mock::new([5, 25, 35, 45, 55]);

    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(child0), Box::new(child1)];

    let mut union_iter = Union::new(children);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 5);

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    assert!(!union_iter.at_eof());

    let result = union_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_when_already_at_eof() {
    let mock1: Mock<'static, 2> = Mock::new([10, 20]);
    let mock2: Mock<'static, 2> = Mock::new([10, 30]);

    mock1.data().set_revalidate_result(MockRevalidateResult::Ok);
    mock2.data().set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    let mut quick_iter = UnionQuickFlat::new(children);

    let mut read_docs = Vec::new();
    while let Some(result) = quick_iter.read().expect("read failed") {
        read_docs.push(result.doc_id);
    }
    assert_eq!(read_docs, vec![10, 20, 30]);
    assert!(quick_iter.at_eof());
    let status = quick_iter.revalidate().expect("revalidate failed");
    assert!(matches!(status, RQEValidateStatus::Ok));
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_with_children_at_eof() {
    // Test 1: Child moves to EOF during revalidate
    {
        let mock1: Mock<'static, 3> = Mock::new([10, 20, 30]);
        let mock2: Mock<'static, 3> = Mock::new([5, 25, 50]);

        let mut data1 = mock1.data();

        let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
            vec![Box::new(mock1), Box::new(mock2)];

        let mut union_iter = Union::new(children);

        let result = union_iter.read().expect("read failed").unwrap();
        assert_eq!(result.doc_id, 5);

        let result = union_iter.read().expect("read failed").unwrap();
        assert_eq!(result.doc_id, 10);

        data1.set_revalidate_result(MockRevalidateResult::Move);

        let status = union_iter.revalidate().expect("revalidate failed");
        assert!(matches!(
            status,
            RQEValidateStatus::Moved { current: Some(_) }
        ));
    }

    // Test 2: ALL children move to EOF during revalidate
    {
        let mock1: Mock<'static, 3> = Mock::new([10, 20, 30]);
        let mock2: Mock<'static, 3> = Mock::new([10, 25, 35]);

        let mut data1 = mock1.data();
        let mut data2 = mock2.data();

        let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
            vec![Box::new(mock1), Box::new(mock2)];

        let mut union_iter = Union::new(children);

        union_iter.read().expect("read failed").unwrap();
        union_iter.read().expect("read failed").unwrap();

        data1.set_revalidate_result(MockRevalidateResult::Move);
        data2.set_revalidate_result(MockRevalidateResult::Move);

        let status = union_iter.revalidate().expect("revalidate failed");
        assert!(matches!(status, RQEValidateStatus::Moved { current: None }));
        assert!(union_iter.at_eof());
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn revalidate_quick_triggers_quick_exit() {
    let mock1: Mock<'static, 3> = Mock::new([10, 30, 50]);
    let mock2: Mock<'static, 3> = Mock::new([20, 40, 60]);

    let mut data1 = mock1.data();
    let mut data2 = mock2.data();

    data1.set_revalidate_result(MockRevalidateResult::Ok);
    data2.set_revalidate_result(MockRevalidateResult::Ok);

    let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
        vec![Box::new(mock1), Box::new(mock2)];

    let mut quick_iter = UnionQuickFlat::new(children);

    let result = quick_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    data1.set_revalidate_result(MockRevalidateResult::Move);
    data2.set_revalidate_result(MockRevalidateResult::Ok);

    let status = quick_iter.revalidate().expect("revalidate failed");
    assert!(matches!(
        status,
        RQEValidateStatus::Moved { current: Some(_) }
    ));
    assert_eq!(quick_iter.last_doc_id(), 20);
}

// =============================================================================
// Current tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn current_after_operations() {
    let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut union_iter = Union::new(children);

    assert!(union_iter.current().is_some());
    assert_eq!(union_iter.last_doc_id(), 0);

    let _ = union_iter.read().expect("read failed");
    let current = union_iter.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 10);

    let _ = union_iter.skip_to(30).expect("skip_to failed");
    let current = union_iter.current();
    assert!(current.is_some());
    assert_eq!(current.unwrap().doc_id, 30);

    union_iter.rewind();
    assert!(union_iter.current().is_some());
    assert_eq!(union_iter.last_doc_id(), 0);

    while union_iter.read().expect("read failed").is_some() {}
    assert!(union_iter.at_eof());
    assert!(union_iter.current().is_none());
}

// =============================================================================
// Quick vs Full mode tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_quick_variant_produces_same_doc_ids() {
    let (full_children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut full_iter = UnionFullFlat::new(full_children);
    let mut full_results = Vec::new();
    while let Some(result) = full_iter.read().expect("read failed") {
        full_results.push(result.doc_id);
    }

    let (quick_children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
    let mut quick_iter = UnionQuickFlat::new(quick_children);
    let mut quick_results = Vec::new();
    while let Some(result) = quick_iter.read().expect("read failed") {
        quick_results.push(result.doc_id);
    }

    assert_eq!(full_results, quick_results);
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_full_aggregates_all_matching_children() {
    let (children, _) = create_mock_3([10, 20, 30], [10, 25, 35], [10, 28, 38]);
    let mut full_iter = UnionFullFlat::new(children);

    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let aggregate = result.as_aggregate().expect("Expected aggregate result");
    assert_eq!(
        aggregate.len(),
        3,
        "Full mode should aggregate all 3 children matching doc 10"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_quick_takes_first_matching_child_only() {
    let (children, _) = create_mock_3([10, 20, 30], [10, 25, 35], [10, 28, 38]);
    let mut quick_iter = UnionQuickFlat::new(children);

    let result = quick_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);

    let aggregate = result.as_aggregate().expect("Expected aggregate result");
    assert_eq!(
        aggregate.len(),
        1,
        "Quick mode should only take first matching child"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_full_aggregates_correct_number_of_children() {
    let (children, _) = create_mock_3([10, 20, 30], [10, 25], [10, 30]);
    let mut full_iter = UnionFullFlat::new(children);

    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 10);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        3,
        "doc 10: all 3 children match"
    );

    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 20);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "doc 20: only child1 matches"
    );

    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 25);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "doc 25: only child2 matches"
    );

    let result = full_iter.read().expect("read failed").unwrap();
    assert_eq!(result.doc_id, 30);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        2,
        "doc 30: child1 and child3 match"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_quick_always_has_one_child() {
    let (children, _) = create_mock_3([10, 20, 30], [10, 25], [10, 30]);
    let mut quick_iter = UnionQuickFlat::new(children);

    while let Some(result) = quick_iter.read().expect("read failed") {
        assert_eq!(
            result.as_aggregate().unwrap().len(),
            1,
            "Quick mode should always have exactly 1 child in aggregate"
        );
    }
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn mode_quick_vs_full_with_skip_to() {
    let (full_children, _) = create_mock_3([10, 30, 50], [20, 40, 50], [25, 45, 50]);
    let mut full_iter = UnionFullFlat::new(full_children);

    let outcome = full_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    let result = full_iter.current().unwrap();
    assert_eq!(result.doc_id, 50);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        3,
        "Full mode skip_to should aggregate all 3 children"
    );

    let (quick_children, _) = create_mock_3([10, 30, 50], [20, 40, 50], [25, 45, 50]);
    let mut quick_iter = UnionQuickFlat::new(quick_children);

    let outcome = quick_iter.skip_to(50).expect("skip_to failed");
    assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
    let result = quick_iter.current().unwrap();
    assert_eq!(result.doc_id, 50);
    assert_eq!(
        result.as_aggregate().unwrap().len(),
        1,
        "Quick mode skip_to should only take first child"
    );
}

// =============================================================================
// Reuse results optimization tests
// =============================================================================

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn reuse_results_optimization_quick_mode() {
    let (children, data) = create_mock_2([3], [2]);
    let mut union = UnionQuickFlat::new(children);

    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 2);
    assert_eq!(
        data[0].read_count(),
        1,
        "child0 should be read once for initial heap setup"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "child1 should be read once for initial heap setup"
    );

    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 3);
    assert_eq!(
        data[0].read_count(),
        1,
        "child0 should NOT be re-read (reuse optimization)"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "child1 was at doc 2, should NOT be re-read (quick mode returns first match)"
    );

    let result = union.read().expect("read failed");
    assert!(result.is_none());
    assert_eq!(
        data[0].read_count(),
        2,
        "child0 should now be read to discover EOF"
    );
    assert_eq!(
        data[1].read_count(),
        2,
        "child1 should now be read to discover EOF"
    );
}

#[test]
#[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
fn reuse_results_optimization_full_mode() {
    let (children, data) = create_mock_3([1, 3, 5], [2, 3, 6], [3, 4, 7]);
    let mut union = UnionFullFlat::new(children);

    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 1);
    assert_eq!(
        data[0].read_count(),
        1,
        "child0 should be read once for initial heap setup"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "child1 should be read once for initial heap setup"
    );
    assert_eq!(
        data[2].read_count(),
        1,
        "child2 should be read once for initial heap setup"
    );

    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 2);
    assert_eq!(
        data[0].read_count(),
        2,
        "child0 was at doc 1, should be read to advance"
    );
    assert_eq!(
        data[1].read_count(),
        1,
        "child1 at doc 2 - reused (no read needed)"
    );
    assert_eq!(
        data[2].read_count(),
        1,
        "child2 at doc 3 - reused (no read needed)"
    );

    let result = union
        .read()
        .expect("read failed")
        .expect("should have result");
    assert_eq!(result.doc_id, 3);
    assert_eq!(
        data[0].read_count(),
        2,
        "child0 at doc 3 - reused (no read needed)"
    );
    assert_eq!(
        data[1].read_count(),
        2,
        "child1 was at doc 2, should be read to advance"
    );
    assert_eq!(
        data[2].read_count(),
        1,
        "child2 at doc 3 - reused (no read needed)"
    );
}
