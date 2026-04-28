/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared test macro for union iterator variants (Flat and Heap).
//!
//! This macro generates the common test suite parameterized by the union type,
//! eliminating duplication between `union_flat.rs` and `union_heap.rs`.

/// Generates the common union iterator test suite for a given Full/Quick type pair.
///
/// Usage:
/// ```ignore
/// union_common_tests!(UnionFullFlat, UnionQuickFlat);
/// ```
macro_rules! union_common_tests {
    ($UnionFull:ident, $UnionQuick:ident) => {
        use crate::utils::{
            Mock, MockIteratorError, MockRevalidateResult, MockVec, create_mock_1, create_mock_2,
            create_mock_3, create_union_children, drain_doc_ids,
        };
        use rqe_iterators::{
            IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
        };
        use crate::utils::FieldMaskMock;

        type Union<I> = $UnionFull<'static, I>;

        // =============================================================================
        // Read tests
        // =============================================================================

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
                    None => {
                        panic!("num_children={num_children}, skip_to({id}) should not return None")
                    }
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

        // =============================================================================
        // Rewind tests
        // =============================================================================

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

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn rewind_restores_original_order_after_exhaustion() {
            // Child 0: [1]         — exhausts first
            // Child 1: [1, 5]      — exhausts second
            // Child 2: [1, 5, 10]  — exhausts last
            let (children, _data) = create_mock_3([1], [1, 5], [1, 5, 10]);
            let mut union_iter = Union::new(children);

            // Record the pointer (address) of each child before any reads.
            let ptrs_before: Vec<*const dyn RQEIterator<'static>> = (0..3)
                .map(|i| {
                    union_iter.child_at(i).unwrap() as *const dyn RQEIterator<'static>
                })
                .collect();

            while union_iter.read().expect("read failed").is_some() {}
            assert!(union_iter.at_eof());

            // Rewind and verify child_at returns the same child objects.
            union_iter.rewind();
            for i in 0..3 {
                let ptr_after = union_iter.child_at(i).unwrap()
                    as *const dyn RQEIterator<'static>;
                assert!(
                    std::ptr::addr_eq(ptrs_before[i], ptr_after),
                    "child_at({i}) should return the same child after rewind"
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
            let (children, _data) = create_mock_2([10, 50, 100], [20, 60, 110]);
            let mut union_iter = Union::new(children);

            union_iter.read().expect("read failed");
            union_iter.read().expect("read failed");

            let outcome = union_iter.skip_to(40).expect("skip_to failed");
            assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
            assert_eq!(union_iter.last_doc_id(), 50);
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
            let mut quick_iter = $UnionQuick::new(children);

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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mut quick_iter = $UnionQuick::new(children);

            let mut read_docs = Vec::new();
            while let Some(result) = quick_iter.read().expect("read failed") {
                read_docs.push(result.doc_id);
            }
            assert_eq!(read_docs, vec![10, 20, 30]);
            assert!(quick_iter.at_eof());
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { quick_iter.revalidate(ctx) }.expect("revalidate failed");
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

                let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
                let ctx = mock_ctx.spec();
                // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

                let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
                let ctx = mock_ctx.spec();
                // SAFETY: test-only call with valid context
            let status = unsafe { union_iter.revalidate(ctx) }.expect("revalidate failed");
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

            let mut quick_iter = $UnionQuick::new(children);

            let result = quick_iter.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            data1.set_revalidate_result(MockRevalidateResult::Move);
            data2.set_revalidate_result(MockRevalidateResult::Ok);

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { quick_iter.revalidate(ctx) }.expect("revalidate failed");
            assert!(matches!(
                status,
                RQEValidateStatus::Moved { current: Some(_) }
            ));
            assert_eq!(quick_iter.last_doc_id(), 20);
        }

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn revalidate_keeps_children_at_current_position() {
            let child0: Mock<'static, 3> = Mock::new([10, 20, 30]);
            let child1: Mock<'static, 3> = Mock::new([10, 25, 35]);

            let mut data0 = child0.data();
            let mut data1 = child1.data();

            data0.set_revalidate_result(MockRevalidateResult::Ok);
            data1.set_revalidate_result(MockRevalidateResult::Ok);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];
            let mut union = $UnionFull::new(children);

            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            data0.set_revalidate_result(MockRevalidateResult::Move);

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let _status = unsafe { union.revalidate(ctx) }.expect("revalidate failed");

            let mut remaining = Vec::new();
            while let Some(result) = union.read().expect("read failed") {
                remaining.push(result.doc_id);
            }

            assert!(
                remaining.contains(&25),
                "Doc 25 from child1 should not be lost after revalidation. Got: {remaining:?}"
            );
            assert!(
                remaining.contains(&35),
                "Doc 35 from child1 should not be lost after revalidation. Got: {remaining:?}"
            );
        }

        /// After `read()` returns doc_id 10, revalidate all children with `Ok`.
        /// Because the minimum hasn't moved, the union should return `Ok`.
        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn revalidate_minimum_unchanged_returns_ok() {
            let child0: Mock<'static, 3> = Mock::new([10, 30, 50]);
            let child1: Mock<'static, 3> = Mock::new([10, 40, 60]);

            child0
                .data()
                .set_revalidate_result(MockRevalidateResult::Ok);
            child1
                .data()
                .set_revalidate_result(MockRevalidateResult::Ok);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = $UnionFull::new(children);

            // Both children share doc_id 10.
            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            // Revalidate — nothing moved, nothing aborted.
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union.revalidate(ctx) }.expect("revalidate failed");
            assert!(
                matches!(status, RQEValidateStatus::Ok),
                "Expected Ok when minimum doc_id is unchanged, got {:?}",
                status
            );

            // Union should still be able to continue reading.
            let result = union.read().expect("read failed").unwrap();
            assert!(
                result.doc_id > 10,
                "Expected next doc_id > 10, got {}",
                result.doc_id
            );
        }

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn revalidate_child_behind_union_position_is_kept() {
            let child0: Mock<'static, 3> = Mock::new([5, 100, 300]);
            let child1: Mock<'static, 3> = Mock::new([10, 50, 200]);

            let mut data1 = child1.data();

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = $UnionQuick::new(children);

            let r = union.read().expect("read failed").unwrap();
            assert_eq!(r.doc_id, 5);

            // skip_to(100): in heap quick mode, advance_lagging_children finds
            // child0 at the root (doc 5 < 100), skips child0 to 100 → Found.
            // QUICK_EXIT returns immediately — child1 stays at doc 10.
            let outcome = union.skip_to(100).expect("skip_to failed");
            assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
            assert_eq!(union.last_doc_id(), 100);

            // State: union at 100.  child0 at 100.  child1 at 10 (never advanced).
            // Trigger Move on child1: mock advances to doc_ids[next_index] = 50.
            //   child1.last_doc_id() = 50  <  100 = union.last_doc_id()
            data1.set_revalidate_result(MockRevalidateResult::Move);

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let ctx = mock_ctx.spec();
            // SAFETY: test-only call with valid context
            let status = unsafe { union.revalidate(ctx) }.expect("revalidate failed");
            assert!(
                matches!(status, RQEValidateStatus::Moved { current: Some(_) }),
                "Expected Moved with a current result, got {status:?}",
            );
            assert_eq!(union.last_doc_id(), 50, "union should move to child1 (50)");

            let r = union.read().expect("read failed").unwrap();
            assert_eq!(r.doc_id, 100);
            let r = union.read().expect("read failed").unwrap();
            assert_eq!(r.doc_id, 200, "child1's doc 200 must not be lost");
            let r = union.read().expect("read failed").unwrap();
            assert_eq!(r.doc_id, 300);
            assert!(union.read().expect("read failed").is_none());
            assert!(union.at_eof());
        }


        // =============================================================================
        // skip_to edge cases (behavioral only, no read_count assertions)
        // =============================================================================

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn skip_to_edge_cases() {
            // Quick mode - child already at target doc_id
            {
                let (children, _data) = create_mock_2([50, 200], [100, 250]);
                let mut quick_iter = $UnionQuick::new(children);
                quick_iter.read().expect("read failed").unwrap();
                assert_eq!(quick_iter.last_doc_id(), 50);
                let outcome = quick_iter.skip_to(100).expect("skip_to failed");
                assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
                assert_eq!(quick_iter.last_doc_id(), 100);
            }

            // Quick mode - child exhausts during skip_to
            {
                let (children, _data) = create_mock_2([10, 30], [20, 200]);
                let mut quick_iter = $UnionQuick::new(children);
                quick_iter.read().expect("read failed").unwrap();
                let outcome = quick_iter.skip_to(100).expect("skip_to failed");
                assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
                assert_eq!(quick_iter.last_doc_id(), 200);
            }

            // Full mode - child at EOF before skip_to
            {
                let (children, _data) = create_mock_2([10, 20], [15, 50, 100]);
                let mut full_iter = $UnionFull::new(children);

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
            }

            // Full mode - child already at target doc_id
            {
                let (children, _data) = create_mock_2([50, 150, 200], [100, 150, 250]);
                let mut full_iter = $UnionFull::new(children);
                full_iter.read().expect("read failed").unwrap();
                assert_eq!(full_iter.last_doc_id(), 50);

                let outcome = full_iter.skip_to(100).expect("skip_to failed");
                assert!(matches!(outcome, Some(SkipToOutcome::Found(_))));
                assert_eq!(full_iter.last_doc_id(), 100);
            }
        }

        /// Two children both start at doc_id 10. After the first `read()`,
        /// the union advances matching children. One of them has only a single
        /// document, so `read()` returns `None` (EOF) during that advancement.
        /// The union should still continue with the remaining child.
        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn child_hits_eof_during_advance_matching_children() {
            // child0 has only doc 10, child1 has doc 10 then more.
            let child0: Mock<'static, 1> = Mock::new([10]);
            let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = $UnionFull::new(children);

            // First read should return 10 (both children match).
            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            // During the first read, child0 is advanced and hits EOF.
            // The union should still return the remaining docs from child1.
            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 20);

            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 30);

            assert!(union.read().expect("read failed").is_none());
            assert!(union.at_eof());
        }

        /// Same as above but in Quick mode — only one matching child is consumed,
        /// so the EOF child should be silently dropped.
        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn child_hits_eof_during_advance_matching_children_quick() {
            let child0: Mock<'static, 1> = Mock::new([10]);
            let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = $UnionQuick::new(children);

            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 20);

            let result = union.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 30);

            assert!(union.read().expect("read failed").is_none());
            assert!(union.at_eof());
        }

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn quick_exit_early_match_in_skip_to() {
            let (children, _data) = create_mock_3([1, 30, 200, 1000], [2, 10, 300, 1000], [3, 20, 100, 1000]);
            let mut union = $UnionQuick::new(children);

            let result = union
                .read()
                .expect("read failed")
                .expect("should have result");
            assert_eq!(result.doc_id, 1);

            let result = union.skip_to(10).expect("skip_to failed").unwrap();
            assert!(
                matches!(result, SkipToOutcome::Found(r) if r.doc_id == 10),
                "expected Found(10) (hit #1)"
            );
            let result = union.skip_to(20).expect("skip_to failed").unwrap();
            assert!(
                matches!(result, SkipToOutcome::Found(r) if r.doc_id == 20),
                "expected Found(20) (hit #2)"
            );
            let result = union.skip_to(100).expect("skip_to failed").unwrap();
            assert!(
                matches!(result, SkipToOutcome::Found(r) if r.doc_id == 100),
                "expected Found(100) (hit #3)"
            );

            let result = union.skip_to(200).expect("skip_to failed").unwrap();
            assert!(
                matches!(result, SkipToOutcome::Found(r) if r.doc_id == 200),
                "expected Found(200) (hit #4)"
            );

            let result = union.skip_to(900).expect("skip_to failed").unwrap();
            assert!(
                matches!(result, SkipToOutcome::NotFound(r) if r.doc_id == 1000),
                "expected NotFound(1000) — all children converge"
            );
            assert!(union.read().expect("read failed").is_none());
            assert!(union.at_eof());
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
            let mut full_iter = $UnionFull::new(full_children);
            let mut full_results = Vec::new();
            while let Some(result) = full_iter.read().expect("read failed") {
                full_results.push(result.doc_id);
            }

            let (quick_children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
            let mut quick_iter = $UnionQuick::new(quick_children);
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
            let mut full_iter = $UnionFull::new(children);

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
            let mut quick_iter = $UnionQuick::new(children);

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
            let mut full_iter = $UnionFull::new(children);

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
            let mut quick_iter = $UnionQuick::new(children);

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
            let mut full_iter = $UnionFull::new(full_children);

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
            let mut quick_iter = $UnionQuick::new(quick_children);

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
        // Reuse results optimization tests (full mode - identical for both variants)
        // =============================================================================

        #[test]
        #[cfg_attr(miri, ignore)] // Calls RSYieldableMetric_Concat FFI in push_borrowed
        fn reuse_results_optimization_full_mode() {
            let (children, data) = create_mock_3([1, 3, 5], [2, 3, 6], [3, 4, 7]);
            let mut union = $UnionFull::new(children);

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

        // =============================================================================
        // Type tests
        // =============================================================================

        #[test]
        fn type_full() {
            let children: Vec<Box<dyn RQEIterator<'static>>> = vec![MockVec::new_boxed(vec![1, 2, 3])];
            let it = $UnionFull::new(children);
            assert_eq!(it.type_(), IteratorType::Union);
        }

        #[test]
        fn type_quick() {
            let children: Vec<Box<dyn RQEIterator<'static>>> = vec![MockVec::new_boxed(vec![1, 2, 3])];
            let it = $UnionQuick::new(children);
            assert_eq!(it.type_(), IteratorType::Union);
        }

        // =============================================================================
        // reset_aggregate tests
        // =============================================================================

        /// Verify that field_mask is reset between reads and doesn't accumulate.
        ///
        /// Without `reset_aggregate`, the aggregate result's field_mask would
        /// be OR'd across reads, leaking bits from previous documents.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn full_mode_field_mask_resets_between_reads() {
            let children: Vec<Box<dyn RQEIterator<'static>>> = vec![
                Box::new(FieldMaskMock::new(vec![10, 20], 0x1)),
                Box::new(FieldMaskMock::new(vec![10, 30], 0x2)),
            ];
            let mut union = $UnionFull::new(children);

            let r = union.read().unwrap().unwrap();
            assert_eq!(r.doc_id, 10);
            assert_eq!(
                r.field_mask, 0x3,
                "doc 10: both children → mask = 0x1 | 0x2 = 0x3"
            );

            let r = union.read().unwrap().unwrap();
            assert_eq!(r.doc_id, 20);
            assert_eq!(
                r.field_mask, 0x1,
                "doc 20: only child0 → mask must be 0x1, not 0x3"
            );

            let r = union.read().unwrap().unwrap();
            assert_eq!(r.doc_id, 30);
            assert_eq!(r.field_mask, 0x2, "doc 30: only child1 → mask must be 0x2");
        }

        // =====================================================================
        // Timeout / error propagation tests
        // =====================================================================

        /// Helper: create 3 mock iterators and set the child at
        /// `timeout_idx` to return a timeout error at EOF.
        fn make_timeout_children(
            timeout_idx: usize,
        ) -> Vec<Box<dyn RQEIterator<'static>>> {
            let mocks = [
                Mock::new([10, 20, 30, 40, 50]),
                Mock::new([10, 20, 30, 40, 50]),
                Mock::new([10, 20, 30, 40, 50]),
            ];
            mocks[timeout_idx]
                .data()
                .set_error_at_done(Some(MockIteratorError::TimeoutError(None)));
            mocks.into_iter().map(|m| Box::new(m) as _).collect()
        }

        /// Read until we get a non-Ok result and assert it is a timeout.
        fn assert_read_eventually_times_out<'a>(
            union: &mut impl RQEIterator<'a>,
        ) {
            loop {
                match union.read() {
                    Ok(Some(_)) => continue,
                    Err(RQEIteratorError::TimedOut) => return,
                    Ok(None) => panic!("expected timeout error, got EOF"),
                    Err(e) => panic!("expected timeout error, got {e:?}"),
                }
            }
        }

        /// Skip forward until we get a non-Ok result and assert it is a timeout.
        fn assert_skip_to_eventually_times_out<'a>(
            union: &mut impl RQEIterator<'a>,
        ) {
            let mut next = 1;
            loop {
                match union.skip_to(next) {
                    Ok(Some(SkipToOutcome::Found(_) | SkipToOutcome::NotFound(_))) => {
                        next = union.last_doc_id() + 1;
                    }
                    Err(RQEIteratorError::TimedOut) => return,
                    Ok(None) => panic!("expected timeout error, got EOF"),
                    Err(e) => panic!("expected timeout error, got {e:?}"),
                }
            }
        }

        // -- Full mode: read propagates timeout ---------------------

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_full_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = $UnionFull::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_full_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = $UnionFull::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_full_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = $UnionFull::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        // -- Quick mode: read propagates timeout --------------------

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_quick_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = $UnionQuick::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_quick_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = $UnionQuick::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn read_quick_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = $UnionQuick::new(children);
            assert_read_eventually_times_out(&mut union);
        }

        // -- Full mode: skip_to propagates timeout ------------------

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_full_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = $UnionFull::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_full_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = $UnionFull::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_full_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = $UnionFull::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        // -- Quick mode: skip_to propagates timeout -----------------

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_quick_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = $UnionQuick::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_quick_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = $UnionQuick::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn skip_to_quick_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = $UnionQuick::new(children);
            assert_skip_to_eventually_times_out(&mut union);
        }

        // =============================================================================
        // into_trimmed
        // =============================================================================

        /// `into_trimmed` on a Full union produces a working `UnionTrimmed` that
        /// yields all children in reverse order when the limit is large enough.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn into_trimmed_full_yields_all_children() {
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionFull::new(children);
            let mut trimmed = union.into_trimmed(usize::MAX, true).unwrap();

            let docs = drain_doc_ids(&mut trimmed);
            assert_eq!(docs, [5, 6, 3, 4, 1, 2]);
        }

        /// `into_trimmed` on a Quick union applies ascending trimming correctly.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn into_trimmed_quick_trims_asc() {
            // 3 children with est [2, 2, 2], limit=1.
            // Asc scan from child[1]: child[1].est=2 > 1 → keep=2.
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionQuick::new(children);
            let mut trimmed = union.into_trimmed(1, true).unwrap();

            assert_eq!(trimmed.num_children_total(), 3, "all children stay alive");
            let docs = drain_doc_ids(&mut trimmed);
            // Active window [0..2), reads in reverse: child[1] then child[0].
            assert_eq!(docs, [3, 4, 1, 2]);
        }

        /// `into_trimmed` on a Quick union applies descending trimming correctly.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn into_trimmed_quick_trims_desc() {
            // 3 children with est [2, 2, 2], limit=1.
            // Desc scan from child[1] backward: child[1].est=2 > 1 → skip=1.
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionQuick::new(children);
            let mut trimmed = union.into_trimmed(1, false).unwrap();

            assert_eq!(trimmed.num_children_total(), 3, "all children stay alive");
            let docs = drain_doc_ids(&mut trimmed);
            // Active window [1..3), reads in reverse: child[2] then child[1].
            assert_eq!(docs, [5, 6, 3, 4]);
        }

        // =============================================================================
        // num_children_total / num_children_active
        // =============================================================================

        /// Before any read, all children should be active.
        #[test]
        fn num_children_active_before_first_read() {
            let (children, _data) = create_mock_2([1, 3, 5], [2, 4, 6]);
            let union = Union::new(children);

            assert_eq!(union.num_children_total(), 2);
            assert_eq!(
                union.num_children_active(),
                2,
                "all children should be active before any read"
            );
        }

        /// After reading to EOF, `num_children_active` should be 0.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn num_children_active_after_eof() {
            let (children, _data) = create_mock_2([1], [2]);
            let mut union = Union::new(children);

            while union.read().expect("read failed").is_some() {}

            assert!(union.at_eof());
            assert_eq!(
                union.num_children_active(),
                0,
                "no children should be active after EOF"
            );
        }

        /// After rewind, `num_children_active` should be restored to the total.
        #[test]
        #[cfg_attr(miri, ignore = "Calls RSYieldableMetric_Concat FFI in push_borrowed")]
        fn num_children_active_after_rewind() {
            let (children, _data) = create_mock_2([1, 3], [2, 4]);
            let mut union = Union::new(children);

            // Read to EOF.
            while union.read().expect("read failed").is_some() {}
            assert_eq!(union.num_children_active(), 0);

            union.rewind();
            assert_eq!(
                union.num_children_active(),
                2,
                "all children should be active after rewind"
            );
        }

        // =============================================================================
        // intersection_sort_weight
        // =============================================================================

        /// When `prioritize_union_children` is false, weight is always 1.0.
        #[test]
        fn intersection_sort_weight_without_priority() {
            let (children, _data) = create_mock_2([1, 3, 5], [2, 4, 6]);
            let union = Union::new(children);
            assert_eq!(union.intersection_sort_weight(false), 1.0);
        }

        /// When `prioritize_union_children` is true, weight equals the total
        /// number of children.
        #[test]
        fn intersection_sort_weight_with_priority() {
            let (children, _data) = create_mock_2([1, 3, 5], [2, 4, 6]);
            let union = Union::new(children);
            assert_eq!(union.intersection_sort_weight(true), 2.0);
        }

        /// Weight is at least 1.0 even with a single child.
        #[test]
        fn intersection_sort_weight_single_child() {
            let (child, _data) = create_mock_1([1, 2, 3]);
            let union = Union::new(vec![child]);
            assert_eq!(union.intersection_sort_weight(true), 1.0);
        }

    };
}
