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
            IteratorType, RQEIterator, RQEIteratorError, SkipToOutcome, TypeErasedRQEIterator,
        };
        use rqe_iterators_test_utils::{ContractChecker, ResumeOutcomeExt};
        use crate::utils::FieldMaskMock;

        type Union<I> = $UnionFull<'static, I>;

        // =============================================================================
        // Read tests
        // =============================================================================

        /// One past `u16::MAX`, the widest child count a 16-bit capacity can hold.
        const CHILD_COUNT_ABOVE_16_BIT: usize = 65536;

        #[test]
        #[cfg_attr(miri, ignore = "65536 children is too slow under miri")]
        fn child_count_is_not_capped_at_16_bits() {
            let children: Vec<Box<dyn RQEIterator<'static>>> = (0..CHILD_COUNT_ABOVE_16_BIT)
                .map(|_| MockVec::new_boxed(vec![1]))
                .collect();

            let mut union_iter = Union::new(children);
            let result = union_iter
                .read()
                .expect("read failed")
                .expect("should have result");

            assert_eq!(result.doc_id, 1);
            assert_eq!(
                result
                    .as_aggregate()
                    .expect("a union result is an aggregate")
                    .len(),
                CHILD_COUNT_ABOVE_16_BIT,
                "every child matches doc 1, so all of them belong to the aggregate"
            );
        }

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
        fn read(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
            let (children, expected) = create_union_children(num_children, base_result_set);

            let mut union_iter = ContractChecker::new(Union::new(children));
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
        fn skip_to(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
            let (children, expected) = create_union_children(num_children, base_result_set);
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn rewind(#[case] num_children: usize, #[case] base_result_set: &[u64]) {
            let (children, expected) = create_union_children(num_children, base_result_set);
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn rewind_restores_original_order_after_exhaustion() {
            // Child 0: [1]         — exhausts first
            // Child 1: [1, 5]      — exhausts second
            // Child 2: [1, 5, 10]  — exhausts last
            let (children, _data) = create_mock_3([1], [1, 5], [1, 5, 10]);
            let mut union_iter = ContractChecker::new(Union::new(children));

            // Record the pointer (address) of each child before any reads.
            let ptrs_before: Vec<*const dyn RQEIterator<'static>> = (0..3)
                .map(|i| {
                    union_iter.inner().child_at(i).unwrap() as *const dyn RQEIterator<'static>
                })
                .collect();

            while union_iter.read().expect("read failed").is_some() {}
            assert!(union_iter.at_eof());

            // Rewind and verify child_at returns the same child objects.
            union_iter.rewind();
            for i in 0..3 {
                let ptr_after = union_iter.inner().child_at(i).unwrap()
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
            let mut union_iter = ContractChecker::new(Union::new(children));

            assert!(matches!(union_iter.read(), Ok(None)));
            assert!(union_iter.at_eof());
            assert_eq!(union_iter.num_estimated(), 0);
            assert!(matches!(union_iter.skip_to(1), Ok(None)));
            assert!(union_iter.at_eof());
        }

        #[test]
        fn edge_case_single_child() {
            let (child, child_data) = create_mock_1([10, 20, 30, 40, 50]);
            let mut union_iter = ContractChecker::new(Union::new(vec![child]));

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
        fn edge_case_disjoint_children() {
            let (children, data) = create_mock_3([1, 2, 3], [10, 20, 30], [100, 200, 300]);

            let mut union_iter = ContractChecker::new(Union::new(children));
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
        fn edge_case_overlapping_children() {
            let (children, data) = create_mock_3(
                [1, 2, 5, 10, 15, 20],
                [2, 5, 8, 10, 18, 20],
                [3, 5, 10, 12, 20, 25],
            );

            let mut union_iter = ContractChecker::new(Union::new(children));
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
        fn edge_case_skip_to_exact_match() {
            let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn edge_case_skip_to_not_found() {
            let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn edge_case_skip_to_past_eof() {
            let (children, _) = create_mock_2([10, 20, 30], [15, 25, 35]);
            let mut union_iter = ContractChecker::new(Union::new(children));

            assert!(matches!(union_iter.skip_to(100), Ok(None)));
            assert!(union_iter.at_eof());

            union_iter.rewind();
            assert!(!union_iter.at_eof());
            let result = union_iter.read().expect("read failed");
            assert!(result.is_some());
            assert_eq!(result.unwrap().doc_id, 10);
        }

        #[test]
        fn edge_case_interleaved_read_and_skip_to() {
            let (children, _) = create_mock_2(
                [10, 20, 30, 40, 50, 60, 70, 80],
                [15, 25, 35, 45, 55, 65, 75, 85],
            );
            let mut union_iter = ContractChecker::new(Union::new(children));

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
            let union_iter = ContractChecker::new(Union::new(children));

            assert_eq!(union_iter.num_estimated(), 15);
        }

        #[test]
        fn edge_case_empty_children_mixed_with_non_empty() {
            let empty_child: Mock<'static, 0> = Mock::new([]);
            let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);
            let child2: Mock<'static, 3> = Mock::new([15, 25, 35]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(empty_child), Box::new(child1), Box::new(child2)];
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn edge_case_all_children_empty() {
            let empty1: Mock<'static, 0> = Mock::new([]);
            let empty2: Mock<'static, 0> = Mock::new([]);
            let empty3: Mock<'static, 0> = Mock::new([]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(empty1), Box::new(empty2), Box::new(empty3)];
            let mut union_iter = ContractChecker::new(Union::new(children));
            assert!(matches!(union_iter.read(), Ok(None)));
            assert!(union_iter.at_eof());
            assert_eq!(union_iter.num_estimated(), 0);
        }
        #[test]
        fn edge_case_skip_to_child_already_past_target() {
            let (children, _data) = create_mock_2([10, 50, 100], [20, 60, 110]);
            let mut union_iter = ContractChecker::new(Union::new(children));

            union_iter.read().expect("read failed");
            union_iter.read().expect("read failed");

            let outcome = union_iter.skip_to(40).expect("skip_to failed");
            assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
            assert_eq!(union_iter.last_doc_id(), 50);
        }

        #[test]
        fn edge_case_skip_to_exhausts_some_children() {
            let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 100]);
            let mut union_iter = ContractChecker::new(Union::new(children));

            let outcome = union_iter.skip_to(50).expect("skip_to failed");
            assert!(matches!(outcome, Some(SkipToOutcome::NotFound(_))));
            assert_eq!(union_iter.last_doc_id(), 100);

            assert!(matches!(union_iter.read(), Ok(None)));
            assert!(union_iter.at_eof());
        }

        #[test]
        fn edge_case_skip_to_exhausts_all_children() {
            let (children, _data) = create_mock_2([10, 20, 30], [15, 25, 35]);
            let mut union_iter = ContractChecker::new(Union::new(children));

            let outcome = union_iter.skip_to(1000).expect("skip_to failed");
            assert!(outcome.is_none());
            assert!(union_iter.at_eof());
        }

        #[test]
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
        fn edge_case_misbehaving_child_returns_none_during_init() {
            let mock1: Mock<'static, 3> = Mock::new([10, 30, 50]);
            let mock2: Mock<'static, 3> = Mock::new([20, 40, 60]);

            let mut data1 = mock1.data();
            data1.set_force_read_none(true);

            let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
                vec![Box::new(mock1), Box::new(mock2)];

            let mut union_iter = ContractChecker::new(Union::new(children));

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
        // Read ordering across children
        // =============================================================================

        #[test]
        fn read_starts_at_the_lowest_child() {
            let child0: Mock<'static, 5> = Mock::new([10, 20, 30, 40, 50]);
            let child1: Mock<'static, 5> = Mock::new([5, 25, 35, 45, 55]);

            let children: Vec<Box<dyn RQEIterator<'static> + 'static>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union_iter = ContractChecker::new(Union::new(children));

            let result = union_iter.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 5);

            let result = union_iter.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 10);

            assert!(!union_iter.at_eof());

            let result = union_iter.read().expect("read failed").unwrap();
            assert_eq!(result.doc_id, 20);
        }

        // =============================================================================
        // skip_to edge cases (behavioral only, no read_count assertions)
        // =============================================================================

        #[test]
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
        fn child_hits_eof_during_advance_matching_children() {
            // child0 has only doc 10, child1 has doc 10 then more.
            let child0: Mock<'static, 1> = Mock::new([10]);
            let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = ContractChecker::new($UnionFull::new(children));

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
        fn child_hits_eof_during_advance_matching_children_quick() {
            let child0: Mock<'static, 1> = Mock::new([10]);
            let child1: Mock<'static, 3> = Mock::new([10, 20, 30]);

            let children: Vec<Box<dyn RQEIterator<'static>>> =
                vec![Box::new(child0), Box::new(child1)];

            let mut union = ContractChecker::new($UnionQuick::new(children));

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
        fn quick_exit_early_match_in_skip_to() {
            let (children, _data) = create_mock_3([1, 30, 200, 1000], [2, 10, 300, 1000], [3, 20, 100, 1000]);
            let mut union = ContractChecker::new($UnionQuick::new(children));

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
        fn current_after_operations() {
            let (children, _) = create_mock_2([10, 20, 30, 40, 50], [15, 25, 35, 45, 55]);
            let mut union_iter = ContractChecker::new(Union::new(children));

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
        fn reuse_results_optimization_full_mode() {
            let (children, data) = create_mock_3([1, 3, 5], [2, 3, 6], [3, 4, 7]);
            let mut union = ContractChecker::new($UnionFull::new(children));

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
        fn full_mode_field_mask_resets_between_reads() {
            let children: Vec<Box<dyn RQEIterator<'static>>> = vec![
                Box::new(FieldMaskMock::new(vec![10, 20], 0x1)),
                Box::new(FieldMaskMock::new(vec![10, 30], 0x2)),
            ];
            let mut union = ContractChecker::new($UnionFull::new(children));

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
        fn read_full_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        fn read_full_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        fn read_full_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        // -- Quick mode: read propagates timeout --------------------

        #[test]
        fn read_quick_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        fn read_quick_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        #[test]
        fn read_quick_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_read_eventually_times_out(&mut union);
        }

        // -- Full mode: skip_to propagates timeout ------------------

        #[test]
        fn skip_to_full_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        fn skip_to_full_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        fn skip_to_full_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = ContractChecker::new($UnionFull::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        // -- Quick mode: skip_to propagates timeout -----------------

        #[test]
        fn skip_to_quick_propagates_timeout_first_child() {
            let children = make_timeout_children(0);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        fn skip_to_quick_propagates_timeout_mid_child() {
            let children = make_timeout_children(1);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        #[test]
        fn skip_to_quick_propagates_timeout_last_child() {
            let children = make_timeout_children(2);
            let mut union = ContractChecker::new($UnionQuick::new(children));
            assert_skip_to_eventually_times_out(&mut union);
        }

        // =============================================================================
        // into_trimmed
        // =============================================================================

        /// `into_trimmed` on a Full union produces a working `UnionTrimmed` that
        /// yields all children in reverse order when the limit is large enough.
        #[test]
        fn into_trimmed_full_yields_all_children() {
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionFull::new(children);
            let mut trimmed = ContractChecker::new_unordered(union.into_trimmed(usize::MAX, true).unwrap());

            let docs = drain_doc_ids(&mut trimmed);
            assert_eq!(docs, [5, 6, 3, 4, 1, 2]);
        }

        /// `into_trimmed` on a Quick union applies ascending trimming correctly.
        #[test]
        fn into_trimmed_quick_trims_asc() {
            // 3 children with est [2, 2, 2], limit=1.
            // Asc scan from child[1]: child[1].est=2 > 1 → keep=2.
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionQuick::new(children);
            let mut trimmed = ContractChecker::new_unordered(union.into_trimmed(1, true).unwrap());

            assert_eq!(trimmed.inner().num_children_total(), 3, "all children stay alive");
            let docs = drain_doc_ids(&mut trimmed);
            // Active window [0..2), reads in reverse: child[1] then child[0].
            assert_eq!(docs, [3, 4, 1, 2]);
        }

        /// `into_trimmed` on a Quick union applies descending trimming correctly.
        #[test]
        fn into_trimmed_quick_trims_desc() {
            // 3 children with est [2, 2, 2], limit=1.
            // Desc scan from child[1] backward: child[1].est=2 > 1 → skip=1.
            let (children, _data) = create_mock_3([1, 2], [3, 4], [5, 6]);
            let union = $UnionQuick::new(children);
            let mut trimmed = ContractChecker::new_unordered(union.into_trimmed(1, false).unwrap());

            assert_eq!(trimmed.inner().num_children_total(), 3, "all children stay alive");
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
            let union = ContractChecker::new(Union::new(children));

            assert_eq!(union.inner().num_children_total(), 2);
            assert_eq!(
                union.inner().num_children_active(),
                2,
                "all children should be active before any read"
            );
        }

        /// After reading to EOF, `num_children_active` should be 0.
        #[test]
        fn num_children_active_after_eof() {
            let (children, _data) = create_mock_2([1], [2]);
            let mut union = ContractChecker::new(Union::new(children));

            while union.read().expect("read failed").is_some() {}

            assert!(union.at_eof());
            assert_eq!(
                union.inner().num_children_active(),
                0,
                "no children should be active after EOF"
            );
        }

        /// After rewind, `num_children_active` should be restored to the total.
        #[test]
        fn num_children_active_after_rewind() {
            let (children, _data) = create_mock_2([1, 3], [2, 4]);
            let mut union = ContractChecker::new(Union::new(children));

            // Read to EOF.
            while union.read().expect("read failed").is_some() {}
            assert_eq!(union.inner().num_children_active(), 0);

            union.rewind();
            assert_eq!(
                union.inner().num_children_active(),
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
            let union = ContractChecker::new(Union::new(children));
            assert_eq!(union.intersection_sort_weight(false), 1.0);
        }

        /// When `prioritize_union_children` is true, weight equals the total
        /// number of children.
        #[test]
        fn intersection_sort_weight_with_priority() {
            let (children, _data) = create_mock_2([1, 3, 5], [2, 4, 6]);
            let union = ContractChecker::new(Union::new(children));
            assert_eq!(union.intersection_sort_weight(true), 2.0);
        }

        /// Weight is at least 1.0 even with a single child.
        #[test]
        fn intersection_sort_weight_single_child() {
            let (child, _data) = create_mock_1([1, 2, 3]);
            let union = ContractChecker::new(Union::new(vec![child]));
            assert_eq!(union.intersection_sort_weight(true), 1.0);
        }

        // =============================================================================
        // Suspend/resume tests
        // =============================================================================

        /// How a resume came out, errors included.
        #[derive(Debug, PartialEq, Eq)]
        enum Outcome {
            Ok,
            Moved,
            Aborted,
            Failed,
        }

        /// How a resume came out, the document it published (with its number
        /// of entries), and everything read afterwards.
        #[derive(Debug, PartialEq, Eq)]
        struct Resumed {
            outcome: Outcome,
            published: Option<(rqe_core::DocId, usize)>,
            tail: Vec<rqe_core::DocId>,
        }

        impl Resumed {
            fn new(
                outcome: Outcome,
                published: Option<(rqe_core::DocId, usize)>,
                tail: &[rqe_core::DocId],
            ) -> Self {
                Self { outcome, published, tail: tail.to_vec() }
            }

            /// An aborted or failed resume leaves no iterator to read from.
            fn gone(outcome: Outcome) -> Self {
                Self::new(outcome, None, &[])
            }
        }

        /// Suspend and resume `it` as a query does around a released read lock,
        /// then report the outcome, the published result and the rest of the
        /// documents. `what` names the scenario in the failure.
        #[track_caller]
        fn assert_resumes_to<'index>(
            what: &str,
            guard: &index_spec::IndexSpecReadGuard<'index>,
            it: TypeErasedRQEIterator<'index>,
            expected: Resumed,
        ) {
            use rqe_iterators::ResumeOutcome;

            fn published<'index>(
                it: &mut TypeErasedRQEIterator<'index>,
            ) -> Option<(rqe_core::DocId, usize)> {
                it.current().map(|r| {
                    (r.doc_id, r.as_aggregate().map_or(0, |aggregate| aggregate.len()))
                })
            }

            let resumed = match rqe_iterators_test_utils::revalidate_via_resume(it, guard) {
                Ok(ResumeOutcome::Ok(mut it)) => {
                    Resumed { outcome: Outcome::Ok, published: published(&mut it), tail: drain_doc_ids(&mut it) }
                }
                Ok(ResumeOutcome::Moved(mut it)) => {
                    Resumed { outcome: Outcome::Moved, published: published(&mut it), tail: drain_doc_ids(&mut it) }
                }
                Ok(ResumeOutcome::Aborted) => Resumed::gone(Outcome::Aborted),
                Err(_) => Resumed::gone(Outcome::Failed),
            };
            assert_eq!(resumed, expected, "{what}");
        }

        /// Erase `it` so unions of either mode, and children of different
        /// concrete types, can share one type.
        fn erase<'index, I>(it: I) -> TypeErasedRQEIterator<'index>
        where
            I: rqe_iterators::RQEIteratorBoxed<'index> + rqe_iterators::profile_print::ProfilePrint + 'index,
        {
            TypeErasedRQEIterator::new(Box::new(it))
        }

        /// A child error part-way through a skip can leave the union with an
        /// empty aggregate while its children are positioned. Resuming that
        /// state must not abort: under `ON_TIMEOUT RETURN` the cursor would lose
        /// every remaining result.
        #[test]
        fn resume_after_an_interrupted_skip_does_not_abort() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            for quick in [false, true] {
                let child0: Mock<'_, 2> = Mock::new([10, 30]);
                let child1: Mock<'_, 2> = Mock::new([10, 40]);
                let mut child0_data = child0.data();
                let children = vec![erase(child0), erase(child1)];
                let mut union = if quick {
                    erase($UnionQuick::new(children))
                } else {
                    erase($UnionFull::new(children))
                };
                assert_eq!(union.read().expect("read").expect("doc 10").doc_id, 10);

                child0_data.set_error_on_skip_to(Some(MockIteratorError::TimeoutError(None)));
                assert!(union.skip_to(20).is_err(), "the child's error must surface");
                child0_data.set_error_on_skip_to(None);

                let outcome = rqe_iterators_test_utils::revalidate_via_resume(union, &guard)
                    .expect("resume should not fail");
                assert!(
                    !matches!(outcome, rqe_iterators::ResumeOutcome::Aborted),
                    "quick: {quick}",
                );
            }
        }

        /// A child running out of documents is not a reason to leave the
        /// position the union already holds.
        #[test]
        fn resume_after_a_child_ran_out_keeps_reading_the_others() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let child0: Mock<'_, 2> = Mock::new([10, 20]);
            let child1: Mock<'_, 5> = Mock::new([15, 25, 35, 45, 55]);
            let mut union = $UnionFull::new(vec![erase(child0), erase(child1)]);
            for expected in [10, 15, 20] {
                assert_eq!(union.read().expect("read failed").unwrap().doc_id, expected);
            }

            assert_resumes_to(
                "child0 spent on the union's document",
                &guard,
                erase(union),
                Resumed::new(Outcome::Ok, Some((20, 1)), &[25, 35, 45, 55]),
            );
        }

        /// A union keeps going as long as one child survives, whatever the
        /// others report.
        #[test]
        fn resume_mixed_ok_moved_abort() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let child0: Mock<'_, 5> = Mock::new([10, 20, 30, 40, 50]);
            let child1: Mock<'_, 5> = Mock::new([15, 25, 35, 45, 55]);
            let child2: Mock<'_, 5> = Mock::new([12, 22, 32, 42, 52]);
            child1
                .data()
                .set_revalidate_result(MockRevalidateResult::Move);
            child2
                .data()
                .set_revalidate_result(MockRevalidateResult::Abort);
            let mut union = $UnionFull::new(vec![erase(child0), erase(child1), erase(child2)]);
            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 10);

            // child0 still backs 10; child1's move to 25 and child2's abort do
            // not change that.
            assert_resumes_to(
                "ok, moved and aborted children",
                &guard,
                erase(union),
                Resumed::new(Outcome::Ok, Some((10, 1)), &[20, 25, 30, 35, 40, 45, 50, 55]),
            );
        }

        /// A spent union stays spent, whichever mode it runs in.
        #[test]
        fn resume_of_a_spent_union_is_ok() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            for quick in [false, true] {
                let children = vec![
                    erase(Mock::<'_, 2>::new([10, 20])),
                    erase(Mock::<'_, 2>::new([10, 30])),
                ];
                let mut union = if quick {
                    erase($UnionQuick::new(children))
                } else {
                    erase($UnionFull::new(children))
                };
                assert_eq!(drain_doc_ids(&mut union), [10, 20, 30]);
                assert!(union.at_eof());

                assert_resumes_to(
                    &format!("quick: {quick}"),
                    &guard,
                    union,
                    Resumed::new(Outcome::Ok, None, &[]),
                );
            }
        }

        /// A spent union sees every child resume even though it has nothing
        /// left to yield, and an aborting child is dropped for good: its state
        /// is unrecoverable, so a later `rewind` must not read it again.
        #[test]
        fn resume_of_a_spent_union_drops_an_aborting_child() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            for outcome in [MockRevalidateResult::Ok, MockRevalidateResult::Abort] {
                let child0: Mock<'_, 2> = Mock::new([10, 30]);
                let child1: Mock<'_, 2> = Mock::new([20, 40]);
                child1.data().set_revalidate_result(outcome);
                let mut union = $UnionFull::new(vec![erase(child0), erase(child1)]);
                assert_eq!(drain_doc_ids(&mut union), [10, 20, 30, 40], "the union is spent");

                let mut resumed =
                    rqe_iterators_test_utils::revalidate_via_resume(erase(union), &guard)
                        .expect("resume failed")
                        .expect_ok();
                assert!(resumed.at_eof(), "child1 {outcome:?}");
                resumed.rewind();
                let expected: &[u64] = match outcome {
                    MockRevalidateResult::Abort => &[10, 30],
                    _ => &[10, 20, 30, 40],
                };
                assert_eq!(drain_doc_ids(&mut resumed), expected, "child1 {outcome:?}");
            }
        }

        /// A spent union still has to resume every child, and a resume consumes
        /// the child, so a child that times out cannot be walked past: the error
        /// reaches the caller.
        #[test]
        fn resume_of_a_spent_union_surfaces_a_child_error() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let child0: Mock<'_, 2> = Mock::new([10, 30]);
            let child1: Mock<'_, 2> = Mock::new([20, 40]);
            for data in [child0.data(), child1.data()].iter_mut() {
                data.set_revalidate_result(MockRevalidateResult::TimedOut);
            }
            let mut union = $UnionFull::new(vec![erase(child0), erase(child1)]);
            assert_eq!(drain_doc_ids(&mut union), [10, 20, 30, 40], "the union is spent");

            assert_resumes_to(
                "a spent union whose children time out",
                &guard,
                erase(union),
                Resumed::gone(Outcome::Failed),
            );
        }

        /// A union with no children has nothing to resume.
        #[test]
        fn resume_of_a_childless_union_is_ok() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            assert_resumes_to(
                "full",
                &guard,
                erase($UnionFull::<TypeErasedRQEIterator<'_>>::new(Vec::new())),
                Resumed::new(Outcome::Ok, None, &[]),
            );
            assert_resumes_to(
                "quick",
                &guard,
                erase($UnionQuick::<TypeErasedRQEIterator<'_>>::new(Vec::new())),
                Resumed::new(Outcome::Ok, None, &[]),
            );
        }

        /// Every child outcome at every position. The position matters because
        /// the abort/error teardown splits the child list at the failing index.
        #[test]
        fn resume_handles_every_child_outcome_at_every_position() {
            const ALL: [u64; 9] = [20, 30, 35, 40, 50, 60, 70, 80, 90];
            let docs: [[u64; 4]; 3] = [[10, 20, 40, 70], [10, 30, 50, 80], [10, 35, 60, 90]];
            for position in 0..3 {
                let without_child = |docs: &[u64; 4]| -> Vec<u64> {
                    ALL.iter().copied().filter(|id| !docs.contains(id)).collect()
                };
                for (outcome, expected) in [
                    (
                        MockRevalidateResult::Ok,
                        Resumed::new(Outcome::Ok, Some((10, 3)), &ALL),
                    ),
                    // The moving child leaves 10 for its next document, which is
                    // still owed; the other two keep backing 10.
                    (
                        MockRevalidateResult::Move,
                        Resumed::new(Outcome::Ok, Some((10, 2)), &ALL),
                    ),
                    (
                        MockRevalidateResult::Abort,
                        Resumed::new(
                            Outcome::Ok,
                            Some((10, 2)),
                            &without_child(&docs[position]),
                        ),
                    ),
                    (MockRevalidateResult::TimedOut, Resumed::gone(Outcome::Failed)),
                ] {
                    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
                    let guard = mock_ctx.spec_read();
                    let children: Vec<Mock<'_, 4>> = docs.iter().map(|d| Mock::new(*d)).collect();
                    children[position].data().set_revalidate_result(outcome);
                    let mut union = $UnionFull::new(children.into_iter().map(erase).collect());
                    assert_eq!(union.read().expect("read failed").expect("doc").doc_id, 10);

                    assert_resumes_to(
                        &format!("child {position} reports {outcome:?}"),
                        &guard,
                        erase(union),
                        expected,
                    );
                }
            }
        }

        /// A child sitting on its last document still owes it, while one that
        /// moves past its end has nothing left: the union keeps the former and
        /// drops the latter.
        #[test]
        fn resume_with_children_moving_to_their_end() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();

            // child0 moves off the union's document onto 20, behind child1's 25.
            let mock1: Mock<'_, 3> = Mock::new([10, 20, 30]);
            let mock2: Mock<'_, 3> = Mock::new([5, 25, 50]);
            mock1
                .data()
                .set_revalidate_result(MockRevalidateResult::Move);
            let mut union = $UnionFull::new(vec![erase(mock1), erase(mock2)]);
            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 5);
            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 10);
            assert_resumes_to(
                "a child moving off the union's document",
                &guard,
                erase(union),
                Resumed::new(Outcome::Moved, Some((20, 1)), &[25, 30, 50]),
            );

            // Every child moves past its last document.
            let mock1: Mock<'_, 2> = Mock::new([10, 20]);
            let mock2: Mock<'_, 2> = Mock::new([10, 25]);
            let (mut data1, mut data2) = (mock1.data(), mock2.data());
            let mut union = $UnionFull::new(vec![erase(mock1), erase(mock2)]);
            union.read().expect("read failed").unwrap();
            union.read().expect("read failed").unwrap();
            data1.set_revalidate_result(MockRevalidateResult::Move);
            data2.set_revalidate_result(MockRevalidateResult::Move);
            assert_resumes_to(
                "every child moving past its end",
                &guard,
                erase(union),
                Resumed::new(Outcome::Moved, None, &[]),
            );

            // Each child moves onto its last document. The rebuild must not
            // mistake them for spent, or 30 and 35 are lost.
            let mock1: Mock<'_, 3> = Mock::new([10, 20, 30]);
            let mock2: Mock<'_, 3> = Mock::new([10, 25, 35]);
            let (mut data1, mut data2) = (mock1.data(), mock2.data());
            let mut union = $UnionFull::new(vec![erase(mock1), erase(mock2)]);
            union.read().expect("read failed").unwrap();
            union.read().expect("read failed").unwrap();
            data1.set_revalidate_result(MockRevalidateResult::Move);
            data2.set_revalidate_result(MockRevalidateResult::Move);
            assert_resumes_to(
                "every child moving onto its last document",
                &guard,
                erase(union),
                Resumed::new(Outcome::Moved, Some((30, 1)), &[35]),
            );
        }

        /// A quick union whose children are all positioned adopts a minimum
        /// that lies ahead, and carries on from there without re-delivering it.
        #[test]
        fn quick_resume_adopts_a_minimum_ahead() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let mock1: Mock<'_, 3> = Mock::new([10, 30, 50]);
            let mock2: Mock<'_, 3> = Mock::new([20, 40, 60]);
            let mut data1 = mock1.data();
            let mut quick = $UnionQuick::new(vec![erase(mock1), erase(mock2)]);
            assert_eq!(quick.read().expect("read failed").unwrap().doc_id, 10);

            // The read probed doc 1, an exact match for neither child, so both
            // children were seeked — no lagging sibling.
            assert_eq!(quick.child_at(0).expect("child0").last_doc_id(), 10);
            assert_eq!(quick.child_at(1).expect("child1").last_doc_id(), 20);

            // child0 moves 10 -> 30 while child1 stays at 20: the minimum is ahead.
            data1.set_revalidate_result(MockRevalidateResult::Move);
            assert_resumes_to(
                "child0 moving past child1",
                &guard,
                erase(quick),
                Resumed::new(Outcome::Moved, Some((20, 1)), &[30, 40, 50, 60]),
            );
        }

        /// Children positioned on the union's document when another one moves
        /// off it still owe their remaining documents.
        #[test]
        fn resume_keeps_children_at_current_position() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let child0: Mock<'_, 3> = Mock::new([10, 20, 30]);
            let child1: Mock<'_, 3> = Mock::new([10, 25, 35]);
            let mut data0 = child0.data();
            let mut union = $UnionFull::new(vec![erase(child0), erase(child1)]);
            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 10);

            data0.set_revalidate_result(MockRevalidateResult::Move);
            assert_resumes_to(
                "child0 moving off the shared document",
                &guard,
                erase(union),
                Resumed::new(Outcome::Ok, Some((10, 1)), &[20, 25, 30, 35]),
            );
        }

        /// Children whose union stays on doc 100 when the last one aborts: a
        /// scored child carrying a metric, a sibling on the same document, and a
        /// doomed child ahead of the union.
        fn children_settling_back_onto_100<'index>() -> Vec<TypeErasedRQEIterator<'index>> {
            let scored = rqe_iterators::metric::MetricSortedById::new(vec![100u64], vec![0.5]);
            let sibling: Mock<'index, 2> = Mock::new([100, 200]);
            let doomed: Mock<'index, 2> = Mock::new([150, 300]);
            doomed
                .data()
                .set_revalidate_result(MockRevalidateResult::Abort);
            vec![erase(scored), erase(sibling), erase(doomed)]
        }

        /// A union moves its children's metrics into its aggregate when it builds
        /// it, so the children hold none afterwards. A resume that settles the
        /// union back onto the same document must keep the metrics already
        /// gathered there, or a parent that later publishes this result loses
        /// e.g. a vector distance.
        #[track_caller]
        fn assert_settling_back_keeps_the_metrics<'index>(
            mut union: TypeErasedRQEIterator<'index>,
            guard: &index_spec::IndexSpecReadGuard<'index>,
        ) {
            let first = union.read().expect("read failed").expect("doc 100");
            assert_eq!(first.doc_id, 100);
            assert_eq!(first.metrics.len(), 1, "the aggregate gathered the scored child's metric");

            let mut union =
                rqe_iterators_test_utils::revalidate_via_resume(union, guard)
                    .expect("resume failed")
                    .expect_ok();
            let current = union.current().expect("still on doc 100");
            assert_eq!(current.doc_id, 100);
            assert_eq!(current.metrics.len(), 1, "the metric must survive the settle");
        }

        #[test]
        fn resume_settling_back_onto_the_same_document_keeps_the_metrics() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            assert_settling_back_keeps_the_metrics(
                erase($UnionFull::new(children_settling_back_onto_100())),
                &mock_ctx.spec_read(),
            );
        }

        #[test]
        fn quick_resume_settling_back_onto_the_same_document_keeps_the_metrics() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            assert_settling_back_keeps_the_metrics(
                erase($UnionQuick::new(children_settling_back_onto_100())),
                &mock_ctx.spec_read(),
            );
        }

        /// A quick union reports one child's metrics. Here two scored children sit
        /// on doc 100 and the first read picks one of them; the aborted child's
        /// removal can reorder the survivors so that the re-settle picks the
        /// other, whose metric is still its own. The union must keep publishing
        /// the metric it gathered, not both.
        #[test]
        fn quick_resume_settling_back_onto_another_scored_backer_keeps_one_childs_metrics() {
            let doomed: Mock<'_, 2> = Mock::new([150, 300]);
            doomed
                .data()
                .set_revalidate_result(MockRevalidateResult::Abort);
            let first_scored =
                rqe_iterators::metric::MetricSortedById::new(vec![100u64], vec![0.5]);
            let second_scored =
                rqe_iterators::metric::MetricSortedById::new(vec![100u64], vec![0.25]);
            let mut union = $UnionQuick::new(vec![
                erase(doomed),
                erase(first_scored),
                erase(second_scored),
            ]);

            let first = union.read().expect("read failed").expect("doc 100");
            assert_eq!(first.doc_id, 100);
            assert_eq!(first.metrics.len(), 1);
            let gathered = first.metrics.get(0).expect("one metric").value();

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let mut union =
                rqe_iterators_test_utils::revalidate_via_resume(erase(union), &mock_ctx.spec_read())
                    .expect("resume failed")
                    .expect_ok();
            let current = union.current().expect("still on doc 100");
            assert_eq!(current.doc_id, 100);
            assert_eq!(current.metrics.len(), 1, "a quick union publishes a single child's metrics");
            assert_eq!(current.metrics.get(0).expect("one metric").value(), gathered);
        }

        /// The metrics a quick union gathered survive the settle that seeks a
        /// lagging sibling back onto the union's document, not only the one that
        /// finds a child already sitting there.
        ///
        /// The union delivers doc 20 from child0 while child1 is stranded on 15,
        /// then child0 moves off 20; child1 is seeked onto it and backs it from
        /// there. The metric is placed on the union's result directly: the mock
        /// children carry none, and what the settle sets aside is the union's own.
        #[test]
        fn quick_resume_seeking_a_lagger_back_onto_the_document_keeps_the_metrics() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let child0: Mock<'_, 3> = Mock::new([10, 20, 40]);
            child0
                .data()
                .set_revalidate_result(MockRevalidateResult::Move);
            let mut union = $UnionQuick::new(vec![
                erase(child0),
                erase(Mock::<'_, 3>::new([15, 20, 25])),
            ]);
            assert_eq!(union.read().unwrap().unwrap().doc_id, 10);
            assert!(union.skip_to(20).expect("skip_to failed").is_some());
            assert_eq!(
                union.child_at(1).expect("child1 is still there").last_doc_id(),
                15,
                "child1 must be stranded behind the union for the settle to seek it",
            );
            union.current().expect("on doc 20").metrics.push_without_key(0.5);

            let mut resumed =
                rqe_iterators_test_utils::revalidate_via_resume(erase(union), &mock_ctx.spec_read())
                    .expect("resume failed")
                    .expect_ok();
            let current = resumed.current().expect("still on doc 20");
            assert_eq!(current.doc_id, 20);
            assert_eq!(current.metrics.len(), 1, "the metric must survive the settle");
            assert_eq!(current.metrics.get(0).expect("one metric").value(), 0.5);
        }

        /// The child that supplied a quick union's document has moved off it,
        /// and the sibling stranded behind the union does not hold it either,
        /// so staying put would publish a result no child backs. The union
        /// advances to its next document and reports the move. Contrast
        /// [`resume_advance_keeps_a_document_a_lagging_child_still_matches`].
        #[test]
        fn quick_resume_advances_when_nothing_backs_the_current_position() {
            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let guard = mock_ctx.spec_read();
            let child0: Mock<'_, 3> = Mock::new([10, 20, 40]);
            let child1: Mock<'_, 2> = Mock::new([15, 25]);
            let mut data0 = child0.data();
            let mut union = $UnionQuick::new(vec![erase(child0), erase(child1)]);

            // Positions both children: child0 on 10, child1 on 15.
            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 10);
            // Probing a document child0 holds exactly strands child1 behind the union.
            assert!(matches!(
                union.skip_to(20).expect("skip_to failed"),
                Some(SkipToOutcome::Found(_))
            ));
            assert_eq!(
                union.child_at(1).expect("child1 is still there").last_doc_id(),
                15,
                "the early return left child1 behind the union",
            );

            // child0 supplied doc 20 and now moves off it, so nothing is left on 20.
            data0.set_revalidate_result(MockRevalidateResult::Move);

            // child1's 25 is the next document: child0 has gone to 40, and child1
            // is seeked past the abandoned position on the way.
            assert_resumes_to(
                "the backer moving off a stranded sibling's union",
                &guard,
                erase(union),
                Resumed::new(Outcome::Moved, Some((25, 1)), &[40]),
            );
        }

        /// Advancing past the abandoned position is only free if nothing else
        /// matches it. A `QUICK_EXIT` union is where a `NOT` puts its child, and a
        /// `NOT` does not consume its child's position — it treats it as a
        /// lookahead boundary it has not reached yet. So a document the union
        /// walks past is one the `NOT` has still to exclude.
        ///
        /// Here doc 20 is held by both children. The union delivers it from
        /// child0 while child1 is left behind on 15, then child0 moves off 20.
        /// Nothing backs 20 any more, but child1 still matches it, so the union
        /// may not step over it without first asking child1.
        #[test]
        fn resume_advance_keeps_a_document_a_lagging_child_still_matches() {
            use rqe_iterators::{not::Not, utils::NoTimeoutChecker};

            let child0: Mock<'_, 3> = Mock::new([10, 20, 40]);
            // Unlike [`quick_resume_advances_when_nothing_backs_the_current_position`],
            // the lagging sibling holds the abandoned document itself.
            let child1: Mock<'_, 3> = Mock::new([15, 20, 25]);
            let mut data0 = child0.data();
            let mut union = $UnionQuick::new(vec![erase(child0), erase(child1)]);

            assert_eq!(union.read().expect("read failed").unwrap().doc_id, 10);
            assert!(matches!(
                union.skip_to(20).expect("skip_to failed"),
                Some(SkipToOutcome::Found(_))
            ));
            assert_eq!(
                union.child_at(1).expect("child1 is still there").last_doc_id(),
                15,
                "the early return left child1 behind the union, short of its own 20",
            );

            // The union is where a NOT keeps its child: ahead of the NOT's own
            // position, marking the next id the NOT must exclude.
            let mut not = Not::new(union, 50, 1.0, NoTimeoutChecker);
            assert!(matches!(
                not.skip_to(12).expect("skip_to failed"),
                Some(SkipToOutcome::Found(_))
            ));
            assert_eq!(not.last_doc_id(), 12);
            assert_eq!(
                not.child().expect("the union is still there").last_doc_id(),
                20,
                "the NOT has not reached its child's position, so doc 20 is still owed",
            );

            // GC drops doc 20 from child0's postings: it moves on to 40, and
            // nothing is left on the union's position — except child1, which has
            // not been asked yet.
            data0.set_revalidate_result(MockRevalidateResult::Move);

            let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
            let mut not =
                rqe_iterators_test_utils::revalidate_via_resume(erase(not), &mock_ctx.spec_read())
                    .expect("resume failed")
                    .expect_ok();
            let remaining = drain_doc_ids(&mut not);
            assert!(
                !remaining.contains(&20),
                "doc 20 is still matched by child1, so the NOT must exclude it. Got: {remaining:?}",
            );
        }

    };
}
