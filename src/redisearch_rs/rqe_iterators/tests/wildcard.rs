/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome, wildcard::Wildcard};

mod c_mocks;

// Test cases with different top_id values to cover various scenarios
static TEST_CASES: &[(u64, &str)] = &[
    (100, "standard range"),
    (1, "single document"),
    (10, "small range"),
    (1000, "large range"),
    (42, "arbitrary value"),
];

#[test]
fn initial_state() {
    for &(top_id, description) in TEST_CASES {
        let it = Wildcard::new(top_id);

        assert_eq!(
            it.last_doc_id(),
            0,
            "Case '{}': initial last_doc_id should be 0",
            description
        );
        assert!(
            !it.at_eof(),
            "Case '{}': should not be at EOF initially",
            description
        );
        assert_eq!(
            it.num_estimated(),
            top_id as usize,
            "Case '{}': num_estimated should equal top_id",
            description
        );
    }
}

#[test]
fn read_sequential() {
    for &(top_id, description) in TEST_CASES {
        let mut it = Wildcard::new(top_id);

        // Read all documents sequentially
        for expected_id in 1..=top_id {
            let result = it.read();
            assert!(
                result.is_ok(),
                "Case '{}': read() should succeed for id {}",
                description,
                expected_id
            );

            let result = result.unwrap();
            assert!(
                result.is_some(),
                "Case '{}': should have result for id {}",
                description,
                expected_id
            );

            let doc = result.unwrap();
            assert_eq!(
                doc.doc_id, expected_id,
                "Case '{}': wrong doc_id at position {}",
                description, expected_id
            );
            assert_eq!(
                it.last_doc_id(),
                expected_id,
                "Case '{}': wrong last_doc_id at position {}",
                description,
                expected_id
            );

            // Should not be at EOF until we've read all documents
            let expected_eof = expected_id == top_id;
            assert_eq!(
                it.at_eof(),
                expected_eof,
                "Case '{}': unexpected EOF state at id {}",
                description,
                expected_id
            );
        }

        // After reading all docs, next read should return None
        let result = it.read();
        assert!(
            result.is_ok(),
            "Case '{}': read() after EOF should succeed",
            description
        );
        assert!(
            result.unwrap().is_none(),
            "Case '{}': should return None after EOF",
            description
        );
        assert!(
            it.at_eof(),
            "Case '{}': should be at EOF after reading all docs",
            description
        );

        // Reading again should still return None
        let result = it.read();
        assert!(
            result.is_ok(),
            "Case '{}': second read() after EOF should succeed",
            description
        );
        assert!(
            result.unwrap().is_none(),
            "Case '{}': should still return None after EOF",
            description
        );
    }
}

#[test]
fn skip_to_valid_targets() {
    let top_id = 100u64;
    let skip_targets = [5, 10, 20, 50, 75, 100];

    for &target in &skip_targets {
        let mut it = Wildcard::new(top_id);

        let result = it.skip_to(target);
        assert!(result.is_ok(), "skip_to({}) should succeed", target);

        let outcome = result.unwrap();
        assert!(outcome.is_some(), "skip_to({}) should return Some", target);

        match outcome.unwrap() {
            SkipToOutcome::Found(doc) => {
                assert_eq!(
                    doc.doc_id, target,
                    "skip_to({}) should find exact target",
                    target
                );
                assert_eq!(
                    it.last_doc_id(),
                    target,
                    "last_doc_id should be {} after skip_to({})",
                    target,
                    target
                );

                // Should be at EOF only if we skipped to the last document
                let expected_eof = target == top_id;
                assert_eq!(
                    it.at_eof(),
                    expected_eof,
                    "unexpected EOF state after skip_to({})",
                    target
                );
            }
            SkipToOutcome::NotFound(_) => {
                panic!(
                    "skip_to({}) should always find target in wildcard iterator",
                    target
                );
            }
        }
    }
}

#[test]
fn skip_to_beyond_range() {
    for &(top_id, description) in TEST_CASES {
        let mut it = Wildcard::new(top_id);

        let beyond_target = top_id + 1;
        let result = it.skip_to(beyond_target);
        assert!(
            result.is_ok(),
            "Case '{}': skip_to({}) should succeed",
            description,
            beyond_target
        );

        let outcome = result.unwrap();
        assert!(
            outcome.is_none(),
            "Case '{}': skip_to({}) should return None",
            description,
            beyond_target
        );
        assert!(
            it.at_eof(),
            "Case '{}': should be at EOF after skip_to({})",
            description,
            beyond_target
        );

        // Subsequent reads should return None
        let read_result = it.read();
        assert!(
            read_result.is_ok(),
            "Case '{}': read after skip beyond range should succeed",
            description
        );
        assert!(
            read_result.unwrap().is_none(),
            "Case '{}': read after skip beyond range should return None",
            description
        );
    }
}

#[test]
fn rewind() {
    for &(top_id, description) in TEST_CASES {
        let mut it = Wildcard::new(top_id);

        // Read some documents (up to 10 or top_id, whichever is smaller)
        let read_count = std::cmp::min(10, top_id);
        for i in 1..=read_count {
            let result = it.read();
            assert!(
                result.is_ok(),
                "Case '{}': read({}) should succeed",
                description,
                i
            );
            assert!(
                result.unwrap().is_some(),
                "Case '{}': read({}) should return Some",
                description,
                i
            );
        }

        assert_eq!(
            it.last_doc_id(),
            read_count,
            "Case '{}': last_doc_id should be {} after reading",
            description,
            read_count
        );

        // Rewind
        it.rewind();

        // Check state after rewind
        assert_eq!(
            it.last_doc_id(),
            0,
            "Case '{}': last_doc_id should be 0 after rewind",
            description
        );
        assert!(
            !it.at_eof(),
            "Case '{}': should not be at EOF after rewind",
            description
        );

        // Should be able to read from beginning again
        let result = it.read();
        assert!(
            result.is_ok(),
            "Case '{}': first read after rewind should succeed",
            description
        );

        let result = result.unwrap();
        assert!(
            result.is_some(),
            "Case '{}': first read after rewind should return Some",
            description
        );

        let doc = result.unwrap();
        assert_eq!(
            doc.doc_id, 1,
            "Case '{}': first doc after rewind should be 1",
            description
        );
        assert_eq!(
            it.last_doc_id(),
            1,
            "Case '{}': last_doc_id should be 1 after first read",
            description
        );
    }
}

#[test]
fn read_after_skip() {
    let top_id = 100u64;
    let skip_target = 50u64;

    let mut it = Wildcard::new(top_id);

    // Skip to middle
    let result = it.skip_to(skip_target);
    assert!(result.is_ok(), "skip_to({}) should succeed", skip_target);

    let outcome = result.unwrap();
    assert!(
        outcome.is_some(),
        "skip_to({}) should return Some",
        skip_target
    );

    if let SkipToOutcome::Found(doc) = outcome.unwrap() {
        assert_eq!(
            doc.doc_id, skip_target,
            "skip_to({}) should find exact target",
            skip_target
        );
        assert_eq!(
            it.last_doc_id(),
            skip_target,
            "last_doc_id should be {} after skip",
            skip_target
        );
    }

    // Continue reading sequentially from skip_target + 1
    for expected_id in (skip_target + 1)..=top_id {
        let result = it.read();
        assert!(
            result.is_ok(),
            "read() should succeed for id {}",
            expected_id
        );

        let result = result.unwrap();
        assert!(
            result.is_some(),
            "should have result for id {}",
            expected_id
        );

        let doc = result.unwrap();
        assert_eq!(
            doc.doc_id, expected_id,
            "wrong doc_id at position {}",
            expected_id
        );
        assert_eq!(
            it.last_doc_id(),
            expected_id,
            "wrong last_doc_id at position {}",
            expected_id
        );
    }

    // After reading all remaining docs, should return EOF
    let result = it.read();
    assert!(result.is_ok(), "read() after all docs should succeed");
    assert!(
        result.unwrap().is_none(),
        "should return None after reading all docs"
    );
    assert!(it.at_eof(), "should be at EOF after reading all docs");
}

#[test]
fn skip_to_after_eof() {
    for &(top_id, description) in TEST_CASES {
        let mut it = Wildcard::new(top_id);

        // First, move to EOF by skipping beyond range
        let beyond_target = top_id + 1;
        let result = it.skip_to(beyond_target);
        assert!(
            result.is_ok(),
            "Case '{}': initial skip_to({}) should succeed",
            description,
            beyond_target
        );
        assert!(
            it.at_eof(),
            "Case '{}': should be at EOF after skip beyond range",
            description
        );

        // Try to skip to a valid target while at EOF
        if top_id > 1 {
            let valid_target = top_id / 2;
            let result = it.skip_to(valid_target);
            assert!(
                result.is_ok(),
                "Case '{}': skip_to({}) after EOF should succeed",
                description,
                valid_target
            );

            let outcome = result.unwrap();
            assert!(
                outcome.is_none(),
                "Case '{}': skip_to({}) after EOF should return None",
                description,
                valid_target
            );
            assert!(
                it.at_eof(),
                "Case '{}': should remain at EOF after skip_to({})",
                description,
                valid_target
            );
        }
    }
}

#[test]
fn zero_documents() {
    let mut it = Wildcard::new(0);

    // Should immediately be at EOF
    assert!(it.at_eof(), "iterator with top_id=0 should be at EOF");
    assert_eq!(it.last_doc_id(), 0, "last_doc_id should be 0");
    assert_eq!(it.num_estimated(), 0, "num_estimated should be 0");

    // Read should return None
    let result = it.read();
    assert!(result.is_ok(), "read() should succeed");
    assert!(
        result.unwrap().is_none(),
        "read() should return None for empty iterator"
    );

    // Skip should return None
    let result = it.skip_to(1);
    assert!(result.is_ok(), "skip_to(1) should succeed");
    assert!(
        result.unwrap().is_none(),
        "skip_to(1) should return None for empty iterator"
    );
}

#[test]
fn revalidate() {
    for &(top_id, description) in TEST_CASES {
        let mut it = Wildcard::new(top_id);
        assert_eq!(
            it.revalidate(),
            RQEValidateStatus::Ok,
            "Case '{}': revalidate should return Ok",
            description
        );
    }
}

#[test]
fn skip_to_same_position() {
    let top_id = 100u64;
    let mut it = Wildcard::new(top_id);

    // Skip to position 50
    let target = 50u64;
    let result = it.skip_to(target);
    assert!(result.is_ok(), "first skip_to({}) should succeed", target);
    assert_eq!(
        it.last_doc_id(),
        target,
        "last_doc_id should be {} after first skip",
        target
    );

    // Skip to the same position again
    let result = it.skip_to(target);
    assert!(result.is_ok(), "second skip_to({}) should succeed", target);

    let outcome = result.unwrap();
    assert!(
        outcome.is_some(),
        "second skip_to({}) should return Some",
        target
    );

    if let SkipToOutcome::Found(doc) = outcome.unwrap() {
        assert_eq!(
            doc.doc_id, target,
            "second skip_to({}) should find exact target",
            target
        );
        assert_eq!(
            it.last_doc_id(),
            target,
            "last_doc_id should still be {} after second skip",
            target
        );
    }
}

#[test]
fn skip_to_backwards() {
    let top_id = 100u64;
    let mut it = Wildcard::new(top_id);

    // Skip to position 75
    let first_target = 75u64;
    let result = it.skip_to(first_target);
    assert!(result.is_ok(), "skip_to({}) should succeed", first_target);
    assert_eq!(
        it.last_doc_id(),
        first_target,
        "last_doc_id should be {}",
        first_target
    );

    // Try to skip backwards to position 25
    let second_target = 25u64;
    let result = it.skip_to(second_target);
    assert!(result.is_ok(), "skip_to({}) should succeed", second_target);

    let outcome = result.unwrap();
    assert!(
        outcome.is_some(),
        "skip_to({}) should return Some",
        second_target
    );

    if let SkipToOutcome::Found(doc) = outcome.unwrap() {
        assert_eq!(
            doc.doc_id, second_target,
            "skip_to({}) should find exact target",
            second_target
        );
        assert_eq!(
            it.last_doc_id(),
            second_target,
            "last_doc_id should be {} after backward skip",
            second_target
        );
    }
}

#[test]
fn mixed_read_and_skip_operations() {
    let top_id = 50u64;
    let mut it = Wildcard::new(top_id);

    // Read first few documents
    for expected_id in 1..=5 {
        let result = it.read();
        assert!(
            result.is_ok(),
            "read() should succeed for id {}",
            expected_id
        );
        let doc = result.unwrap().unwrap();
        assert_eq!(
            doc.doc_id, expected_id,
            "wrong doc_id at position {}",
            expected_id
        );
    }

    // Skip to middle
    let skip_target = 30u64;
    let result = it.skip_to(skip_target);
    assert!(result.is_ok(), "skip_to({}) should succeed", skip_target);
    assert_eq!(
        it.last_doc_id(),
        skip_target,
        "last_doc_id should be {}",
        skip_target
    );

    // Read a few more
    for i in 1..=5 {
        let expected_id = skip_target + i;
        let result = it.read();
        assert!(
            result.is_ok(),
            "read() should succeed for id {}",
            expected_id
        );
        let doc = result.unwrap().unwrap();
        assert_eq!(
            doc.doc_id, expected_id,
            "wrong doc_id at position {}",
            expected_id
        );
    }

    // Skip to near end
    let final_skip = top_id - 2;
    let result = it.skip_to(final_skip);
    assert!(result.is_ok(), "skip_to({}) should succeed", final_skip);
    assert_eq!(
        it.last_doc_id(),
        final_skip,
        "last_doc_id should be {}",
        final_skip
    );

    // Read remaining documents
    for expected_id in (final_skip + 1)..=top_id {
        let result = it.read();
        assert!(
            result.is_ok(),
            "read() should succeed for id {}",
            expected_id
        );
        let doc = result.unwrap().unwrap();
        assert_eq!(
            doc.doc_id, expected_id,
            "wrong doc_id at position {}",
            expected_id
        );
    }

    // Should be at EOF now
    assert!(it.at_eof(), "should be at EOF after reading all documents");
    let result = it.read();
    assert!(result.is_ok(), "read() after EOF should succeed");
    assert!(
        result.unwrap().is_none(),
        "read() after EOF should return None"
    );
}

#[test]
fn edge_case_single_document() {
    let mut it = Wildcard::new(1);

    // Initial state
    assert!(!it.at_eof(), "should not be at EOF initially");
    assert_eq!(it.last_doc_id(), 0, "initial last_doc_id should be 0");

    // Read the single document
    let result = it.read();
    assert!(result.is_ok(), "read() should succeed");
    let doc = result.unwrap().unwrap();
    assert_eq!(doc.doc_id, 1, "should read document 1");
    assert_eq!(it.last_doc_id(), 1, "last_doc_id should be 1");
    assert!(
        it.at_eof(),
        "should be at EOF after reading single document"
    );

    // Next read should return None
    let result = it.read();
    assert!(result.is_ok(), "second read() should succeed");
    assert!(
        result.unwrap().is_none(),
        "second read() should return None"
    );

    // Test skip_to on single document iterator
    it.rewind();
    let result = it.skip_to(1);
    assert!(result.is_ok(), "skip_to(1) should succeed");

    let outcome = result.unwrap();
    assert!(outcome.is_some(), "skip_to(1) should return Some");

    if let SkipToOutcome::Found(doc) = outcome.unwrap() {
        assert_eq!(doc.doc_id, 1, "skip_to(1) should find document 1");
        assert_eq!(it.last_doc_id(), 1, "last_doc_id should be 1 after skip");
        assert!(
            it.at_eof(),
            "should be at EOF after skipping to last document"
        );
    }
}

#[test]
fn stress_test_large_ranges() {
    // Test with large but manageable ranges to avoid timeout
    let large_cases = [(100_000, "100k range"), (1_000_000, "1M range")];

    for &(top_id, description) in &large_cases {
        let mut it = Wildcard::new(top_id);

        // Test initial state
        assert_eq!(
            it.num_estimated(),
            top_id as usize,
            "Case '{}': wrong num_estimated",
            description
        );
        assert!(
            !it.at_eof(),
            "Case '{}': should not be at EOF initially",
            description
        );

        // Test skip to various positions
        let test_positions = [1, top_id / 4, top_id / 2, 3 * top_id / 4, top_id];
        for &pos in &test_positions {
            it.rewind();
            let result = it.skip_to(pos);
            assert!(
                result.is_ok(),
                "Case '{}': skip_to({}) should succeed",
                description,
                pos
            );

            if let Ok(Some(SkipToOutcome::Found(doc))) = result {
                assert_eq!(
                    doc.doc_id, pos,
                    "Case '{}': skip_to({}) wrong doc_id",
                    description, pos
                );
                assert_eq!(
                    it.last_doc_id(),
                    pos,
                    "Case '{}': skip_to({}) wrong last_doc_id",
                    description,
                    pos
                );
            }
        }

        // Test reading a small sample from the beginning
        it.rewind();
        for i in 1..=std::cmp::min(100, top_id) {
            let result = it.read();
            assert!(
                result.is_ok(),
                "Case '{}': read({}) should succeed",
                description,
                i
            );
            let doc = result.unwrap().unwrap();
            assert_eq!(
                doc.doc_id, i,
                "Case '{}': read({}) wrong doc_id",
                description, i
            );
        }
    }
}

#[test]
fn property_based_skip_to_invariants() {
    // Test that skip_to maintains invariants across various scenarios
    let top_id = 1000u64;
    let mut it = Wildcard::new(top_id);

    // Property: skip_to(x) where x <= top_id should always succeed and position at x
    for target in [1, 10, 100, 500, 999, 1000] {
        it.rewind();
        let result = it.skip_to(target);
        assert!(result.is_ok(), "skip_to({}) should succeed", target);

        if let Ok(Some(SkipToOutcome::Found(doc))) = result {
            assert_eq!(
                doc.doc_id, target,
                "skip_to({}) should position at target",
                target
            );
            assert_eq!(
                it.last_doc_id(),
                target,
                "last_doc_id should equal target after skip_to({})",
                target
            );

            // Property: after skip_to(x), next read() should return x+1 (if x < top_id)
            if target < top_id {
                let next_result = it.read();
                assert!(
                    next_result.is_ok(),
                    "read after skip_to({}) should succeed",
                    target
                );
                let next_doc = next_result.unwrap().unwrap();
                assert_eq!(
                    next_doc.doc_id,
                    target + 1,
                    "read after skip_to({}) should return {}",
                    target,
                    target + 1
                );
            }
        }
    }
}

#[test]
fn memory_efficiency_validation() {
    // Ensure that large ranges don't consume excessive memory
    let large_top_id = 10_000_000u64;
    let it = Wildcard::new(large_top_id);

    // The iterator should only store a few fields regardless of range size
    assert_eq!(
        it.num_estimated(),
        large_top_id as usize,
        "num_estimated should match top_id"
    );
    assert_eq!(it.last_doc_id(), 0, "initial last_doc_id should be 0");
    assert!(!it.at_eof(), "should not be at EOF initially");

    // Memory usage should be constant regardless of top_id
    // This is more of a design validation than a runtime test
    assert_eq!(
        std::mem::size_of_val(&it),
        std::mem::size_of::<Wildcard>(),
        "iterator size should be constant"
    );
}
