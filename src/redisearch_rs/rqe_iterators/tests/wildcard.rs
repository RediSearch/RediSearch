/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{RQEIterator, SkipToOutcome, wildcard::Wildcard};

mod c_mocks;

/// Helper macro to assert skip_to result with expected doc_id
/// This preserves the call site location in test failures
macro_rules! assert_skip_to_found {
    ($result:expr, $target_doc_id:expr) => {
        assert!($result.is_ok());

        let outcome = $result.unwrap();
        assert!(outcome.is_some());

        if let Some(SkipToOutcome::Found(doc)) = outcome {
            assert_eq!(doc.doc_id, $target_doc_id);
        } else {
            panic!("Expected Found outcome, got {:?}", outcome);
        }
    };
}

#[test]
fn initial_state() {
    let it = Wildcard::new(10);

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 10);
}

#[test]
fn read_with_post_call_mutate() {
    // small test to ensure such mutations are possible
    // for iterators which wrap Wildcard.
    let mut it = Wildcard::new(2);

    for step in 1..=2 {
        let result = it.read().unwrap().unwrap();
        // iterators do not reset between calls
        // so important to not do something like this test,
        // where you accumulate!!! Instead you want to assign these properties (always)
        // such that they have the value you expect. Here be dragons.
        assert_eq!(result.weight, if step == 1 { 0. } else { 42. });
        result.weight += 42.;
        assert_eq!(result.weight, if step == 1 { 42. } else { 84. });
    }
}

#[test]
fn read_sequential() {
    let mut it = Wildcard::new(5);

    // Read all documents sequentially
    for expected_id in 1..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();
        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);

        // Should not be at EOF until we've read all documents
        let expected_eof = expected_id == 5;
        assert_eq!(it.at_eof(), expected_eof);
    }

    // After reading all docs, next read should return None
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());

    // Reading again should still return None
    let result = it.read().unwrap();
    assert!(result.is_none());
}

#[test]
fn skip_to_valid_targets() {
    let mut it = Wildcard::new(10);

    // Test skipping to middle
    let result = it.skip_to(5);
    assert_skip_to_found!(result, 5);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);
    assert!(!it.at_eof());

    // Test skipping to last document
    let result = it.skip_to(10);
    assert_skip_to_found!(result, 10);
    assert_eq!(it.last_doc_id(), 10);
    assert_eq!(it.current().unwrap().doc_id, 10);
    assert!(it.at_eof());
}

#[test]
fn skip_to_beyond_range() {
    let mut it = Wildcard::new(10);

    let result = it.skip_to(11); // Beyond range

    let outcome = result.unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());

    // Subsequent reads should return None
    let result = it.read().unwrap();
    assert!(result.is_none());
}

#[test]
fn rewind() {
    let mut it = Wildcard::new(10);

    // Read some documents
    for _i in 1..=3 {
        let result = it.read().unwrap();
        assert!(result.is_some());
    }

    assert_eq!(it.last_doc_id(), 3);
    assert_eq!(it.current().unwrap().doc_id, 3);

    // Rewind
    it.rewind();

    // Check state after rewind
    assert_eq!(it.last_doc_id(), 0);
    assert_eq!(it.current().unwrap().doc_id, 0);
    assert!(!it.at_eof());

    // Should be able to read from beginning again
    let result = it.read().unwrap();
    let doc = result.unwrap();

    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(it.current().unwrap().doc_id, 1);
}

#[test]
fn read_after_skip() {
    let mut it = Wildcard::new(10);

    // Skip to middle
    let result = it.skip_to(5);
    assert_skip_to_found!(result, 5);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);

    // Continue reading sequentially from 6 to 10
    for expected_id in 6..=10 {
        let result = it.read().unwrap();
        let doc = result.unwrap();

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);
    }

    // After reading all remaining docs, should return EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

#[test]
fn skip_to_after_eof() {
    let mut it = Wildcard::new(10);

    // First, move to EOF by skipping beyond range
    let result = it.skip_to(11);
    assert!(result.is_ok());
    assert!(it.at_eof());

    // Try to skip to a valid target while at EOF
    let result = it.skip_to(5);
    let outcome = result.unwrap();
    assert!(outcome.is_none());
    assert!(it.at_eof());
}

#[test]
fn zero_documents() {
    let mut it = Wildcard::new(0);

    // Should immediately be at EOF
    assert!(it.at_eof(), "iterator with top_id=0 should be at EOF");
    assert_eq!(it.last_doc_id(), 0, "last_doc_id should be 0");
    assert_eq!(it.current().unwrap().doc_id, 0, "current().id should be 0");
    assert_eq!(it.num_estimated(), 0, "num_estimated should be 0");

    // Read should return None
    let result = it.read();
    let outcome = result.unwrap();
    assert!(outcome.is_none());

    // Skip should return None
    let result = it.skip_to(1);
    let outcome = result.expect("skip_to(1) should succeed");
    assert!(
        outcome.is_none(),
        "skip_to(1) should return Ok(None) for empty iterator"
    );
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn skip_to_backwards() {
    let mut it = Wildcard::new(100);

    let _ = it.skip_to(75);

    // Try to skip backwards to position 25, should panic
    let _ = it.skip_to(25);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn skip_to_same_position() {
    let mut it = Wildcard::new(10);

    // Skip to position 5
    let result = it.skip_to(5);
    assert!(result.is_ok());
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().unwrap().doc_id, 5);

    // Try to skip backwards to the same position, should panic
    let _ = it.skip_to(5);
}
