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
fn initial_state_none() {
    let it: Option<Wildcard> = None;

    assert_eq!(it.last_doc_id(), 0);
    assert!(it.at_eof());
    assert_eq!(it.num_estimated(), 0);
}

#[test]
fn initial_state_some_wildcard() {
    let it = Some(Wildcard::new(10));

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), 10);
}

#[test]
fn read_none() {
    let mut it: Option<Wildcard> = None;

    assert_eq!(it.num_estimated(), 0);
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn read_some_wildcard_sequential() {
    let mut it = Some(Wildcard::new(5));

    // Read all documents sequentially
    for expected_id in 1..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();
        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);

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
fn skip_to_none() {
    let mut it: Option<Wildcard> = None;

    assert!(matches!(it.skip_to(1), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(matches!(it.skip_to(1000), Ok(None)));
}

#[test]
fn skip_to_some_wildcard_valid_targets() {
    let mut it = Some(Wildcard::new(10));

    // Test skipping to middle
    let result = it.skip_to(5);
    assert_skip_to_found!(result, 5);
    assert_eq!(it.last_doc_id(), 5);
    assert!(!it.at_eof());

    // Test skipping to last document
    let result = it.skip_to(10);
    assert_skip_to_found!(result, 10);
    assert_eq!(it.last_doc_id(), 10);
    assert!(it.at_eof());
}

#[test]
fn rewind_none() {
    let mut it: Option<Wildcard> = None;

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn rewind_some_wildcard() {
    let mut it = Some(Wildcard::new(10));

    // Read some documents
    for _i in 1..=3 {
        let result = it.read().unwrap();
        assert!(result.is_some());
    }

    assert_eq!(it.last_doc_id(), 3);

    // Rewind
    it.rewind();

    // Check state after rewind
    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());

    // Should be able to read from beginning again
    let result = it.read().unwrap();
    let doc = result.unwrap();

    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
}

#[test]
fn revalidate_none() {
    let mut it: Option<Wildcard> = None;
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_some_wildcard() {
    let mut it = Some(Wildcard::new(10));
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}
