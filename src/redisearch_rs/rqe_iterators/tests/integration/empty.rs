/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{IteratorType, RQEIterator, empty::Empty};
use rqe_iterators_test_utils::ContractChecker;

#[test]
fn current() {
    let mut it = ContractChecker::new(Empty::default());
    assert!(it.current().is_none());
}

#[test]
fn read() {
    let mut it = ContractChecker::new(Empty::default());

    assert_eq!(it.num_estimated(), 0);
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn skip_to() {
    let mut it = ContractChecker::new(Empty::default());

    assert!(matches!(it.skip_to(1), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(matches!(it.skip_to(1000), Ok(None)));
}

#[test]
fn rewind() {
    let mut it = ContractChecker::new(Empty::default());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn type_() {
    let it = ContractChecker::new(Empty::default());
    assert_eq!(it.type_(), IteratorType::Empty);
}

mod via_resume {}

#[test]
fn empty_upholds_current_contract() {
    use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
    let mut it = ContractChecker::new(Empty::default());
    assert!(assert_current_contract(&mut it).is_empty());
    assert_current_contract_via_skip_to(&mut it, 81);
}
