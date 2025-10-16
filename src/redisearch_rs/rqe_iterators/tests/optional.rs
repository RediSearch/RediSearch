/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, optional::Optional, wildcard::Wildcard,
};

mod c_mocks;

#[test]
fn read_default() {
    let mut it = Optional::default();
    assert_eq!(it.num_estimated(), 0);

    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 0);
}

#[test]
fn rewind_default() {
    let mut it = Optional::default();

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn read_pure_virtual() {
    let mut it = Optional::new(2, 3., None);
    assert_eq!(it.num_estimated(), 2);

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 2);
}

#[test]
fn read_pure_wildcard() {
    let mut it = Optional::new(2, 3., Some(Wildcard::new(3)));
    assert_eq!(it.num_estimated(), 2);

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 3.);
        assert_eq!(outcome.doc_id, expected_id);
    }

    // inner child still has availability,
    // but optional iterator cuts off at max = 2

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 2);
}

#[test]
fn read_hybrid_virtual() {
    let mut it = Optional::new(5, 3., Some(Wildcard::new(2)));
    assert_eq!(it.num_estimated(), 5);

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 3.);
        assert_eq!(outcome.doc_id, expected_id);
    }

    // three more virtual

    for expected_id in 3..=5 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 5);
}

#[test]
fn skip_to_default() {
    let mut it = Optional::default();

    assert!(matches!(it.skip_to(1), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(matches!(it.skip_to(1000), Ok(None)));
}

#[test]
fn skip_to_pure_virtual() {
    let mut it = Optional::new(3, 5., None);

    match it
        .skip_to(2)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 2);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }

    assert!(!it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn read_after_skip_pure_virtual() {
    let mut it = Optional::new(8, 5., None);

    match it
        .skip_to(3)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 3);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(5)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 5);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    for expected_id in 6..=8 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 8);

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());

    assert_eq!(it.num_estimated(), 8);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn skip_to_pure_virtual_backwards() {
    let mut it = Optional::new(3, 5., None);

    let _ = it.skip_to(2);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(1);
}

#[test]
fn rewind_pure_virtual() {
    let mut it = Optional::new(3, 2., None);

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(!it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    for expected_id in 1..=3 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn skip_to_pure_wildcard() {
    let mut it = Optional::new(3, 5., Some(Wildcard::new(5)));

    match it
        .skip_to(1)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 1);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(3)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 3);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn read_after_skip_pure_wildcard() {
    let mut it = Optional::new(8, 5., Some(Wildcard::new(10)));

    match it
        .skip_to(3)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 3);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(5)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 5);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    for expected_id in 6..=8 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 5.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 8);

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());

    assert_eq!(it.num_estimated(), 8);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn skip_to_pure_wildcard_backwards() {
    let mut it = Optional::new(3, 5., Some(Wildcard::new(8)));

    let _ = it.skip_to(2);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(1);
}

#[test]
fn rewind_pure_wildcard() {
    let mut it = Optional::new(3, 2., Some(Wildcard::new(5)));

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 2.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(!it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    for expected_id in 1..=3 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 2.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn skip_to_hybrid_virtual() {
    let mut it = Optional::new(5, 5., Some(Wildcard::new(3)));

    match it
        .skip_to(1)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 1);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(3)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 3);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(5)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 5);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());
}
#[test]
fn read_after_skip_hybrid_virtual() {
    let mut it = Optional::new(8, 5., Some(Wildcard::new(5)));

    match it
        .skip_to(3)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 3);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    match it
        .skip_to(5)
        .expect("skip_to == Ok(..)")
        .expect("skip_to == Ok(Some(..))")
    {
        SkipToOutcome::Found(outcome) => {
            assert_eq!(outcome.weight, 5.);
            assert_eq!(outcome.doc_id, 5);
        }
        SkipToOutcome::NotFound(outcome) => panic!("unexpected not-found outcome: {outcome:?}"),
    }
    assert!(!it.at_eof());

    for expected_id in 6..=8 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 8);

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(1000), Ok(None)));
    assert!(it.at_eof());

    assert_eq!(it.num_estimated(), 8);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic]
fn skip_to_hybrid_virtual_backwards() {
    let mut it = Optional::new(6, 5., Some(Wildcard::new(3)));

    let _ = it.skip_to(4);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(2);
}

#[test]
fn rewind_hybrid_virtual() {
    let mut it = Optional::new(3, 2., Some(Wildcard::new(1)));

    assert!(!it.at_eof());
    let outcome = it
        .read()
        .expect("read == Ok(..)")
        .expect("read == Ok(Some(..))");
    assert_eq!(outcome.weight, 2.);
    assert_eq!(outcome.doc_id, 1);
    assert!(!it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, if expected_id == 1 { 2. } else { 0. });
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(!it.at_eof());

    it.rewind();
    assert!(!it.at_eof());

    for expected_id in 1..=3 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, if expected_id == 1 { 2. } else { 0. });
        assert_eq!(outcome.doc_id, expected_id);
    }
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn revalidate_default() {
    let mut it = Optional::default();
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_pure_virtual() {
    let mut it = Optional::new(5, 1., None);
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_pure_wildcard() {
    let mut it = Optional::new(5, 1., Some(Wildcard::new(8)));
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_hybrid() {
    let mut it = Optional::new(5, 1., Some(Wildcard::new(2)));
    assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
}

// TODO: once there is a Moved/Aborted scenario
// also test these here...
