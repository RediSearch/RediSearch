/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, empty::Empty, optional::Optional,
    wildcard::Wildcard,
};

mod c_mocks;

#[test]
fn read_pure_virtual() {
    let mut it = Optional::new(2, 3., Empty::default());
    assert_eq!(it.num_estimated(), 2);
    assert_eq!(it.last_doc_id(), 0); // starting point

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));

    assert_eq!(it.num_estimated(), 2);
    assert_eq!(it.last_doc_id(), 2);
}

#[test]
fn read_pure_wildcard() {
    let mut it = Optional::new(2, 3., Wildcard::new(3));
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
    let mut it = Optional::new(5, 3., Wildcard::new(2));
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
fn skip_to_pure_virtual() {
    let mut it = Optional::new(3, 5., Empty::default());

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
    let mut it = Optional::new(8, 5., Empty::default());

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
#[should_panic]
fn skip_to_pure_virtual_backwards() {
    let mut it = Optional::new(3, 5., Empty::default());

    let _ = it.skip_to(2);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(1);
}

#[test]
fn rewind_pure_virtual() {
    let mut it = Optional::new(3, 2., Empty::default());

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
    let mut it = Optional::new(3, 5., Wildcard::new(5));

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
    let mut it = Optional::new(8, 5., Wildcard::new(10));

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
#[should_panic]
fn skip_to_pure_wildcard_backwards() {
    let mut it = Optional::new(3, 5., Wildcard::new(8));

    let _ = it.skip_to(2);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(1);
}

#[test]
fn rewind_pure_wildcard() {
    let mut it = Optional::new(3, 2., Wildcard::new(5));

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
    let mut it = Optional::new(5, 5., Wildcard::new(3));

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
    let mut it = Optional::new(8, 5., Wildcard::new(5));

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
#[should_panic]
fn skip_to_hybrid_virtual_backwards() {
    let mut it = Optional::new(6, 5., Wildcard::new(3));

    let _ = it.skip_to(4);

    // Try to skip backwards to position 1, should panic
    let _ = it.skip_to(2);
}

#[test]
fn rewind_hybrid_virtual() {
    let mut it = Optional::new(3, 2., Wildcard::new(1));

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
fn revalidate_pure_virtual() {
    let mut it = Optional::new(5, 1., Empty::default());
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_pure_wildcard() {
    let mut it = Optional::new(5, 1., Wildcard::new(8));
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_hybrid() {
    let mut it = Optional::new(5, 1., Wildcard::new(2));
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}

#[derive(Default)]
struct RevalidateTestIterator<'index> {
    result: inverted_index::RSIndexResult<'index>,
    max_doc: ffi::t_docId,
    next_doc_id: ffi::t_docId,
    revalidate_jumps: Vec<ffi::t_docId>,
    abort_at: Option<ffi::t_docId>,
}

impl<'index> RevalidateTestIterator<'index> {
    fn new(
        max_doc: ffi::t_docId,
        mut revalidate_jumps: Vec<ffi::t_docId>,
        abort_at: Option<ffi::t_docId>,
    ) -> Self {
        revalidate_jumps.reverse();
        Self {
            result: inverted_index::RSIndexResult::numeric(42.),
            max_doc,
            next_doc_id: 1,
            revalidate_jumps,
            abort_at,
        }
    }
}

impl<'index> RQEIterator<'index> for RevalidateTestIterator<'index> {
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        if self.at_eof() {
            Ok(None)
        } else {
            self.result.doc_id = self.next_doc_id;
            self.next_doc_id += 1;
            Ok(Some(&mut self.result))
        }
    }

    fn skip_to(
        &mut self,
        _doc_id: ffi::t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError> {
        unimplemented!()
    }

    fn rewind(&mut self) {
        unimplemented!()
    }

    fn num_estimated(&self) -> usize {
        unimplemented!()
    }

    fn last_doc_id(&self) -> ffi::t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.max_doc
    }

    fn revalidate(
        &mut self,
    ) -> Result<RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        if let Some(abort_at) = self.abort_at
            && self.result.doc_id >= abort_at
        {
            assert_eq!(self.result.doc_id, abort_at);
            return Ok(RQEValidateStatus::Aborted);
        }
        if let Some(jump) = self.revalidate_jumps.pop() {
            self.next_doc_id = jump;
            self.result.doc_id = jump - 1;
            Ok(RQEValidateStatus::Moved {
                current: Some(&mut self.result),
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }
}

#[test]
fn revalidate_jumps() {
    let mut it = Optional::new(7, 1., RevalidateTestIterator::new(9, vec![4, 6], None));

    assert!(!it.at_eof());
    let outcome = it
        .read()
        .expect("read == Ok(..)")
        .expect("read == Ok(Some(..))");
    assert!(outcome.as_numeric().is_some());
    assert_eq!(outcome.weight, 1.);
    assert_eq!(outcome.doc_id, 1);

    // real hit => child revalidateD with move, but jumped ahead,
    // optional iterator needs to continue on its sequential path!
    match it.revalidate().unwrap() {
        RQEValidateStatus::Moved {
            current: Some(outcome),
        } => {
            assert!(outcome.as_numeric().is_none());
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 2);
        }
        _ => panic!("unexpected result"),
    }

    // virtual... but we did revalidate child... again,
    // meaning that the child is now already at pos 6...
    //
    // Status is ok however as we were virtual so from POV of callee
    // we did not move.
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);

    for expected_id in 3..=5 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(outcome.as_numeric().is_none());
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    for expected_id in 6..=7 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(outcome.as_numeric().is_some());
        assert_eq!(outcome.weight, 1.);
        assert_eq!(outcome.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    assert!(it.at_eof());
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 7);
}

#[test]
fn revalidate_abort() {
    let mut it = Optional::new(7, 1., RevalidateTestIterator::new(9, vec![], Some(2)));

    for expected_id in 1..=2 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(outcome.as_numeric().is_some());
        assert_eq!(outcome.weight, 1.);
        assert_eq!(outcome.doc_id, expected_id);
    }

    // abort but was real, we move regardless
    match it.revalidate().unwrap() {
        RQEValidateStatus::Moved {
            current: Some(outcome),
        } => {
            assert!(outcome.as_numeric().is_none());
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 3);
        }
        _ => panic!("unexpected result"),
    }

    for expected_id in 4..=7 {
        assert!(!it.at_eof());
        let outcome = it
            .read()
            .expect("read == Ok(..)")
            .expect("read == Ok(Some(..))");
        assert!(outcome.as_numeric().is_none());
        assert_eq!(outcome.weight, 0.);
        assert_eq!(outcome.doc_id, expected_id);

        // virtual is always ok...
        assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
    }

    assert!(it.at_eof());
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn revalidate_abort_at_virtual() {
    let mut it = Optional::new(4, 1., RevalidateTestIterator::new(3, vec![3], Some(2)));

    // real-read, all goood

    assert!(!it.at_eof());
    let outcome = it
        .read()
        .expect("read == Ok(..)")
        .expect("read == Ok(Some(..))");
    assert!(outcome.as_numeric().is_some());
    assert_eq!(outcome.weight, 1.);
    assert_eq!(outcome.doc_id, 1);

    // real hit => child revalidated with move, but jumped ahead,
    // optional iterator needs to continue on its sequential path!
    match it.revalidate().unwrap() {
        RQEValidateStatus::Moved {
            current: Some(outcome),
        } => {
            assert!(outcome.as_numeric().is_none());
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 2);
        }
        _ => panic!("unexpected result"),
    }

    // was virtual (due  to previous revalidate => now abort while virtual...
    // this will just move it 1 ahead...)

    match it.revalidate().unwrap() {
        RQEValidateStatus::Moved {
            current: Some(outcome),
        } => {
            assert!(outcome.as_numeric().is_none());
            assert_eq!(outcome.weight, 0.);
            assert_eq!(outcome.doc_id, 3);
        }
        _ => panic!("unexpected result"),
    }

    assert!(!it.at_eof());
    let outcome = it
        .read()
        .expect("read == Ok(..)")
        .expect("read == Ok(Some(..))");
    assert!(outcome.as_numeric().is_none());
    assert_eq!(outcome.weight, 0.);
    assert_eq!(outcome.doc_id, 4);

    // virtual is always ok...
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);

    // done though

    assert!(it.at_eof());
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}
