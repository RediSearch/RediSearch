/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, maybe_empty::MaybeEmpty,
};

mod c_mocks;

#[derive(Default)]
struct Infinite<'index>(inverted_index::RSIndexResult<'index>);

impl<'index> RQEIterator<'index> for Infinite<'index> {
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        Some(&mut self.0)
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, RQEIteratorError> {
        self.0.doc_id += 1;
        Ok(Some(&mut self.0))
    }

    fn skip_to(
        &mut self,
        doc_id: ffi::t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, crate::RQEIteratorError> {
        self.0.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.0)))
    }

    fn rewind(&mut self) {
        self.0.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        usize::MAX
    }

    fn last_doc_id(&self) -> ffi::t_docId {
        self.0.doc_id
    }

    fn at_eof(&self) -> bool {
        false
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, crate::RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }
}

#[test]
fn initial_state_empty() {
    let it = MaybeEmpty::<Infinite>::new_empty();

    assert_eq!(it.last_doc_id(), 0);
    assert!(it.at_eof());
    assert_eq!(it.num_estimated(), 0);
}

#[test]
fn initial_state_not_empty() {
    let it = MaybeEmpty::new(Infinite::default());

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), usize::MAX);
}

#[test]
fn read_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert_eq!(it.num_estimated(), 0);
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn read_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());
    for expected_id in 1..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();
        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert!(!it.at_eof());
    }
}

#[test]
fn skip_to_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert!(matches!(it.skip_to(1), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(matches!(it.skip_to(1000), Ok(None)));
}

#[test]
fn skip_to_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());

    for i in 1..=5 {
        let id = (i * 5) as ffi::t_docId;
        let outcome = it.skip_to(id).unwrap();
        assert_eq!(
            outcome,
            Some(SkipToOutcome::Found(
                &mut inverted_index::RSIndexResult::virt().doc_id(id)
            ))
        );
        assert_eq!(it.last_doc_id(), id);
        assert!(!it.at_eof());
    }
}

#[test]
fn rewind_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn rewind_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());

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
fn revalidate_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}

#[test]
fn revalidate_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());
    assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
}
