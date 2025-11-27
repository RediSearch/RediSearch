/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::RSIndexResult;
use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, id_list::SortedIdList,
    not_iterator::NotIterator,
};

mod c_mocks;

// Basic iterator invariants before any read.
#[test]
fn initial_state() {
    let child = SortedIdList::new(vec![2, 4, 6]);
    let it = NotIterator::new(child, 10);

    // Before first read, cursor is at 0 and we are not at EOF.
    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    // max_doc_id=10, so NOT can yield at most 10 docs.
    assert_eq!(it.num_estimated(), 10);
}

// Read path with sparse child: NOT must skip exactly the child doc IDs.
#[test]
fn read_skips_child_docs() {
    let child_ids = vec![2, 4, 7];
    let mut it = NotIterator::new(SortedIdList::new(child_ids), 10);

    // Child has [2, 4, 7]; complement in [1..=10] is [1, 3, 5, 6, 8, 9, 10].
    let expected = vec![1, 3, 5, 6, 8, 9, 10];

    for &expected_id in &expected {
        let result = it.read();
        let result = result.expect("read() must not error");
        let doc = result.expect("iterator should yield more docs");

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert_eq!(it.current().unwrap().doc_id, expected_id);
    }

    // After consuming all expected docs, we must be at EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

// Empty child: NOT behaves like a wildcard over [1, max_doc_id].
#[test]
fn read_with_empty_child_behaves_like_wildcard() {
    // When the child is empty, NOT should yield all doc IDs in [1, max_doc_id]
    let mut it = NotIterator::new(SortedIdList::new(vec![]), 5);

    for expected_id in 1u64..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();

        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
    }

    // Next read should be EOF
    let result = it.read().unwrap();
    assert!(result.is_none());
    assert!(it.at_eof());
}

// Child covers full range: NOT should be empty and report EOF.
#[test]
fn read_with_child_covering_full_range_yields_no_docs() {
    let mut it = NotIterator::new(SortedIdList::new(vec![1, 2, 3, 4, 5]), 5);

    // Child already produces 1..=5, so there is no doc left for NOT to return.
    let res = it.read().expect("read() must not error");
    assert!(res.is_none(), "NOT of full-range child should be empty");
    // Iterator still walks up to max_doc_id=5 internally and then reports EOF.
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 5);

    assert!(matches!(it.read(), Ok(None)));
}

// skip_to on ids below, between and inside child: Found vs NotFound semantics.
#[test]
fn skip_to_honours_child_membership() {
    let mut it = NotIterator::new(SortedIdList::new(vec![2, 4, 7]), 10);

    // 5 is not in child {2, 4, 7}, so NOT must return Found(5).
    let outcome = it.skip_to(5).expect("skip_to(5) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 5);
        assert_eq!(it.last_doc_id(), 5);
        assert!(!it.at_eof());
    } else {
        panic!("Expected Found outcome for skip_to(5), got {:?}", outcome);
    }

    // 1 is below first child doc (2) and not in child, so Found(1).
    it.rewind();
    let outcome = it.skip_to(1).expect("skip_to(1) must not error");
    if let Some(SkipToOutcome::Found(doc)) = outcome {
        assert_eq!(doc.doc_id, 1);
        assert_eq!(it.last_doc_id(), 1);
    } else {
        panic!("Expected Found outcome for skip_to(1), got {:?}", outcome);
    }

    // 4 is in child, so NOT should skip it and return NotFound(next allowed = 5).
    it.rewind();
    let outcome = it.skip_to(4).expect("skip_to(4) must not error");
    match outcome {
        Some(SkipToOutcome::NotFound(doc)) => {
            assert_eq!(doc.doc_id, 5);
            assert_eq!(it.last_doc_id(), 5);
        }
        other => panic!("Expected NotFound outcome for skip_to(4), got {:?}", other),
    }
}

// skip_to past max_doc_id: should return None and move to EOF.
#[test]
fn skip_to_past_max_docid_returns_none_and_sets_eof() {
    let mut it = NotIterator::new(SortedIdList::new(vec![2, 4, 7]), 10);

    // 11 > max_doc_id=10, so there is no valid target and we end at EOF.
    let res = it.skip_to(11).expect("skip_to(11) must not error");
    assert!(res.is_none());
    assert!(it.at_eof());
    assert_eq!(it.last_doc_id(), 10);

    assert!(matches!(it.read(), Ok(None)));
}

// rewind should restore the initial state and read sequence.
#[test]
fn rewind_resets_state() {
    let mut it = NotIterator::new(SortedIdList::new(vec![2, 4, 7]), 10);

    // For child [2, 4, 7] and max_doc_id=10, the first two NOT results are 1 and 3.
    for expected in [1u64, 3] {
        let doc = it.read().unwrap().unwrap();
        assert_eq!(doc.doc_id, expected);
    }
    assert_eq!(it.last_doc_id(), 3);
    assert!(!it.at_eof());

    it.rewind();

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());

    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
}

#[derive(Clone, Copy)]
enum NextRevalidate {
    Ok,
    Aborted,
}

struct MockChild<'index> {
    inner: SortedIdList<'index>,
    status: NextRevalidate,
}

impl<'index> MockChild<'index> {
    fn new_ok(ids: Vec<u64>) -> Self {
        Self {
            inner: SortedIdList::new(ids),
            status: NextRevalidate::Ok,
        }
    }

    fn new_aborted(ids: Vec<u64>) -> Self {
        Self {
            inner: SortedIdList::new(ids),
            status: NextRevalidate::Aborted,
        }
    }
}

impl<'index> RQEIterator<'index> for MockChild<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.inner.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.inner.read()
    }

    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.inner.skip_to(doc_id)
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(match self.status {
            NextRevalidate::Ok => RQEValidateStatus::Ok,
            NextRevalidate::Aborted => RQEValidateStatus::Aborted,
        })
    }

    fn rewind(&mut self) {
        self.inner.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.inner.num_estimated()
    }

    fn last_doc_id(&self) -> u64 {
        self.inner.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.inner.at_eof()
    }
}

// Child revalidate Ok: NOT still excludes the child's doc IDs.
#[test]
fn revalidate_child_ok_preserves_exclusions() {
    let mut it = NotIterator::new(MockChild::new_ok(vec![2, 4]), 5);

    let status = it.revalidate().expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    let mut seen = Vec::new();
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // Child has [2, 4] in [1..=5], so NOT must yield the complement [1, 3, 5].
    assert_eq!(seen, vec![1, 3, 5]);
}

// Child revalidate Aborted: NOT degenerates to wildcard (empty child).
#[test]
fn revalidate_child_aborted_replaces_child_with_empty() {
    let mut it = NotIterator::new(MockChild::new_aborted(vec![2, 4]), 5);

    let status = it.revalidate().expect("revalidate() failed");
    assert_eq!(status, RQEValidateStatus::Ok);

    let mut seen = Vec::new();
    while let Some(doc) = it.read().unwrap() {
        seen.push(doc.doc_id);
    }

    // After child aborts, NOT behaves like having an empty child: [1..=5] is returned.
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}
