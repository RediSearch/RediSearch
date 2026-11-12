/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::RefCell, collections::HashSet, rc::Rc};

use index_result::{RSIndexResult, RSResultKind};
use rqe_core::DocId;
use rqe_iterators::{
    ExpirationChecker, IteratorType, NoOpChecker, RQEIterator, SkipToOutcome, lex_range::LexRange,
};
use rqe_iterators_test_utils::ContractChecker;

/// Expires the documents in a set the test can still change after handing the
/// checker to the iterator, as time passing between two cursor batches would.
#[derive(Clone, Default)]
struct ExpiredDocs(Rc<RefCell<HashSet<DocId>>>);

impl ExpiredDocs {
    fn expire(&self, doc_id: DocId) {
        self.0.borrow_mut().insert(doc_id);
    }
}

impl ExpirationChecker for ExpiredDocs {
    fn has_expiration(&self) -> bool {
        true
    }

    fn is_expired(&self, result: &RSIndexResult) -> bool {
        self.0.borrow().contains(&result.doc_id)
    }
}

fn lex_range<C: ExpirationChecker>(ids: Vec<DocId>, expiration: C) -> LexRange<'static, C> {
    LexRange::new(
        ids,
        RSIndexResult::build_virt().frequency(1).build(),
        expiration,
        None,
        0,
    )
}

fn drain<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    let mut ids = Vec::new();
    while let Some(result) = it.read().expect("an ID list never fails to read") {
        ids.push(result.doc_id);
    }
    ids
}

#[test]
fn reads_collected_ids_as_virtual_results() {
    let mut it = ContractChecker::new(lex_range(vec![1, 3, 5], NoOpChecker));
    assert_eq!(it.type_(), IteratorType::IdListSorted);
    let first = it.read().unwrap().expect("three ids to read");
    assert_eq!(first.kind(), RSResultKind::Virtual);
    assert_eq!(first.freq, 1);
    assert_eq!(drain(&mut it), vec![3, 5]);
}

#[test]
fn read_skips_documents_expired_after_collection() {
    let expired = ExpiredDocs::default();
    let mut it = ContractChecker::new(lex_range(vec![1, 2, 3, 4], expired.clone()));
    assert_eq!(it.read().unwrap().map(|r| r.doc_id), Some(1));

    // NOTE: Expiring after the first read stands for a field expiring between
    // two cursor batches, after the range collected its documents.
    expired.expire(2);
    expired.expire(4);

    assert_eq!(drain(&mut it), vec![3]);
}

#[test]
fn skip_to_valid_document_is_found() {
    let mut it = ContractChecker::new(lex_range(vec![1, 2, 3], ExpiredDocs::default()));
    let Ok(Some(SkipToOutcome::Found(result))) = it.skip_to(2) else {
        panic!("2 is collected and not expired");
    };
    assert_eq!(result.doc_id, 2);
}

#[test]
fn skip_to_expired_document_moves_to_next_valid_one() {
    let expired = ExpiredDocs::default();
    let mut it = ContractChecker::new(lex_range(vec![1, 2, 3, 4], expired.clone()));
    expired.expire(2);
    expired.expire(3);

    let Ok(Some(SkipToOutcome::NotFound(result))) = it.skip_to(2) else {
        panic!("2 is expired, so the skip must report the next valid document");
    };
    assert_eq!(result.doc_id, 4);
}

#[test]
fn skip_to_past_last_valid_document_reaches_end() {
    let expired = ExpiredDocs::default();
    let mut it = ContractChecker::new(lex_range(vec![1, 2, 3], expired.clone()));
    expired.expire(3);

    assert!(matches!(it.skip_to(3), Ok(None)));
    assert!(it.at_eof());
}
