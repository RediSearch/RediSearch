/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags, t_docId};
use inverted_index::{
    DecodedBy, Encoder, InvertedIndex, RSIndexResult, RSResultKind, test_utils::TermRecordCompare,
};
use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome};
use std::cell::UnsafeCell;

/// Test basic read and skip_to functionality for a given iterator.
pub(super) struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    pub(super) ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
}

/// assert that both records are equal
fn check_record(record: &RSIndexResult, expected: &RSIndexResult) {
    if record.kind() == RSResultKind::Term {
        // the term record is not encoded in the II so we can't compare it directly
        assert_eq!(TermRecordCompare(record), TermRecordCompare(expected));
    } else {
        assert_eq!(record, expected);
    }
}

impl<E: Encoder> BaseTest<E> {
    pub(super) fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::<E>::new(ii_flags);

        for doc_id in doc_ids.iter() {
            let record = create_record(*doc_id);
            ii.add_record(&record).expect("failed to add record");
        }

        Self {
            doc_ids,
            ii,
            expected_record,
        }
    }

    /// Iterator over all the document ids present in the inverted index.
    pub(super) fn docs_ids_iter(&self) -> impl Iterator<Item = u64> {
        self.doc_ids.iter().map(|id| *id)
    }

    /// test read functionality for a given iterator.
    ///
    /// `docs_ids` is an iterator over the expected document ids to read.
    pub(super) fn read<'index, I>(&self, it: &mut I, doc_ids: impl Iterator<Item = u64>)
    where
        I: RQEIterator<'index>,
    {
        let expected_record = &*self.expected_record;

        for doc_id in doc_ids {
            let record = it
                .read()
                .expect("failed to read")
                .expect("expected result not eof");

            check_record(record, &expected_record(record.doc_id));
            assert_eq!(it.last_doc_id(), doc_id);
            assert_eq!(it.current().unwrap().doc_id, doc_id);
            assert!(!it.at_eof());
        }

        // We should have read all the documents
        assert_eq!(it.read().unwrap(), None);
        assert!(it.at_eof());
        assert_eq!(it.num_estimated(), self.doc_ids.len());
        assert_eq!(it.num_estimated(), self.ii.unique_docs() as usize);

        // try reading at eof
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    /// Test skip_to functionality for a given iterator.
    ///
    /// Since the index contains only ODD doc IDs (1, 3, 5, 7, ...), when we skip to an EVEN doc ID,
    /// we expect `NotFound` with the next odd doc ID returned.
    pub(super) fn skip_to<'index, I>(&self, it: &mut I)
    where
        I: RQEIterator<'index>,
    {
        // Test skipping to any id between 1 and the last id.
        let expected_record = &*self.expected_record;
        // Test skipping to any id between 1 and the last id
        let mut i = 1;
        for id in self.doc_ids.iter().copied() {
            // First, test skipping to the even doc ID that comes before this odd ID
            // (except for the first iteration where i=1 and id=1).
            // Since doc IDs are odd numbers (1, 3, 5, ...), the even numbers don't exist.
            if i < id {
                it.rewind();
                let res = it.skip_to(i);
                // Expect NotFound because `i` doesn't exist in the index (it's an even number).
                // The iterator should return the next available document, which is `id`.
                let Ok(Some(SkipToOutcome::NotFound(record))) = res else {
                    panic!("skip_to {i} should succeed with NotFound: {res:?}");
                };

                check_record(record, &expected_record(id));
                assert_eq!(it.last_doc_id(), id);
                assert_eq!(it.current().unwrap().doc_id, id);
                i += 1;
            }
            // Now test skipping to the exact doc ID that exists in the index.
            it.rewind();
            let res = it.skip_to(id);
            // Expect Found because `id` is an odd number that exists in the index.
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            check_record(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
            i += 1;
        }

        // Test reading after skipping to the last id
        assert!(matches!(it.read(), Ok(None)));
        let last_doc_id = it.last_doc_id();
        assert!(matches!(it.skip_to(last_doc_id + 1), Ok(None)));
        assert!(it.at_eof());

        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(!it.at_eof());

        // Test skipping to all ids that exist
        for id in self.doc_ids.iter().copied() {
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            check_record(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            assert_eq!(it.current().unwrap().doc_id, id);
        }

        // Test skipping to an id that exceeds the last id
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(!it.at_eof());
        let res = it.skip_to(self.doc_ids.last().unwrap() + 1);
        assert!(matches!(res, Ok(None)));
        // we just rewound
        assert_eq!(it.last_doc_id(), 0);
        assert_eq!(it.current().unwrap().doc_id, 0);
        assert!(it.at_eof());
    }
}

/// Test the revalidation of the iterator.
pub(super) struct RevalidateTest<E> {
    #[allow(dead_code)]
    doc_ids: Vec<t_docId>,
    // FIXME: horrible hack so we can get a mutable reference to the InvertedIndex while holding an immutable one through the iterator.
    // We should get rid of it once we have designed a proper way to manage concurrent access to the II.
    pub(super) ii: UnsafeCell<InvertedIndex<E>>,
}

impl<E: Encoder + DecodedBy> RevalidateTest<E> {
    pub(super) fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::<E>::new(ii_flags);
        for doc_id in doc_ids.iter() {
            let record = create_record(*doc_id);
            ii.add_record(&record).expect("failed to add record");
        }

        Self {
            doc_ids,
            ii: UnsafeCell::new(ii),
        }
    }

    /// test basic revalidation functionality - should return `RQEValidateStatus::Ok`` when index is valid
    pub(super) fn revalidate_basic<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(matches!(it.read(), Ok(Some(_))));
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
    }

    /// test revalidation functionality when iterator is at EOF
    pub(super) fn revalidate_at_eof<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        // Read all documents to reach EOF
        while let Some(_record) = it.read().expect("failed to read") {}
        assert!(it.at_eof());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
    }

    /// test revalidate returns `Aborted` when the underlying index disappears
    pub(super) fn revalidate_after_index_disappears<'index, I>(
        &self,
        it: &mut I,
        full_iterator: bool,
    ) where
        I: for<'iterator> RQEIterator<'index>,
    {
        // First, verify the iterator works normally and read at least one document
        // TODO: update this comment once we actually implement CheckAbort:
        // This is important because CheckAbort functions need current->data.term.term to be set
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        if full_iterator {
            // Full iterators don't have sctx, so they can't detect disappearance
            // They will always return Ok regardless of index state
            assert_eq!(
                it.revalidate().expect("revalidate failed"),
                RQEValidateStatus::Ok
            );
        } else {
            todo!()
        }
    }

    /// Remove the document with the given id from the inverted index.
    #[cfg(not(miri))] // Miri does not like UnsafeCell
    pub(super) fn remove_document(&self, doc_id: t_docId) {
        let ii = unsafe { &mut *self.ii.get() };

        let scan_delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &inverted_index::IndexBlock)>,
            )
            .expect("scan GC failed")
            .expect("no GC scan delta");
        let info = ii.apply_gc(scan_delta);
        assert_eq!(info.entries_removed, 1);
    }

    /// test revalidate returns `Moved` when the document at the iterator position is deleted from the index.
    #[cfg(not(miri))] // Miri does not like UnsafeCell
    pub(super) fn revalidate_after_document_deleted<'index, I>(&self, it: &mut I)
    where
        I: for<'iterator> RQEIterator<'index>,
    {
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // First, read a few documents to establish a position
        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[0]);

        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[1]);

        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[2]);

        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Nothing changed in the index so revalidate does nothing
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Remove an element before the current iteration position.
        self.remove_document(self.doc_ids[0]);
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Remove an element after the current iteration position.
        self.remove_document(self.doc_ids[4]);
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert_eq!(it.last_doc_id(), self.doc_ids[2]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

        // Remove the element at the current position of the iterator.
        // When validating we won't be able to skip to this element, so we should get RQEValidateStatus::Moved.
        self.remove_document(self.doc_ids[2]);
        let res = it.revalidate().expect("revalidate failed");
        let current_doc = match res {
            RQEValidateStatus::Moved {
                current: Some(current),
            } => current,
            _ => panic!("wrong revalidate result: {:?}", res),
        };
        assert_eq!(current_doc.doc_id, self.doc_ids[3]);
        // iterator advanced to the next element
        assert_eq!(it.last_doc_id(), self.doc_ids[3]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[3]);

        // read the next element, docs_ids[4] has been removed so iterator should return the one after.
        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[5]);
        assert_eq!(it.last_doc_id(), self.doc_ids[5]);
        assert_eq!(it.current().unwrap().doc_id, self.doc_ids[5]);

        // edge case: iterator is at the last document which is then removed.
        assert!(!it.at_eof());
        let last_doc_id = *self.doc_ids.last().unwrap();
        let doc = match it.skip_to(last_doc_id) {
            Ok(Some(SkipToOutcome::Found(doc))) => doc,
            _ => panic!("skip_to {last_doc_id} should succeed"),
        };
        assert_eq!(doc.doc_id, last_doc_id);
        assert_eq!(it.last_doc_id(), last_doc_id);
        assert_eq!(it.current().unwrap().doc_id, last_doc_id);

        self.remove_document(last_doc_id);
        // revalidate should return Moved without current doc and be at EOF.
        let res = it.revalidate().expect("revalidate failed");
        assert!(matches!(res, RQEValidateStatus::Moved { current: None }));
        assert!(it.at_eof());
    }
}
