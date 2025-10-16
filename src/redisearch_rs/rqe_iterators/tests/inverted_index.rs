/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cell::UnsafeCell;

use ::inverted_index::{DecodedBy, Encoder, InvertedIndex, RSIndexResult, numeric::Numeric};
use ffi::{IndexFlags, IndexFlags_Index_DocIdsOnly, t_docId};
use rqe_iterators::{RQEIterator, RQEValidateStatus, SkipToOutcome, inverted_index::NumericFull};

mod c_mocks;

/// Test basic read and skip_to functionality for a given iterator.
struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
}

impl<E: Encoder + DecodedBy + Default> BaseTest<E> {
    fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::new(ii_flags, E::default());
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

    /// test read functionality for a given iterator.
    fn read<I: RQEIterator>(&self, mut it: I) {
        let expected_record = &*self.expected_record;
        let mut i = 0;

        for _ in 0..=self.doc_ids.len() {
            {
                let result = it.read();
                match result {
                    Ok(Some(record)) => {
                        assert_eq!(record, &expected_record(record.doc_id));
                    }
                    _ => break,
                }
            }

            assert_eq!(it.last_doc_id(), self.doc_ids[i]);
            assert!(!it.at_eof());
            i += 1;
        }

        assert_eq!(
            i,
            self.doc_ids.len(),
            "expected to read {} documents but only got {i}",
            self.doc_ids.len()
        );
        assert!(it.at_eof());
        assert_eq!(it.num_estimated(), self.doc_ids.len());
        assert_eq!(it.num_estimated(), self.ii.unique_docs());
    }

    /// test skip_to functionality for a given iterator.
    fn skip_to<I: RQEIterator>(&self, mut it: I) {
        let expected_record = &*self.expected_record;
        // Test skipping to any id between 1 and the last id
        let mut i = 1;
        for id in self.doc_ids.iter().copied() {
            while i < id {
                it.rewind();
                let res = it.skip_to(i);
                let Ok(Some(SkipToOutcome::NotFound(record))) = res else {
                    panic!("skip_to {i} should succeed with NotFound: {res:?}");
                };

                assert_eq!(record, &expected_record(id));
                assert_eq!(it.last_doc_id(), id);
                i += 1;
            }
            it.rewind();
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            assert_eq!(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            i += 1;
        }

        // Test reading after skipping to the last id
        assert!(matches!(it.read(), Ok(None)));
        assert!(matches!(it.skip_to(it.last_doc_id() + 1), Ok(None)));
        assert!(it.at_eof());

        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());

        // Test skipping to all ids that exist
        for id in self.doc_ids.iter().copied() {
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            assert_eq!(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
        }

        // Test skipping to an id that exceeds the last id
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
        let res = it.skip_to(self.doc_ids.last().unwrap() + 1);
        assert!(matches!(res, Ok(None)));
        // we just rewound
        assert_eq!(it.last_doc_id(), 0);
        assert!(it.at_eof());
    }
}

/// Test the revalidation of the iterator.
struct RevalidateTest<E> {
    doc_ids: Vec<t_docId>,
    // FIXME: horrible hack so we can get a mutable reference to the InvertedIndex while holding an immutable one through the iterator.
    // We should get rid of it once we have designed a proper way to manage concurrent access to the II.
    ii: UnsafeCell<InvertedIndex<E>>,
}

impl<E: Encoder + DecodedBy + Default> RevalidateTest<E> {
    fn new(
        ii_flags: IndexFlags,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        n_docs: u64,
    ) -> Self {
        let create_record = &*expected_record;
        // Generate a set of odd document IDs for testing, starting from 1.
        let doc_ids = (0..=n_docs)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::new(ii_flags, E::default());
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
    fn revalidate_basic<I: RQEIterator>(&self, mut it: I) {
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
    fn revalidate_at_eof<I: RQEIterator>(&self, mut it: I) {
        // Read all documents to reach EOF
        while let Some(_record) = it.read().expect("failed to read") {}
        assert!(it.at_eof());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
    }

    /// test revalidate returns `Aborted` when the underlying index disappears
    fn revalidate_after_index_disappears<I: RQEIterator>(&self, mut it: I, full_iterator: bool) {
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
    fn remove_document(&self, doc_id: t_docId) {
        let ii = unsafe { &mut *self.ii.get() };

        let scan_delta = ii
            .scan_gc(|d| d != doc_id, |_, _| {})
            .expect("scan GC failed")
            .expect("no GC scan delta");
        let info = ii.apply_gc(scan_delta);
        assert_eq!(info.entries_removed, 1);
    }

    /// test revalidate returns `Moved` when the document at the iterator position is deleted from the index.
    fn revalidate_after_document_deleted<I: RQEIterator>(&self, mut it: I) {
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

        // Remove an element after the current iteration position.
        self.remove_document(self.doc_ids[4]);
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert_eq!(it.last_doc_id(), self.doc_ids[2]);

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

        // read the next element, docs_ids[4] has been removed so iterator should return the one after.
        let doc = it
            .read()
            .expect("failed to read")
            .expect("should not be at EOF");
        assert_eq!(doc.doc_id, self.doc_ids[5]);
        assert_eq!(it.last_doc_id(), self.doc_ids[5]);

        // edge case: iterator is at the lost document which is then removed.
        assert!(!it.at_eof());
        let last_doc_id = self.doc_ids.last().unwrap().clone();
        let doc = match it.skip_to(last_doc_id) {
            Ok(Some(SkipToOutcome::Found(doc))) => doc,
            _ => panic!("skip_to {last_doc_id} should succeed"),
        };
        assert_eq!(doc.doc_id, last_doc_id);
        assert_eq!(it.last_doc_id(), last_doc_id);

        self.remove_document(last_doc_id);
        // revalidate should return Moved without current doc and be at EOF.
        let res = it.revalidate().expect("revalidate failed");
        assert!(matches!(res, RQEValidateStatus::Moved { current: None }));
        assert!(it.at_eof());
    }
}

struct NumericTest {
    test: BaseTest<Numeric>,
    revalidate_test: RevalidateTest<Numeric>,
}

impl NumericTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        // The numeric record has a value of `doc_id * 2.0`.
        RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_DocIdsOnly,
                Box::new(Self::expected_record),
                n_docs,
            ),
            revalidate_test: RevalidateTest::new(
                IndexFlags_Index_DocIdsOnly,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    fn read(self) {
        self.test.read(NumericFull::new(self.test.ii.reader()));
    }

    fn skip_to(self) {
        self.test.skip_to(NumericFull::new(self.test.ii.reader()));
    }

    fn revalidate_basic(self) {
        let reader = unsafe { (*self.revalidate_test.ii.get()).reader() };
        self.revalidate_test
            .revalidate_basic(NumericFull::new(reader));
    }

    fn revalidate_at_eof(self) {
        let reader = unsafe { (*self.revalidate_test.ii.get()).reader() };
        self.revalidate_test
            .revalidate_at_eof(NumericFull::new(reader));
    }

    fn revalidate_after_index_disappears(self, full_iterator: bool) {
        let reader = unsafe { (*self.revalidate_test.ii.get()).reader() };
        self.revalidate_test
            .revalidate_after_index_disappears(NumericFull::new(reader), full_iterator);
    }

    fn revalidate_after_document_deleted(self) {
        let reader = unsafe { (*self.revalidate_test.ii.get()).reader() };
        self.revalidate_test
            .revalidate_after_document_deleted(NumericFull::new(reader));
    }
}

#[test]
/// test reading from NumericFull iterator
fn numeric_full_read() {
    NumericTest::new(100).read();
}

#[test]
/// test skipping from NumericFull iterator
fn numeric_full_skip_to() {
    NumericTest::new(100).skip_to();
}

#[test]
fn numeric_full_revalidate_basic() {
    NumericTest::new(10).revalidate_basic();
}

#[test]
fn numeric_full_revalidate_at_eof() {
    NumericTest::new(10).revalidate_at_eof();
}

#[test]
fn numeric_full_revalidate_after_index_disappears() {
    NumericTest::new(10).revalidate_after_index_disappears(true);
}

#[test]
fn numeric_full_revalidate_after_document_deleted() {
    NumericTest::new(10).revalidate_after_document_deleted();
}
