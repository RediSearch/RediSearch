/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use core::panic;

use ffi::{IndexFlags_Index_DocIdsOnly, t_docId};
use inverted_index::{Encoder, InvertedIndex, RSIndexResult};
use rqe_iterators::{
    RQEIterator, RQEValidateStatus, SkipToOutcome, inverted_index_it::NumericFull,
};

mod c_mocks;

/// Test basic read and skip_to functionality for a given iterator.
struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
}

impl<E: Encoder> BaseTest<E> {
    fn new(enc: E) -> Self {
        let n_records = E::RECOMMENDED_BLOCK_ENTRIES;
        // Generate a set of odd document IDs for testing, starting from 1.
        // The numeric record has a value of `doc_id * 2.0`.
        let doc_ids = (0..n_records)
            .map(|i| (2 * i + 1) as t_docId)
            .collect::<Vec<_>>();

        let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, enc);
        for doc_id in doc_ids.iter() {
            let record = RSIndexResult::numeric(*doc_id as f64 * 2.0).doc_id(*doc_id);
            ii.add_record(&record).expect("failed to add record");
        }

        Self { doc_ids, ii }
    }

    /// test read functionality for a given iterator.
    fn read<I: RQEIterator>(&self, mut it: I) {
        let mut i = 0;

        loop {
            {
                let result = it.read();
                match result {
                    Ok(Some(record)) => {
                        assert_eq!(record.doc_id, self.doc_ids[i]);
                        assert_eq!(record.as_numeric(), Some(self.doc_ids[i] as f64 * 2.0));
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
        // Test skipping to any id between 1 and the last id
        let mut i = 1;
        for id in self.doc_ids.iter().copied() {
            while i < id {
                it.rewind();
                let res = it.skip_to(i);
                let Ok(Some(SkipToOutcome::NotFound(record))) = res else {
                    panic!("skip_to {i} should succeed with NotFound: {res:?}");
                };

                assert_eq!(record.doc_id, id);
                assert_eq!(record.as_numeric(), Some(id as f64 * 2.0));
                assert_eq!(it.last_doc_id(), id);
                i += 1;
            }
            it.rewind();
            let res = it.skip_to(id);
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            assert_eq!(record.doc_id, id);
            assert_eq!(record.as_numeric(), Some(id as f64 * 2.0));
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
            assert_eq!(record.doc_id, id);
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

#[test]
/// test reading from NumericFull iterator
fn numeric_full_read() {
    let test = BaseTest::new(inverted_index::numeric::Numeric::default());
    test.read(NumericFull::new(test.ii.reader()));
}

#[test]
/// test skipping from NumericFull iterator
fn numeric_full_skip_to() {
    let test = BaseTest::new(inverted_index::numeric::Numeric::default());
    test.skip_to(NumericFull::new(test.ii.reader()));
}

struct RevalidateTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
}

impl<E: Encoder> RevalidateTest<E> {
    fn new(enc: E) -> Self {
        let mut ii = InvertedIndex::new(IndexFlags_Index_DocIdsOnly, enc);

        let n_docs = 10;
        let doc_ids = (0..n_docs).map(|i| (i + 1) as t_docId).collect::<Vec<_>>();

        for i in 0..n_docs {
            let record = RSIndexResult::numeric(i as f64 * 10.0).doc_id(doc_ids[i]);
            ii.add_record(&record).expect("failed to add record");
        }

        Self { doc_ids, ii }
    }

    /// Basic test to ensure the iterator setup works correctly
    fn basic_iterator_functionality<I: RQEIterator>(&self, mut it: I) {
        // Test that we can read all documents
        let mut count = 0;
        while let Some(record) = it.read().expect("failed to read") {
            assert_eq!(record.doc_id, self.doc_ids[count]);
            count += 1;
        }
        assert!(it.at_eof());
        assert_eq!(count, self.doc_ids.len());

        // Test rewind functionality
        it.rewind();
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
    }

    /// test basic revalidation functionality - should return VALIDATE_OK when index is valid
    fn revalidate_basic<I: RQEIterator>(&self, mut it: I) {
        assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
        assert!(matches!(it.read(), Ok(Some(_))));
        assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
    }

    /// test revalidation functionality when iterator is at EOF
    fn revalidate_at_eof<I: RQEIterator>(&self, mut it: I) {
        while let Some(_record) = it.read().expect("failed to read") {}
        assert!(it.at_eof());
        assert_eq!(it.revalidate(), RQEValidateStatus::Ok);
    }

    // test revalidate returns `Aborted` when the underlying index disappears
    fn revalidate_after_index_disappears<I: RQEIterator>(&self, mut _it: I) {
        // TODO
    }

    // test revalidate returns `Moved` when the lastDocId was deleted from the index
    fn revalidate_after_document_deleted<I: RQEIterator>(&self, mut _it: I) {
        // TODO
    }
}

#[test]
/// test revalidating numeric_full iterator
fn numeric_full_revalidate() {
    let test = RevalidateTest::new(inverted_index::numeric::Numeric::default());
    test.basic_iterator_functionality(NumericFull::new(test.ii.reader()));
    test.revalidate_basic(NumericFull::new(test.ii.reader()));
    test.revalidate_at_eof(NumericFull::new(test.ii.reader()));
    test.revalidate_after_index_disappears(NumericFull::new(test.ii.reader()));
    test.revalidate_after_document_deleted(NumericFull::new(test.ii.reader()));
}
