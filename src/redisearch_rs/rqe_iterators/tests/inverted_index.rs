/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags, IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{
    Encoder, FilterNumericReader, InvertedIndex, NumericFilter, RSIndexResult, numeric::Numeric,
};
use rqe_iterators::{RQEIterator, SkipToOutcome, inverted_index::NumericFull};

mod c_mocks;

/// Test basic read and skip_to functionality for a given iterator.
struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
}

impl<E: Encoder + Default> BaseTest<E> {
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
    fn read<'index, I>(&self, it: &mut I)
    where
        I: RQEIterator<'index>,
    {
        let expected_record = &*self.expected_record;
        let mut i = 0;

        for _ in 0..=self.doc_ids.len() {
            let result = it.read();
            match result {
                Ok(Some(record)) => {
                    assert_eq!(record, &expected_record(record.doc_id));
                    assert_eq!(it.last_doc_id(), self.doc_ids[i]);
                    assert!(!it.at_eof());
                    i += 1;
                }
                _ => break,
            }
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

        // try reading at eof
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    /// Test skip_to functionality for a given iterator.
    ///
    /// Since the index contains only ODD doc IDs (1, 3, 5, 7, ...), when we skip to an EVEN doc ID,
    /// we expect `NotFound` with the next odd doc ID returned.
    fn skip_to<'index, I>(&self, it: &mut I)
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

                assert_eq!(record, &expected_record(id));
                assert_eq!(it.last_doc_id(), id);
                i += 1;
            }
            // Now test skipping to the exact doc ID that exists in the index.
            it.rewind();
            let res = it.skip_to(id);
            // Expect Found because `id` is an odd number that exists in the index.
            let Ok(Some(SkipToOutcome::Found(record))) = res else {
                panic!("skip_to {id} should succeed with Found: {res:?}");
            };
            assert_eq!(record, &expected_record(id));
            assert_eq!(it.last_doc_id(), id);
            i += 1;
        }

        // Test reading after skipping to the last id
        assert!(matches!(it.read(), Ok(None)));
        let last_doc_id = it.last_doc_id();
        assert!(matches!(it.skip_to(last_doc_id + 1), Ok(None)));
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

struct NumericTest {
    test: BaseTest<Numeric>,
}

impl NumericTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        // The numeric record has a value of `doc_id * 2.0`.
        RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_StoreNumeric,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }
}

#[test]
/// test reading from NumericFull iterator
fn numeric_full_read() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = NumericFull::new(reader);
    test.test.read(&mut it);

    // same but using a passthrough filter
    let test = NumericTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = NumericFull::new(reader);
    test.test.read(&mut it);
}

#[test]
/// test skipping from NumericFull iterator
fn numeric_full_skip_to() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = NumericFull::new(reader);
    test.test.skip_to(&mut it);
}
