/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{FilterNumericReader, InvertedIndex, NumericFilter, RSIndexResult};
use rqe_iterators::{RQEIterator, inverted_index::Numeric};

use crate::inverted_index::utils::{BaseTest, RevalidateTest};

struct NumericTest {
    test: BaseTest<inverted_index::numeric::Numeric>,
    revalidate_test: RevalidateTest<inverted_index::numeric::Numeric>,
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
            revalidate_test: RevalidateTest::new(
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
    let mut it = Numeric::new(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());

    // same but using a passthrough filter
    let test = NumericTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = Numeric::new(reader);
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Numeric iterator
fn numeric_full_skip_to() {
    let test = NumericTest::new(100);
    let reader = test.test.ii.reader();
    let mut it = Numeric::new(reader);
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Numeric iterator with a filter
fn numeric_filter() {
    let test = NumericTest::new(100);
    let filter = NumericFilter {
        min: 50.0,
        max: 75.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, test.test.ii.reader());
    let mut it = Numeric::new(reader);
    let docs_ids = test
        .test
        .docs_ids_iter()
        // records have a numeric value of twice their doc id
        .filter(|id| *id * 2 >= 50 && *id * 2 <= 75);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn numeric_full_revalidate_basic() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test.revalidate_basic(&mut it);
}

#[test]
fn numeric_full_revalidate_at_eof() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test.revalidate_at_eof(&mut it);
}

#[test]
fn numeric_full_revalidate_after_index_disappears() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test
        .revalidate_after_index_disappears(&mut it, true);
}

#[cfg(not(miri))] // Miri does not like UnsafeCell
#[test]
fn numeric_full_revalidate_after_document_deleted() {
    let test = NumericTest::new(10);
    let reader = unsafe { (*test.revalidate_test.ii.get()).reader() };
    let mut it = Numeric::new(reader);
    test.revalidate_test
        .revalidate_after_document_deleted(&mut it);
}

#[test]
fn skip_multi_id() {
    // Add multiple entries with the same docId
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(3.0).doc_id(1));

    let mut it = Numeric::new(ii.reader());

    // Read the first entry. Expect to get the entry with value 1.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(1.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn skip_multi_id_and_value() {
    // Add multiple entries with the same docId and numeric value
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));

    let mut it = Numeric::new(ii.reader());

    // Read the first entry. Expect to get the entry with value 1.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(1.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn get_correct_value() {
    // Add entries with the same ID but different values
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(3.0).doc_id(1));

    // Create an iterator that reads only entries with value >= 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 3.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());
    let mut it = Numeric::new(reader);

    // Read the first entry. Expect to get the entry with value 2.0
    let record = it
        .read()
        .expect("failed to read")
        .expect("expected result not eof");
    assert_eq!(record.doc_id, 1);
    assert_eq!(record.as_numeric(), Some(2.0));
    assert_eq!(it.last_doc_id(), 1);
    assert!(!it.at_eof());

    // Read the next entry. Expect EOF since we have only one unique docId with value 2.0
    assert_eq!(it.read().unwrap(), None);
    assert!(it.at_eof());
}

#[test]
fn eof_after_filtering() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);

    // Fill the index with entries, all with value 1.0
    for id in 1..=1234 {
        let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(id));
    }

    // Create an iterator that reads only entries with value 2.0
    let filter = NumericFilter {
        min: 2.0,
        max: 2.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, ii.reader());
    let mut it = Numeric::new(reader);

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::ExpirationTest;
    use ffi::t_fieldIndex;
    use field::FieldExpirationPredicate;

    struct NumericExpirationTest {
        test: ExpirationTest<inverted_index::numeric::Numeric>,
    }

    impl NumericExpirationTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            // The numeric record has a value of `doc_id * 2.0`.
            RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
        }

        fn new(n_docs: u64, multi: bool) -> Self {
            Self {
                test: ExpirationTest::new(
                    IndexFlags_Index_StoreNumeric,
                    Box::new(Self::expected_record),
                    n_docs,
                    multi,
                ),
            }
        }

        fn test_read_expiration(&mut self) {
            const FIELD_INDEX: t_fieldIndex = 42;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(FIELD_INDEX));

            let reader = self.test.ii.reader();
            let mut it = Numeric::with_context(
                reader,
                self.test.context(),
                FIELD_INDEX,
                FieldExpirationPredicate::Default,
            );

            self.test.read(&mut it);
        }

        fn test_skip_to_expiration(&mut self) {
            const FIELD_INDEX: t_fieldIndex = 42;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(FIELD_INDEX));

            let reader = self.test.ii.reader();
            let mut it = Numeric::with_context(
                reader,
                self.test.context(),
                FIELD_INDEX,
                FieldExpirationPredicate::Default,
            );

            self.test.skip_to(&mut it);
        }
    }

    #[test]
    fn numeric_read_expiration() {
        NumericExpirationTest::new(100, false).test_read_expiration();
    }

    #[test]
    fn numeric_read_skip_multi_expiration() {
        NumericExpirationTest::new(100, true).test_read_expiration();
    }

    #[test]
    fn numeric_skip_to_expiration() {
        NumericExpirationTest::new(100, false).test_skip_to_expiration();
    }
}
