/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{IndexFlags_Index_StoreNumeric, RS_INVALID_FIELD_INDEX, t_docId};
use field::FieldExpirationPredicate;
use inverted_index::{FilterNumericReader, InvertedIndex, NumericFilter, RSIndexResult};
use rqe_iterators::{RQEIterator, inverted_index::Numeric};

use crate::inverted_index::utils::{BaseTest, MockContext};

struct NumericBaseTest {
    test: BaseTest<inverted_index::numeric::Numeric>,
}

impl NumericBaseTest {
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

    fn create_iterator(
        &self,
    ) -> Numeric<'_, inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>> {
        let reader = self.test.ii.reader();

        Numeric::new(
            reader,
            self.test.mock_ctx.sctx(),
            RS_INVALID_FIELD_INDEX,
            FieldExpirationPredicate::Default,
            self.test.mock_ctx.numeric_range_tree(),
        )
    }
}

#[test]
/// test reading from Numeric iterator
fn numeric_read() {
    let test = NumericBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());

    // same but using a passthrough filter
    let test = NumericBaseTest::new(100);
    let filter = NumericFilter::default();
    let reader = test.test.ii.reader();
    let reader = FilterNumericReader::new(&filter, reader);
    let mut it = Numeric::new(
        reader,
        test.test.mock_ctx.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        test.test.mock_ctx.numeric_range_tree(),
    );
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
/// test skipping from Numeric iterator
fn numeric_skip_to() {
    let test = NumericBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
/// test reading from Numeric iterator with a filter
fn numeric_filter() {
    let test = NumericBaseTest::new(100);
    let filter = NumericFilter {
        min: 50.0,
        max: 75.0,
        ..Default::default()
    };
    let reader = FilterNumericReader::new(&filter, test.test.ii.reader());
    let mut it = Numeric::new(
        reader,
        test.test.mock_ctx.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        test.test.mock_ctx.numeric_range_tree(),
    );
    let docs_ids = test
        .test
        .docs_ids_iter()
        // records have a numeric value of twice their doc id
        .filter(|id| *id * 2 >= 50 && *id * 2 <= 75);
    test.test.read(&mut it, docs_ids);
}

#[test]
fn skip_multi_id() {
    // Add multiple entries with the same docId
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(3.0).doc_id(1));

    let context = MockContext::new(0, 0);
    let mut it = Numeric::new(
        ii.reader(),
        context.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        context.numeric_range_tree(),
    );

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

    let context = MockContext::new(0, 0);
    let mut it = Numeric::new(
        ii.reader(),
        context.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        context.numeric_range_tree(),
    );

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

    let context = MockContext::new(0, 0);
    let mut it = Numeric::new(
        reader,
        context.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        context.numeric_range_tree(),
    );

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
    let context = MockContext::new(0, 0);
    let mut it = Numeric::new(
        reader,
        context.sctx(),
        RS_INVALID_FIELD_INDEX,
        FieldExpirationPredicate::Default,
        context.numeric_range_tree(),
    );

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{ExpirationTest, RevalidateIndexType, RevalidateTest};
    use ffi::t_fieldIndex;
    use field::FieldExpirationPredicate;
    use rqe_iterators::RQEValidateStatus;

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

        fn create_iterator(
            &self,
            index: t_fieldIndex,
        ) -> Numeric<'_, inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>>
        {
            let reader = self.test.ii.reader();

            Numeric::new(
                reader,
                self.test.mock_ctx.sctx(),
                index,
                FieldExpirationPredicate::Default,
                self.test.mock_ctx.numeric_range_tree(),
            )
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

            let mut it = self.create_iterator(FIELD_INDEX);
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

            let mut it = self.create_iterator(FIELD_INDEX);
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

    struct NumericRevalidateTest {
        test: RevalidateTest,
    }

    impl NumericRevalidateTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            // The numeric record has a value of `doc_id * 2.0`.
            RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
        }

        fn new(n_docs: u64) -> Self {
            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Numeric,
                    Box::new(Self::expected_record),
                    n_docs,
                ),
            }
        }

        fn inverted_index(
            &self,
        ) -> &mut inverted_index::InvertedIndex<inverted_index::numeric::Numeric> {
            let context = &self.test.context;

            // Create a numeric filter to find ranges
            let mut filter = NumericFilter::default();
            filter.ascending = false;
            filter.field_spec = context.field_spec();

            // Find a range that covers our data to get the inverted index
            let ranges = unsafe {
                ffi::NumericRangeTree_Find(
                    context.numeric_range_tree().as_ptr(),
                    // cast inverted_index::NumericFilter to ffi::NumericFilter
                    &filter as *const _ as *const ffi::NumericFilter,
                )
            };
            assert!(!ranges.is_null());
            unsafe {
                assert!(ffi::Vector_Size(ranges) > 0);
            }
            let mut range: *mut ffi::NumericRange = std::ptr::null_mut();
            unsafe {
                let range_out = &mut range as *mut *mut ffi::NumericRange;
                assert!(ffi::Vector_Get(ranges, 0, range_out.cast()) == 1);
            }
            assert!(!range.is_null());
            let range = unsafe { &*range };
            let ii = range.entries;
            assert!(!ii.is_null());
            let ii: *mut inverted_index_ffi::InvertedIndex = ii.cast();
            let ii = unsafe { &mut *ii };

            unsafe {
                ffi::Vector_Free(ranges);
            }

            match ii {
                inverted_index_ffi::InvertedIndex::Numeric(entries) => entries.inner_mut(),
                _ => panic!("Unexpected inverted index type"),
            }
        }

        fn create_iterator(
            &self,
        ) -> Numeric<'_, inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>>
        {
            let ii = self.inverted_index();
            let context = &self.test.context;
            let fs = context.field_spec();

            rqe_iterators::Numeric::new(
                ii.reader(),
                context.sctx,
                fs.index,
                field::FieldExpirationPredicate::Default,
                context.numeric_range_tree(),
            )
        }
    }

    #[test]
    fn numeric_revalidate_basic() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn numeric_revalidate_at_eof() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn numeric_revalidate_after_index_disappears() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // First, verify the iterator works normally and read at least one document
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // For numeric iterators, we can simulate index disappearance by
        // manipulating the revision ID. check_abort() compares the stored
        // revision ID with the current one from the NumericRangeTree.
        let context = &test.test.context;
        let mut rt = context.numeric_range_tree();
        // Simulate the range tree being modified by incrementing its revision ID
        // This simulates a scenario where the tree was modified (e.g., node split, removal)
        // while the iterator was suspended.
        unsafe {
            let rt = rt.as_mut();
            rt.revisionId += 1;
        }
        // Now Revalidate should return Aborted because the revision IDs don't match
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // Restore the original revision ID for proper cleanup
        unsafe {
            let rt = rt.as_mut();
            rt.revisionId -= 1;
        }
    }

    #[test]
    fn numeric_revalidate_after_document_deleted() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = test.inverted_index();

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }
}
