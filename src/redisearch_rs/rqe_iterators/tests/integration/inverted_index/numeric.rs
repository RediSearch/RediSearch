/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{IndexFlags_Index_StoreNumeric, RS_INVALID_FIELD_INDEX, t_docId, t_fieldIndex};
use field::FieldExpirationPredicate;
use inverted_index::{
    FilterNumericReader, InvertedIndex, NumericFilter, NumericReader, RSIndexResult,
};
use rqe_iterators::{RQEIterator, inverted_index::Numeric};

use crate::inverted_index::utils::{BaseTest, MockContext};

/// Builder for creating a Numeric iterator with optional parameters.
#[allow(dead_code)]
struct NumericBuilder<'index, R> {
    reader: R,
    context: NonNull<ffi::RedisSearchCtx>,
    index: t_fieldIndex,
    predicate: FieldExpirationPredicate,
    range_tree: Option<NonNull<ffi::NumericRangeTree>>,
    range_min: Option<f64>,
    range_max: Option<f64>,
    _marker: std::marker::PhantomData<&'index ()>,
}

#[allow(dead_code)]
impl<'index, R> NumericBuilder<'index, R>
where
    R: NumericReader<'index>,
{
    /// Create a new builder with the required parameters.
    ///
    /// All other parameters are optional and will use sensible defaults:
    /// - `field_index`: RS_INVALID_FIELD_INDEX
    /// - `predicate`: FieldExpirationPredicate::Default
    /// - `range_tree`: None
    /// - `range_min`: None
    /// - `range_max`: None
    fn new(reader: R, context: NonNull<ffi::RedisSearchCtx>) -> Self {
        Self {
            reader,
            context,
            index: RS_INVALID_FIELD_INDEX,
            predicate: FieldExpirationPredicate::Default,
            range_tree: None,
            range_min: None,
            range_max: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Set the field index for checking field expiration.
    fn field_index(mut self, index: t_fieldIndex) -> Self {
        self.index = index;
        self
    }

    /// Set the field expiration predicate.
    fn predicate(mut self, predicate: FieldExpirationPredicate) -> Self {
        self.predicate = predicate;
        self
    }

    /// Set the numeric range tree.
    fn range_tree(mut self, range_tree: NonNull<ffi::NumericRangeTree>) -> Self {
        self.range_tree = Some(range_tree);
        self
    }

    /// Set the minimum numeric range (for debug printing).
    fn range_min(mut self, range_min: f64) -> Self {
        self.range_min = Some(range_min);
        self
    }

    /// Set the maximum numeric range (for debug printing).
    fn range_max(mut self, range_max: f64) -> Self {
        self.range_max = Some(range_max);
        self
    }

    /// Build the Numeric iterator.
    fn build(self) -> Numeric<'index, R> {
        Numeric::new(
            self.reader,
            self.context,
            self.index,
            self.predicate,
            self.range_tree,
            self.range_min,
            self.range_max,
        )
    }
}

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

        NumericBuilder::new(reader, self.test.mock_ctx.sctx())
            .range_tree(self.test.mock_ctx.numeric_range_tree())
            .build()
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
    let mut it = NumericBuilder::new(reader, test.test.mock_ctx.sctx())
        .range_tree(test.test.mock_ctx.numeric_range_tree())
        .build();
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
    let mut it = NumericBuilder::new(reader, test.test.mock_ctx.sctx())
        .range_tree(test.test.mock_ctx.numeric_range_tree())
        .build();
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
    let mut it = NumericBuilder::new(ii.reader(), context.sctx())
        .range_tree(context.numeric_range_tree())
        .build();

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
    let mut it = NumericBuilder::new(ii.reader(), context.sctx())
        .range_tree(context.numeric_range_tree())
        .build();

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
    let mut it = NumericBuilder::new(reader, context.sctx())
        .range_tree(context.numeric_range_tree())
        .build();

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
    let mut it = NumericBuilder::new(reader, context.sctx())
        .range_tree(context.numeric_range_tree())
        .build();

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}

#[test]
fn numeric_range() {
    let ii = InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let context = MockContext::new(0, 0);

    let it = NumericBuilder::new(ii.reader(), context.sctx())
        .range_min(1.0)
        .range_max(10.0)
        .build();
    assert_eq!(it.range_min(), 1.0);
    assert_eq!(it.range_max(), 10.0);
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{ExpirationTest, RevalidateIndexType, RevalidateTest};
    use rqe_iterators::RQEValidateStatus;

    struct NumericExpirationTest {
        test: ExpirationTest,
    }

    impl NumericExpirationTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            // The numeric record has a value of `doc_id * 2.0`.
            RSIndexResult::numeric(doc_id as f64 * 2.0).doc_id(doc_id)
        }

        fn new(n_docs: u64, multi: bool) -> Self {
            Self {
                test: ExpirationTest::numeric(Box::new(Self::expected_record), n_docs, multi),
            }
        }

        fn create_iterator(
            &self,
        ) -> Numeric<'_, inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>>
        {
            let reader = self.test.numeric_inverted_index().reader();
            let field_index = self.test.context.field_spec().index;

            NumericBuilder::new(reader, self.test.context.sctx)
                .field_index(field_index)
                .range_tree(self.test.context.numeric_range_tree())
                .build()
        }

        fn test_read_expiration(&mut self) {
            let field_index = self.test.context.field_spec().index;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(field_index));

            let mut it = self.create_iterator();
            self.test.read(&mut it);
        }

        fn test_skip_to_expiration(&mut self) {
            let field_index = self.test.context.field_spec().index;
            // Make every even document ID field expired
            let even_ids = self
                .test
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0)
                .copied()
                .collect();

            self.test
                .mark_index_expired(even_ids, field::FieldMaskOrIndex::Index(field_index));

            let mut it = self.create_iterator();
            self.test.skip_to(&mut it);
        }
    }

    #[test]
    fn numeric_read_expiration() {
        NumericExpirationTest::new(10, false).test_read_expiration();
    }

    #[test]
    fn numeric_read_skip_multi_expiration() {
        NumericExpirationTest::new(10, true).test_read_expiration();
    }

    #[test]
    fn numeric_skip_to_expiration() {
        NumericExpirationTest::new(10, false).test_skip_to_expiration();
    }

    #[test]
    fn numeric_skip_to_expiration_multi() {
        NumericExpirationTest::new(10, true).test_skip_to_expiration();
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

        fn create_iterator(
            &self,
        ) -> Numeric<'_, inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>>
        {
            let ii = self.test.context.numeric_inverted_index().as_numeric();
            let context = &self.test.context;
            let fs = context.field_spec();

            NumericBuilder::new(ii.reader(), context.sctx)
                .field_index(fs.index)
                .range_tree(context.numeric_range_tree())
                .build()
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
        let ii = test.test.context.numeric_inverted_index().as_numeric();

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }
}
