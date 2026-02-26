/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{IndexFlags_Index_StoreNumeric, t_docId};
use inverted_index::{
    FilterNumericReader, IndexReader, InvertedIndex, NumericFilter, NumericReader, RSIndexResult,
};
use rqe_iterators::{
    NoOpChecker, RQEIterator, RQEValidateStatus, SkipToOutcome, inverted_index::Numeric,
};

use crate::inverted_index::utils::BaseTest;
use rqe_iterators_test_utils::MockContext;

/// Builder for creating a Numeric iterator with optional parameters.
#[allow(dead_code)]
struct NumericBuilder<'index, R, E = NoOpChecker> {
    reader: R,
    range_tree: Option<NonNull<ffi::NumericRangeTree>>,
    range_min: Option<f64>,
    range_max: Option<f64>,
    expiration_checker: E,
    _marker: std::marker::PhantomData<&'index ()>,
}

#[allow(dead_code)]
impl<'index, R> NumericBuilder<'index, R, NoOpChecker>
where
    R: NumericReader<'index>,
{
    /// Create a new builder with the required parameters.
    ///
    /// All other parameters are optional and will use sensible defaults:
    /// - `range_tree`: None
    /// - `range_min`: None
    /// - `range_max`: None
    /// - `expiration_checker`: NoOpChecker
    fn new(reader: R) -> Self {
        Self {
            reader,
            range_tree: None,
            range_min: None,
            range_max: None,
            expiration_checker: NoOpChecker,
            _marker: std::marker::PhantomData,
        }
    }
}

#[allow(dead_code)]
impl<'index, R, E> NumericBuilder<'index, R, E>
where
    R: NumericReader<'index>,
    E: rqe_iterators::ExpirationChecker,
{
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

    /// Set the expiration checker.
    fn expiration_checker<E2: rqe_iterators::ExpirationChecker>(
        self,
        checker: E2,
    ) -> NumericBuilder<'index, R, E2> {
        NumericBuilder {
            reader: self.reader,
            range_tree: self.range_tree,
            range_min: self.range_min,
            range_max: self.range_max,
            expiration_checker: checker,
            _marker: std::marker::PhantomData,
        }
    }

    /// Build the Numeric iterator.
    fn build(self) -> Numeric<'index, R, E> {
        // SAFETY: `range_tree`, when provided, is a valid pointer to a
        // `NumericRangeTree` that outlives the returned iterator.
        unsafe {
            Numeric::new(
                self.reader,
                self.expiration_checker,
                self.range_tree,
                self.range_min,
                self.range_max,
            )
        }
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
    ) -> Numeric<
        '_,
        inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>,
        NoOpChecker,
    > {
        let reader = self.test.ii.reader();

        NumericBuilder::new(reader)
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
    let mut it = NumericBuilder::new(reader)
        .range_tree(test.test.mock_ctx.numeric_range_tree())
        .build();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
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
    let mut it = NumericBuilder::new(reader)
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
    let mut it = NumericBuilder::new(ii.reader())
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
    let mut it = NumericBuilder::new(ii.reader())
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
    let mut it = NumericBuilder::new(reader)
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
    let mut it = NumericBuilder::new(reader)
        .range_tree(context.numeric_range_tree())
        .build();

    // Attempt to skip to the first entry, expecting EOF since no entries match the filter
    assert_eq!(it.skip_to(1).expect("skip_to failed"), None);
}

#[test]
fn numeric_range() {
    let ii = InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);

    let it = NumericBuilder::new(ii.reader())
        .range_min(1.0)
        .range_max(10.0)
        .build();
    assert_eq!(it.range_min(), 1.0);
    assert_eq!(it.range_max(), 10.0);

    // Default range values when not explicitly set.
    let it = NumericBuilder::new(ii.reader()).build();
    assert_eq!(it.range_min(), f64::NEG_INFINITY);
    assert_eq!(it.range_max(), f64::INFINITY);
}

/// Test that read correctly skips remaining duplicates after skip_to lands
/// on a doc with multiple entries in a multi-value index.
#[test]
fn skip_to_then_read_with_duplicates() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    // Add multiple entries with the same docId (triggers HasMultiValue flag).
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(10.0).doc_id(5));

    let context = MockContext::new(0, 0);
    let mut it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Skip to doc 1 — should find it.
    let res = it.skip_to(1).expect("skip_to failed");
    let Some(SkipToOutcome::Found(record)) = res else {
        panic!("expected Found for doc 1, got {res:?}");
    };
    assert_eq!(record.doc_id, 1);

    // Read should skip the remaining duplicate entries for doc 1 and return doc 5.
    let record = it.read().expect("read failed").expect("expected a result");
    assert_eq!(record.doc_id, 5);

    // No more docs.
    assert_eq!(it.read().expect("read failed"), None);
    assert!(it.at_eof());
}

/// Test the `reader()` accessor on the Numeric iterator.
#[test]
fn numeric_reader_accessor() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(3));

    let context = MockContext::new(0, 0);
    let it = NumericBuilder::new(ii.reader())
        .range_tree(context.numeric_range_tree())
        .build();

    // Verify the reader is accessible and reports correct unique doc count.
    assert_eq!(it.reader().unique_docs(), 2);
}

/// Test `should_abort` returns false when no range tree is provided.
#[test]
fn numeric_no_range_tree_revalidate() {
    let mut ii =
        InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
    let _ = ii.add_record(&RSIndexResult::numeric(1.0).doc_id(1));
    let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(3));

    // Build without a range tree — should_abort will return false.
    let mut it = NumericBuilder::new(ii.reader()).build();

    // Read one doc to advance the iterator.
    let record = it.read().expect("read failed").expect("expected a result");
    assert_eq!(record.doc_id, 1);

    // Revalidate should succeed (not abort) even though there is no range tree.
    assert_eq!(
        it.revalidate().expect("revalidate failed"),
        RQEValidateStatus::Ok
    );
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{
        ExpirationTest, MockExpirationChecker, RevalidateIndexType, RevalidateTest,
    };
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
        ) -> Numeric<
            '_,
            inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>,
            MockExpirationChecker,
        > {
            let reader = self.test.numeric_inverted_index().reader();
            let checker = self.test.create_mock_checker();

            // SAFETY: `numeric_range_tree()` returns a valid pointer that
            // outlives the returned iterator.
            unsafe {
                Numeric::new(
                    reader,
                    checker,
                    Some(self.test.context.numeric_range_tree()),
                    None,
                    None,
                )
            }
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

    /// Test that skip_to on a non-existent doc ID where the next doc found is
    /// NOT expired returns NotFound via the `skip_to_check_expiration` path.
    /// Exercises the NotFound branch when the seeked doc is not expired.
    #[test]
    fn numeric_skip_to_non_existent_with_expiration() {
        use crate::inverted_index::utils::MockExpirationChecker;
        use std::collections::HashSet;

        // Create docs with IDs 1, 3, 5, 7 (gaps at 2, 4, 6).
        let mut ii =
            InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
        let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
        let _ = ii.add_record(&RSIndexResult::numeric(6.0).doc_id(3));
        let _ = ii.add_record(&RSIndexResult::numeric(10.0).doc_id(5));
        let _ = ii.add_record(&RSIndexResult::numeric(14.0).doc_id(7));

        // Mark doc 1 as expired
        let mut expired_docs = HashSet::new();
        expired_docs.insert(1);
        let checker = MockExpirationChecker::new(expired_docs);

        let context = MockContext::new(0, 0);
        let mut it = NumericBuilder::new(ii.reader())
            .range_tree(context.numeric_range_tree())
            .expiration_checker(checker)
            .build();

        // Skip to doc 2, which doesn't exist. The seeker finds doc 3
        // (the next available), which is NOT expired.
        // This exercises skip_to_check_expiration's NotFound branch for non-expired docs.
        let res = it.skip_to(2).expect("skip_to failed");
        let Some(SkipToOutcome::NotFound(record)) = res else {
            panic!("expected NotFound for doc 2, got {res:?}");
        };
        assert_eq!(record.doc_id, 3);
        assert_eq!(it.last_doc_id(), 3);
    }

    /// Test that `has_expiration` returns false when using an empty expiration checker.
    /// This simulates the case where expiration checking is disabled.
    #[test]
    fn numeric_no_expiration_with_invalid_field_index() {
        use crate::inverted_index::utils::MockExpirationChecker;
        use std::collections::HashSet;

        // Create docs with IDs 1, 2, 3.
        let mut ii =
            InvertedIndex::<inverted_index::numeric::Numeric>::new(IndexFlags_Index_StoreNumeric);
        let _ = ii.add_record(&RSIndexResult::numeric(2.0).doc_id(1));
        let _ = ii.add_record(&RSIndexResult::numeric(4.0).doc_id(2));
        let _ = ii.add_record(&RSIndexResult::numeric(6.0).doc_id(3));

        // Use an empty MockExpirationChecker (has_expiration returns false)
        // to simulate RS_INVALID_FIELD_INDEX behavior.
        let checker = MockExpirationChecker::new(HashSet::new());

        let context = MockContext::new(0, 0);
        let mut it = NumericBuilder::new(ii.reader())
            .range_tree(context.numeric_range_tree())
            .expiration_checker(checker)
            .build();

        // Since expiration checking is disabled (has_expiration returns false),
        // we should see all docs including doc 1.
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 1);
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 2);
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 3);
        assert_eq!(it.read().expect("read failed"), None);
    }

    /// Test that revalidation with `last_doc_id == 0` returns Ok even when
    /// the underlying index has been modified (needs_revalidation is true).
    /// Exercises the `last_doc_id == 0` early return in `revalidate`.
    #[test]
    fn numeric_revalidate_needs_revalidation_before_reads() {
        let test = NumericRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = {
            use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
            Numeric::from_mut_opaque(test.test.context.numeric_inverted_index()).inner_mut()
        };

        // Trigger GC on the index so needs_revalidation() returns true.
        test.test.remove_document(ii, 1);

        // Revalidate before any reads. last_doc_id is 0, so even though
        // needs_revalidation is true, we should get Ok.
        assert_eq!(
            it.revalidate().expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // The iterator should still work — doc 1 was removed, so first doc is 3.
        let record = it.read().expect("read failed").expect("expected a result");
        assert_eq!(record.doc_id, 3);
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
        ) -> Numeric<
            '_,
            inverted_index::IndexReaderCore<'_, inverted_index::numeric::Numeric>,
            NoOpChecker,
        > {
            let ii = {
                use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
                Numeric::from_mut_opaque(self.test.context.numeric_inverted_index()).inner_mut()
            };
            let context = &self.test.context;

            NumericBuilder::new(ii.reader())
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
        let ii = {
            use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
            Numeric::from_mut_opaque(test.test.context.numeric_inverted_index()).inner_mut()
        };

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }
}
