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
    Encoder, InvertedIndex, RSIndexResult, RSResultKind, test_utils::TermRecordCompare,
};
use rqe_iterators::{RQEIterator, SkipToOutcome};
use rqe_iterators_test_utils::MockContext;

/// Test basic read and skip_to functionality for a given iterator.
pub(super) struct BaseTest<E> {
    doc_ids: Vec<t_docId>,
    pub(super) ii: InvertedIndex<E>,
    expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
    pub(super) mock_ctx: MockContext,
}

/// assert that both records are equal
pub fn check_record(record: &RSIndexResult, expected: &RSIndexResult) {
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

        let mock_ctx = MockContext::new(0, 0);

        Self {
            doc_ids,
            ii,
            expected_record,
            mock_ctx,
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

#[cfg(not(miri))]
// Those tests rely on ffi calls which are not supported in miri.
pub(super) mod not_miri {
    use super::*;
    use ffi::{
        IndexFlags, IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
        IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets,
        IndexFlags_Index_WideSchema, t_docId,
    };
    use field::FieldMaskOrIndex;
    use inverted_index::{DecodedBy, Encoder, InvertedIndex, RSIndexResult};
    use rqe_iterators::{ExpirationChecker, RQEIterator, RQEValidateStatus, SkipToOutcome};
    use std::collections::HashSet;

    // Re-export TestContext and GlobalGuard from the main library's test_utils module
    pub use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    /// ---------- Expiration Tests ----------

    /// A mock expiration checker for testing.
    ///
    /// This allows testing expiration logic without requiring TTL tables.
    /// The `ExpirationTest` will mark documents as expired in this mock checker
    /// instead of in the TTL tables.
    #[derive(Debug, Clone)]
    pub struct MockExpirationChecker {
        expired_docs: HashSet<t_docId>,
    }

    impl MockExpirationChecker {
        pub fn new(expired_docs: HashSet<t_docId>) -> Self {
            Self { expired_docs }
        }

        pub fn mark_expired(&mut self, doc_id: t_docId) {
            self.expired_docs.insert(doc_id);
        }
    }

    impl ExpirationChecker for MockExpirationChecker {
        fn has_expiration(&self) -> bool {
            !self.expired_docs.is_empty()
        }

        fn is_expired(&self, result: &RSIndexResult) -> bool {
            self.expired_docs.contains(&result.doc_id)
        }
    }

    /// The type of index used in the expiration test.
    enum ExpirationIndexType {
        Numeric,
        Term,
        TermWide,
    }

    /// Test fields expiration using TestContext's inverted index.
    /// Supports both numeric and term index types.
    /// Uses a `MockExpirationChecker` instead of TTL tables for expiration tracking.
    pub struct ExpirationTest {
        pub(crate) doc_ids: Vec<t_docId>,
        expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
        pub(crate) context: TestContext,
        index_type: ExpirationIndexType,
        mock_checker: MockExpirationChecker,
        _guard: GlobalGuard,
    }

    impl ExpirationTest {
        /// Create a new numeric expiration test.
        pub(crate) fn numeric(
            expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
            n_docs: u64,
            multi: bool,
        ) -> Self {
            let create_record = &*expected_record;
            // Generate a set of document IDs for testing.
            let doc_ids = (1..=n_docs).map(|i| i as t_docId).collect::<Vec<_>>();

            // Create a TestContext which creates the inverted index via NumericRangeTree
            let context = TestContext::numeric(doc_ids.iter().map(|id| create_record(*id)), multi);

            Self {
                doc_ids,
                expected_record,
                context,
                index_type: ExpirationIndexType::Numeric,
                mock_checker: MockExpirationChecker::new(HashSet::new()),
                _guard: GlobalGuard::default(),
            }
        }

        /// Create a new term expiration test.
        pub(crate) fn term(
            ii_flags: IndexFlags,
            expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
            n_docs: u64,
            multi: bool,
        ) -> Self {
            let create_record = &*expected_record;
            // Generate a set of document IDs for testing.
            let doc_ids = (1..=n_docs).map(|i| i as t_docId).collect::<Vec<_>>();

            // Create a TestContext which creates the inverted index
            let context =
                TestContext::term(ii_flags, doc_ids.iter().map(|id| create_record(*id)), multi);

            let index_type = if (ii_flags & IndexFlags_Index_WideSchema) != 0 {
                ExpirationIndexType::TermWide
            } else {
                ExpirationIndexType::Term
            };

            Self {
                doc_ids,
                expected_record,
                context,
                index_type,
                mock_checker: MockExpirationChecker::new(HashSet::new()),
                _guard: GlobalGuard::default(),
            }
        }

        /// Get the numeric inverted index from the TestContext.
        /// Panics if this is not a numeric expiration test.
        pub(crate) fn numeric_inverted_index(
            &self,
        ) -> &mut inverted_index::InvertedIndex<inverted_index::numeric::Numeric> {
            use inverted_index::{numeric::Numeric, opaque::OpaqueEncoding};
            Numeric::from_mut_opaque(self.context.numeric_inverted_index()).inner_mut()
        }

        /// Get the term inverted index from the TestContext (non-wide).
        /// Panics if this is not a term expiration test or if it uses wide schema.
        pub(crate) fn term_inverted_index(
            &self,
        ) -> &inverted_index::FieldMaskTrackingIndex<inverted_index::full::Full> {
            self.context.term_inverted_index()
        }

        /// Get the term inverted index from the TestContext (wide schema).
        /// Panics if this is not a term expiration test or if it doesn't use wide schema.
        pub(crate) fn term_inverted_index_wide(
            &self,
        ) -> &inverted_index::FieldMaskTrackingIndex<inverted_index::full::FullWide> {
            self.context.term_inverted_index_wide()
        }

        /// Get the text field bit from the TestContext.
        pub(crate) fn text_field_bit(&self) -> ffi::t_fieldMask {
            self.context.text_field_bit()
        }

        /// Create a mock expiration checker.
        /// Returns a clone of the internal mock checker with all currently expired documents.
        pub(crate) fn create_mock_checker(&self) -> MockExpirationChecker {
            self.mock_checker.clone()
        }

        /// Get the number of unique documents in the index.
        fn unique_docs(&self) -> u32 {
            match self.index_type {
                ExpirationIndexType::Numeric => self.numeric_inverted_index().unique_docs(),
                ExpirationIndexType::Term => self.term_inverted_index().unique_docs(),
                ExpirationIndexType::TermWide => self.term_inverted_index_wide().unique_docs(),
            }
        }

        /// Mark the index as expired for the given document IDs.
        /// This updates the mock expiration checker instead of the TTL tables.
        pub(crate) fn mark_index_expired(&mut self, ids: Vec<t_docId>, _field: FieldMaskOrIndex) {
            for id in ids {
                self.mock_checker.mark_expired(id);
            }
        }

        /// test read of expired documents.
        pub(crate) fn read<'index, I>(&self, it: &mut I)
        where
            I: RQEIterator<'index>,
        {
            let expected_record = &*self.expected_record;

            for doc_id in self.doc_ids.iter().step_by(2).copied() {
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
            assert_eq!(it.num_estimated(), self.unique_docs() as usize);

            // try reading at eof
            assert!(matches!(it.read(), Ok(None)));
            assert!(it.at_eof());
        }

        /// test skip_to on expired documents.
        pub(crate) fn skip_to<'index, I>(&self, it: &mut I)
        where
            I: RQEIterator<'index>,
        {
            let expected_record = &*self.expected_record;

            let last_id = self.doc_ids.last().copied().unwrap();

            // Skip to odd IDs should work
            let odd_ids = self.doc_ids.iter().filter(|id| **id % 2 != 0).copied();
            for doc_id in odd_ids {
                let record = match it
                    .skip_to(doc_id)
                    .expect("skip_to failed")
                    .expect("expected result not eof")
                {
                    SkipToOutcome::Found(res) => res,
                    SkipToOutcome::NotFound(_) => panic!("Document not found"),
                };

                check_record(record, &expected_record(doc_id));
                assert_eq!(it.last_doc_id(), doc_id);
                assert_eq!(it.current().unwrap().doc_id, doc_id);
                assert!(!it.at_eof());
            }

            // Test skipping to even IDs - should skip to next odd ID
            it.rewind();

            let even_ids = self
                .doc_ids
                .iter()
                .filter(|id| **id % 2 == 0 && **id != last_id)
                .copied();
            for doc_id in even_ids {
                let record = match it
                    .skip_to(doc_id)
                    .expect("skip_to failed")
                    .expect("expected result not eof")
                {
                    SkipToOutcome::Found(_) => panic!("Should not find even ID"),
                    SkipToOutcome::NotFound(res) => res,
                };

                check_record(record, &expected_record(doc_id + 1));
                assert_eq!(it.last_doc_id(), doc_id + 1);
                assert_eq!(it.current().unwrap().doc_id, doc_id + 1);
                assert!(!it.at_eof());
            }

            if last_id % 2 == 0 {
                // the last id is odd, so trying to skip to it should move to eof
                assert!(it.skip_to(last_id).expect("skip_to failed").is_none());
                assert!(it.at_eof());
            }

            // iterator has reached eof
            assert!(it.skip_to(last_id + 1).expect("skip_to failed").is_none());

            // Test skipping to ID beyond range
            it.rewind();
            assert!(it.skip_to(last_id + 1).expect("skip_to failed").is_none());
        }
    }

    /// ---------- Revalidate Tests ----------

    pub enum RevalidateIndexType {
        Numeric,
        Term,
        Wildcard,
    }

    /// Test the revalidation of the iterator.
    pub struct RevalidateTest {
        #[allow(dead_code)]
        doc_ids: Vec<t_docId>,
        pub context: TestContext,
        _guard: GlobalGuard,
    }

    impl RevalidateTest {
        pub fn new(
            index_type: RevalidateIndexType,
            expected_record: Box<dyn Fn(t_docId) -> RSIndexResult<'static>>,
            n_docs: u64,
        ) -> Self {
            // Generate a set of odd document IDs for testing, starting from 1.
            let doc_ids = (0..=n_docs)
                .map(|i| (2 * i + 1) as t_docId)
                .collect::<Vec<_>>();

            let context = match index_type {
                RevalidateIndexType::Numeric => {
                    TestContext::numeric(doc_ids.iter().map(|id| expected_record(*id)), false)
                }
                RevalidateIndexType::Term => {
                    let flags = IndexFlags_Index_StoreFreqs
                        | IndexFlags_Index_StoreTermOffsets
                        | IndexFlags_Index_StoreFieldFlags
                        | IndexFlags_Index_StoreByteOffsets;
                    TestContext::term(flags, doc_ids.iter().map(|id| expected_record(*id)), false)
                }
                RevalidateIndexType::Wildcard => TestContext::wildcard(doc_ids.iter().copied()),
            };

            Self {
                doc_ids,
                context,
                _guard: GlobalGuard::default(),
            }
        }

        /// test basic revalidation functionality - should return `RQEValidateStatus::Ok`` when index is valid
        pub fn revalidate_basic<'index, I>(&self, it: &mut I)
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
        pub fn revalidate_at_eof<'index, I>(&self, it: &mut I)
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

        /// Remove the document with the given id from the inverted index.
        pub fn remove_document<E: Encoder + DecodedBy>(
            &self,
            ii: &mut InvertedIndex<E>,
            doc_id: t_docId,
        ) {
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
        pub fn revalidate_after_document_deleted<'index, I, E: Encoder + DecodedBy>(
            &self,
            it: &mut I,
            ii: &mut InvertedIndex<E>,
        ) where
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
            self.remove_document(ii, self.doc_ids[0]);
            assert_eq!(
                it.revalidate().expect("revalidate failed"),
                RQEValidateStatus::Ok
            );
            assert_eq!(it.last_doc_id(), self.doc_ids[2]);
            assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

            // Remove an element after the current iteration position.
            self.remove_document(ii, self.doc_ids[4]);
            assert_eq!(
                it.revalidate().expect("revalidate failed"),
                RQEValidateStatus::Ok
            );
            assert_eq!(it.last_doc_id(), self.doc_ids[2]);
            assert_eq!(it.current().unwrap().doc_id, self.doc_ids[2]);

            // Remove the element at the current position of the iterator.
            // When validating we won't be able to skip to this element, so we should get RQEValidateStatus::Moved.
            self.remove_document(ii, self.doc_ids[2]);
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

            self.remove_document(ii, last_doc_id);
            // revalidate should return Moved without current doc and be at EOF.
            let res = it.revalidate().expect("revalidate failed");
            assert!(matches!(res, RQEValidateStatus::Moved { current: None }));
            assert!(it.at_eof());
        }
    }
}

#[cfg(not(miri))]
pub use not_miri::{ExpirationTest, MockExpirationChecker, RevalidateIndexType, RevalidateTest};
