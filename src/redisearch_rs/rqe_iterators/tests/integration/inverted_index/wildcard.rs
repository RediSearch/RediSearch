/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the wildcard inverted index iterator.

use ffi::{IndexFlags_Index_DocIdsOnly, RS_FIELDMASK_ALL, t_docId};
use inverted_index::{RSIndexResult, doc_ids_only::DocIdsOnly};
use rqe_iterators::{IteratorType, RQEIterator, inverted_index::Wildcard};

use crate::inverted_index::utils::BaseTest;

pub struct WildcardBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl WildcardBaseTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        RSIndexResult::build_virt()
            .doc_id(doc_id)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .weight(1.0)
            .build()
    }

    pub(crate) fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_DocIdsOnly,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    pub(crate) fn create_iterator(&self) -> Wildcard<'_, DocIdsOnly> {
        Wildcard::new(self.test.ii.reader(), 1.0)
    }
}

#[test]
fn wildcard_type() {
    let test = WildcardBaseTest::new(10);
    let it = test.create_iterator();
    assert_eq!(it.type_(), IteratorType::InvIdxWildcard);
}

#[test]
fn wildcard_read() {
    let test = WildcardBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
fn wildcard_skip_to() {
    let test = WildcardBaseTest::new(10);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
fn wildcard_empty_index() {
    let ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);

    let mut it = Wildcard::new(ii.reader(), 1.0);

    // Should immediately be at EOF
    assert!(it.read().expect("read failed").is_none());
    assert!(it.at_eof());
}

#[cfg(not(miri))]
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{RevalidateIndexType, RevalidateTest};
    use inverted_index::opaque::OpaqueEncoding;
    use rqe_iterators::RQEValidateStatus;

    struct WildcardRevalidateTest {
        test: RevalidateTest,
    }

    impl WildcardRevalidateTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            RSIndexResult::build_virt()
                .doc_id(doc_id)
                .field_mask(RS_FIELDMASK_ALL)
                .frequency(1)
                .weight(1.0)
                .build()
        }

        fn new(n_docs: u64) -> Self {
            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Wildcard,
                    Box::new(Self::expected_record),
                    n_docs,
                ),
            }
        }

        fn create_iterator(&self) -> Wildcard<'_, DocIdsOnly> {
            let ii = DocIdsOnly::from_opaque(self.test.context.wildcard_inverted_index());
            // SAFETY: `self.test.context` provides a valid `RedisSearchCtx` with a valid
            // `spec` and `existingDocs` that outlive the returned iterator.
            Wildcard::new(ii.reader(), 1.0)
        }
    }

    #[test]
    fn wildcard_revalidate_basic() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn wildcard_revalidate_at_eof() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn wildcard_revalidate_after_index_disappears() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let sctx = test.test.context.spec;

        // Verify the iterator works normally and read at least one document
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );
        assert!(it.read().expect("failed to read").is_some());
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Simulate existingDocs being garbage collected and recreated by
        // pointing spec.existingDocs to a different inverted index.
        let new_ii = Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
            inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));
        let old_existing_docs;
        unsafe {
            let spec = test.test.context.spec.as_ptr();
            old_existing_docs = (*spec).existingDocs;
            (*spec).existingDocs = new_ii.cast();
        }

        // Revalidate should return Aborted because existingDocs no longer
        // points to the same index the reader was created from.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // Restore original existingDocs and free the temporary index for
        // proper cleanup.
        unsafe {
            let spec = test.test.context.spec.as_ptr();
            (*spec).existingDocs = old_existing_docs;
            drop(Box::from_raw(new_ii));
        }
    }

    #[test]
    fn wildcard_revalidate_after_document_deleted() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.wildcard_inverted_index());

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }

    /// Test that revalidation returns `Aborted` when `existingDocs` is set to
    /// NULL, simulating the garbage collector removing all documents.
    #[test]
    fn wildcard_revalidate_after_existing_docs_nulled() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // Read at least one document so the iterator has a position.
        assert!(it.read().expect("failed to read").is_some());
        let sctx = test.test.context.spec;
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Simulate the garbage collector setting existingDocs to NULL after
        // collecting all documents.
        let old_existing_docs;
        unsafe {
            let spec = test.test.context.spec.as_ptr();
            old_existing_docs = (*spec).existingDocs;
            (*spec).existingDocs = std::ptr::null_mut();
        }

        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // Restore for proper cleanup.
        unsafe {
            let spec = test.test.context.spec.as_ptr();
            (*spec).existingDocs = old_existing_docs;
        }
    }

    /// Test that `reader()` returns a reference to the underlying reader.
    #[test]
    fn wildcard_reader_accessor() {
        let test = WildcardRevalidateTest::new(10);
        let it = test.create_iterator();

        let reader = it.reader();
        let ii = DocIdsOnly::from_opaque(test.test.context.wildcard_inverted_index());
        assert!(reader.points_to_ii(ii));
    }
}
