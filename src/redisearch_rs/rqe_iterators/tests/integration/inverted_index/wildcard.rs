/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the wildcard inverted index iterator.

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::doc_ids_only::DocIdsOnly;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, RQEIterator, inverted_index::Wildcard};

use crate::inverted_index::utils::BaseTest;

pub struct WildcardBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl WildcardBaseTest {
    fn expected_record(doc_id: DocId) -> RSIndexResult<'static> {
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
    use ffi::{ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK};
    use inverted_index::opaque::OpaqueEncoding;
    use rqe_iterators_test_utils::revalidate_via_resume;

    struct WildcardRevalidateTest {
        test: RevalidateTest,
    }

    impl WildcardRevalidateTest {
        fn expected_record(doc_id: DocId) -> RSIndexResult<'static> {
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
        let it = test.create_iterator();
        test.test.revalidate_basic(Box::new(it));
    }

    #[test]
    fn wildcard_revalidate_at_eof() {
        let test = WildcardRevalidateTest::new(10);
        let it = test.create_iterator();
        test.test.revalidate_at_eof(Box::new(it));
    }

    #[test]
    fn wildcard_revalidate_after_index_disappears() {
        let test = WildcardRevalidateTest::new(10);
        let it = Box::new(test.create_iterator());

        // Verify the iterator works normally and read at least one document
        let guard = test.test.context.spec_read();
        let (mut it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ValidateStatus_VALIDATE_OK);
        assert!(it.read().expect("failed to read").is_some());
        let (it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ValidateStatus_VALIDATE_OK);

        // Simulate existingDocs being garbage collected and recreated by
        // pointing spec.existingDocs to a different inverted index.
        let new_ii = Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
            inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));
        let old_existing_docs = test.test.context.spec_read().existing_docs_ptr();
        test.test
            .context
            .spec_write()
            .set_existing_docs_ptr(new_ii.cast());

        // Revalidate should return Aborted because existingDocs no longer
        // points to the same index the reader was created from.
        let (_it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ValidateStatus_VALIDATE_ABORTED);

        // Restore original existingDocs and free the temporary index for
        // proper cleanup.
        test.test
            .context
            .spec_write()
            .set_existing_docs_ptr(old_existing_docs);
        unsafe {
            drop(Box::from_raw(new_ii));
        }
    }

    #[test]
    fn wildcard_revalidate_after_document_deleted() {
        let test = WildcardRevalidateTest::new(10);
        let it = test.create_iterator();
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.wildcard_inverted_index());

        test.test
            .revalidate_after_document_deleted(Box::new(it), ii);
    }

    /// Test that revalidation returns `Aborted` when `existingDocs` is set to
    /// NULL, simulating the garbage collector removing all documents.
    #[test]
    fn wildcard_revalidate_after_existing_docs_nulled() {
        let test = WildcardRevalidateTest::new(10);
        let mut it = Box::new(test.create_iterator());

        // Read at least one document so the iterator has a position.
        assert!(it.read().expect("failed to read").is_some());
        let guard = test.test.context.spec_read();
        let (it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ValidateStatus_VALIDATE_OK);

        // Simulate the garbage collector setting existingDocs to NULL after
        // collecting all documents.
        let old_existing_docs = test.test.context.spec_read().existing_docs_ptr();
        test.test
            .context
            .spec_write()
            .set_existing_docs_ptr(std::ptr::null_mut());

        let (_it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ValidateStatus_VALIDATE_ABORTED);

        // Restore for proper cleanup.
        test.test
            .context
            .spec_write()
            .set_existing_docs_ptr(old_existing_docs);
    }
}
