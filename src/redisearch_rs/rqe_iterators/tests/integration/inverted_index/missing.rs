/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the missing-field inverted index iterator.

use ffi::{IndexFlags_Index_DocIdsOnly, RS_FIELDMASK_ALL, t_docId};
use inverted_index::{RSIndexResult, doc_ids_only::DocIdsOnly};
use rqe_iterators::{IteratorType, NoOpChecker, RQEIterator, inverted_index::Missing};
use rqe_iterators_test_utils::MockContext;

use crate::inverted_index::utils::BaseTest;

struct MissingBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl MissingBaseTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
        RSIndexResult::build_virt()
            .doc_id(doc_id)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .weight(0.0)
            .build()
    }

    fn new(n_docs: u64) -> Self {
        Self {
            test: BaseTest::new(
                IndexFlags_Index_DocIdsOnly,
                Box::new(Self::expected_record),
                n_docs,
            ),
        }
    }

    fn create_iterator(&self) -> Missing<'_, DocIdsOnly, NoOpChecker> {
        let reader = self.test.ii.reader();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
        // that outlives the returned iterator. field_index is 0 (unused with NoOpChecker).
        unsafe { Missing::new(reader, self.test.mock_ctx.sctx(), 0, NoOpChecker) }
    }
}

#[test]
fn missing_type() {
    let test = MissingBaseTest::new(10);
    let it = test.create_iterator();
    assert_eq!(it.type_(), IteratorType::InvIdxMissing);
}

#[test]
fn missing_read() {
    let test = MissingBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
fn missing_skip_to() {
    let test = MissingBaseTest::new(10);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
fn missing_empty_index() {
    let ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let mock_ctx = MockContext::new(0, 0);

    let reader = ii.reader();
    // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
    // that outlives the iterator.
    let mut it = unsafe { Missing::new(reader, mock_ctx.sctx(), 0, NoOpChecker) };

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
    use std::ffi::CStr;

    struct MissingRevalidateTest {
        test: RevalidateTest,
    }

    impl MissingRevalidateTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
            RSIndexResult::build_virt()
                .doc_id(doc_id)
                .field_mask(RS_FIELDMASK_ALL)
                .frequency(1)
                .weight(0.0)
                .build()
        }

        fn new(n_docs: u64) -> Self {
            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Missing,
                    Box::new(Self::expected_record),
                    n_docs,
                ),
            }
        }

        fn create_iterator(&self) -> Missing<'_, DocIdsOnly, NoOpChecker> {
            let ii = DocIdsOnly::from_opaque(self.test.context.missing_inverted_index());
            let field_index = self.test.context.field_spec().index;
            // SAFETY: `self.test.context` provides a valid `RedisSearchCtx` with a valid
            // `spec` and `missingFieldDict` that outlive the returned iterator.
            unsafe {
                Missing::new(
                    ii.reader(),
                    self.test.context.sctx,
                    field_index,
                    NoOpChecker,
                )
            }
        }
    }

    #[test]
    fn missing_revalidate_basic() {
        let test = MissingRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn missing_revalidate_at_eof() {
        let test = MissingRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn missing_revalidate_after_index_disappears() {
        let test = MissingRevalidateTest::new(10);
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

        // Simulate the missing-field inverted index being garbage collected and
        // recreated by replacing the dict entry with a new inverted index.
        // We create the replacement via `Box::into_raw(Box::new(...))` using
        // `inverted_index::opaque::InvertedIndex`, which is the same type that
        // `InvertedIndex_Free` (the dict's value destructor) expects.
        let new_ii = Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
            inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));
        let field_name = test.test.context.field_spec().fieldName;

        // Replace the dict entry. `dictDelete` calls the value destructor
        // which frees the original inverted index. Then add the new one.
        // Note: the iterator's reader holds a (now-dangling) pointer to the
        // original II, but `should_abort` only compares pointers via
        // `is_index` without dereferencing it, so this is safe.
        unsafe {
            let dict = (*test.test.context.spec.as_ptr()).missingFieldDict;
            ffi::RS_dictDelete(dict, field_name as *mut _);
            let rc = ffi::RS_dictAdd(dict, field_name as *mut _, new_ii as *mut _);
            assert_eq!(rc, 0, "dictAdd failed");
        }

        // Revalidate should return Aborted because the missing II no longer
        // points to the same index the reader was created from.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // No restore needed: the new II will be freed by `dictRelease` during
        // `IndexSpec_RemoveFromGlobals` in `TestContext::drop`.
    }

    #[test]
    fn missing_revalidate_after_document_deleted() {
        let test = MissingRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.missing_inverted_index());

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }

    /// Test that revalidation returns `Aborted` when the missing-field inverted
    /// index is removed from the dict (entry deleted), simulating the garbage
    /// collector removing all documents.
    #[test]
    fn missing_revalidate_after_dict_entry_removed() {
        let test = MissingRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // Read at least one document so the iterator has a position.
        assert!(it.read().expect("failed to read").is_some());
        let sctx = test.test.context.spec;
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Simulate the garbage collector removing the missing-field index
        // by deleting the dict entry. `dictDelete` calls the value destructor
        // which frees the inverted index.
        let field_name = test.test.context.field_spec().fieldName;
        unsafe {
            let dict = (*test.test.context.spec.as_ptr()).missingFieldDict;
            ffi::RS_dictDelete(dict, field_name as *mut _);
        }

        // `should_abort` sees NULL from `dictFetchValue` and returns true.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // No restore needed: the entry was properly freed by `dictDelete`.
        // `TestContext::drop` calls `dictRelease` which is fine with a
        // missing entry.
    }

    /// Test that `reader()` returns a reference to the underlying reader.
    #[test]
    fn missing_reader_accessor() {
        let test = MissingRevalidateTest::new(10);
        let it = test.create_iterator();

        let reader = it.reader();
        let ii = DocIdsOnly::from_opaque(test.test.context.missing_inverted_index());
        assert!(reader.points_to_ii(ii));
    }

    #[test]
    fn missing_field_name() {
        let test = MissingRevalidateTest::new(10);
        let it = test.create_iterator();

        let (field_name, field_name_len) = it.field_name();
        // SAFETY: `field_name()` returns a valid pointer to the field name stored in the live spec.
        let field_name = unsafe { CStr::from_ptr(field_name) };

        assert_eq!(field_name.to_bytes().len(), field_name_len);
        assert_eq!(field_name.to_bytes(), b"text_field");
    }
}
