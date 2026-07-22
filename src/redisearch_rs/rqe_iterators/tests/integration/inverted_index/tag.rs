/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the tag inverted index iterator.

use ffi::IndexFlags_Index_DocIdsOnly;
use index_result::RSIndexResult;
use inverted_index::doc_ids_only::DocIdsOnly;
use query_term::RSQueryTerm;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, NoOpChecker, RQEIterator, inverted_index::Tag};
use rqe_iterators_test_utils::MockContext;

use crate::inverted_index::utils::BaseTest;

use tag_index::TrieLookup;

struct TagBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl TagBaseTest {
    fn expected_record(doc_id: DocId) -> RSIndexResult<'static> {
        RSIndexResult::build_term()
            .doc_id(doc_id)
            .field_mask(RS_FIELDMASK_ALL)
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

    fn create_term() -> Box<RSQueryTerm> {
        RSQueryTerm::new("test_tag", 0, 0)
    }

    fn create_iterator(&self) -> Tag<'_, DocIdsOnly, TrieLookup, NoOpChecker> {
        let reader = self.test.ii.reader();
        let term = Self::create_term();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
        // that outlives the returned iterator. The TagIndex is an empty index
        // which is fine since NoOpChecker doesn't trigger revalidation
        // lookups, and `should_abort` is not called in basic tests.
        unsafe {
            Tag::new(
                reader,
                self.test.mock_ctx.sctx(),
                TrieLookup::new(self.test.mock_ctx.tag_index()),
                term,
                0.0,
                NoOpChecker,
            )
        }
    }
}

#[test]
fn tag_type() {
    let test = TagBaseTest::new(10);
    let it = test.create_iterator();
    assert_eq!(it.type_(), IteratorType::InvIdxTag);
}

#[test]
fn tag_read() {
    let test = TagBaseTest::new(100);
    let mut it = test.create_iterator();
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
fn tag_skip_to() {
    let test = TagBaseTest::new(10);
    let mut it = test.create_iterator();
    test.test.skip_to(&mut it);
}

#[test]
fn tag_empty_index() {
    let ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    let mock_ctx = MockContext::new(0, 0);

    let reader = ii.reader();
    let term = RSQueryTerm::new("test_tag", 0, 0);
    // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
    // that outlives the iterator.
    let mut it = unsafe {
        Tag::new(
            reader,
            mock_ctx.sctx(),
            TrieLookup::new(mock_ctx.tag_index()),
            term,
            0.0,
            NoOpChecker,
        )
    };

    // Should immediately be at EOF
    assert!(it.read().expect("read failed").is_none());
    assert!(it.at_eof());
}

#[cfg(not(miri))]
// Creating the [`rqe_iterators_test_utils::TestContext`] requires ffi calls which are not supported by miri.
mod not_miri {
    use super::*;
    use crate::inverted_index::utils::{RevalidateIndexType, RevalidateTest};
    use rqe_iterators::RQEValidateStatus;

    struct TagRevalidateTest {
        test: RevalidateTest,
    }

    impl TagRevalidateTest {
        fn expected_record(doc_id: DocId) -> RSIndexResult<'static> {
            RSIndexResult::build_term()
                .doc_id(doc_id)
                .field_mask(RS_FIELDMASK_ALL)
                .build()
        }

        fn new(n_docs: u64) -> Self {
            Self {
                test: RevalidateTest::new(
                    RevalidateIndexType::Tag,
                    Box::new(Self::expected_record),
                    n_docs,
                ),
            }
        }

        fn create_iterator(&self) -> Tag<'_, DocIdsOnly, TrieLookup, NoOpChecker> {
            let ii = self.test.context.tag_inverted_index();
            let tag_index = self.test.context.tag_index();
            let term = RSQueryTerm::new("test_tag", 0, 0);
            // SAFETY: `self.test.context` provides a valid `RedisSearchCtx` with a valid
            // `spec` and `TagIndex` that outlive the returned iterator.
            unsafe {
                Tag::new(
                    ii.reader(),
                    self.test.context.sctx,
                    TrieLookup::new(tag_index),
                    term,
                    0.0,
                    NoOpChecker,
                )
            }
        }
    }

    #[test]
    fn tag_revalidate_basic() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn tag_revalidate_at_eof() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn tag_revalidate_after_index_disappears() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // Verify the iterator works normally and read at least one document
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);
        assert!(it.read().expect("failed to read").is_some());
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Simulate the tag's inverted index being garbage collected and
        // recreated by replacing the value stored for "test_tag" with a new,
        // empty inverted index. `insert_empty_tag` drops the old inverted
        // index; the iterator's reader holds a (now-dangling) raw pointer to it,
        // but `should_abort` only compares pointers via `points_to_ii`
        // (`std::ptr::eq`) without dereferencing it.
        let mut tag_index = test.test.context.tag_index();
        // SAFETY: `tag_index` points to the `TagIndex` owned by `context`, which
        // outlives this call. The iterator is not read between here and the
        // revalidation below, per the revalidation protocol.
        unsafe { tag_index.as_mut() }.insert_empty_tag(b"test_tag");

        // Revalidate should return Aborted because the tag II no longer
        // points to the same index the reader was created from.
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);
    }

    #[test]
    fn tag_revalidate_after_document_deleted() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let mut tag_index = test.test.context.tag_index();
        // SAFETY: `tag_index` points to the `TagIndex` owned by `context`, which
        // outlives this borrow; the iterator only reads it between revalidations,
        // per the revalidation protocol driven by the helper below.
        let ii = unsafe { tag_index.as_mut() }
            .find_value_mut(b"test_tag")
            .expect("test_tag should be indexed");

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }

    /// Test that revalidation returns `Aborted` when the tag value is removed
    /// from the TagIndex's TrieMap, simulating the garbage collector removing
    /// all documents for this tag.
    #[test]
    fn tag_revalidate_after_triemap_entry_removed() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();

        // Read at least one document so the iterator has a position.
        assert!(it.read().expect("failed to read").is_some());
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Simulate the garbage collector removing the tag's inverted index
        // by deleting the value stored for "test_tag". `delete_tag_value` drops
        // the inverted index; the iterator's reader holds a (now-dangling) raw
        // pointer to it, but `should_abort` sees the tag is missing and returns
        // true without dereferencing the reader.
        let mut tag_index = test.test.context.tag_index();
        // SAFETY: `tag_index` points to the `TagIndex` owned by `context`, which
        // outlives this call. The iterator is not read during the mutation, per
        // the revalidation protocol.
        unsafe { tag_index.as_mut() }.delete_tag_value(b"test_tag");

        // `should_abort` sees the tag value is missing and returns true.
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);
    }

    /// Test that `reader()` returns a reference to the underlying reader.
    #[test]
    fn tag_reader_accessor() {
        let test = TagRevalidateTest::new(10);
        let it = test.create_iterator();

        let reader = it.reader();
        let ii = test.test.context.tag_inverted_index();
        assert!(reader.points_to_ii(ii));
    }
}
