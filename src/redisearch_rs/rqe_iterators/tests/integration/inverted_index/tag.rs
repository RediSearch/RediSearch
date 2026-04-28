/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the tag inverted index iterator.

use ffi::{IndexFlags_Index_DocIdsOnly, RS_FIELDMASK_ALL, t_docId};
use inverted_index::{RSIndexResult, doc_ids_only::DocIdsOnly};
use query_term::RSQueryTerm;
use rqe_iterators::{IteratorType, NoOpChecker, RQEIterator, inverted_index::Tag};
use rqe_iterators_test_utils::MockContext;

use crate::inverted_index::utils::BaseTest;

struct TagBaseTest {
    test: BaseTest<DocIdsOnly>,
}

impl TagBaseTest {
    fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
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

    fn create_iterator(&self) -> Tag<'_, DocIdsOnly, NoOpChecker> {
        let reader = self.test.ii.reader();
        let term = Self::create_term();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
        // that outlives the returned iterator. The TagIndex pointer points to a
        // zeroed struct which is fine since NoOpChecker doesn't trigger revalidation
        // lookups, and `should_abort` is not called in basic tests.
        unsafe {
            Tag::new(
                reader,
                self.test.mock_ctx.sctx(),
                self.test.mock_ctx.tag_index(),
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
            mock_ctx.tag_index(),
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
    use inverted_index::opaque::OpaqueEncoding;
    use rqe_iterators::RQEValidateStatus;
    use std::ffi::c_void;

    struct TagRevalidateTest {
        test: RevalidateTest,
    }

    impl TagRevalidateTest {
        fn expected_record(doc_id: t_docId) -> RSIndexResult<'static> {
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

        fn create_iterator(&self) -> Tag<'_, DocIdsOnly, NoOpChecker> {
            let ii = DocIdsOnly::from_opaque(self.test.context.tag_inverted_index());
            let tag_index = self.test.context.tag_index();
            let term = RSQueryTerm::new("test_tag", 0, 0);
            // SAFETY: `self.test.context` provides a valid `RedisSearchCtx` with a valid
            // `spec` and `TagIndex` that outlive the returned iterator.
            unsafe {
                Tag::new(
                    ii.reader(),
                    self.test.context.sctx,
                    tag_index,
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

        // Simulate the tag's inverted index being garbage collected and
        // recreated by replacing the TrieMap entry with a new inverted index.
        let new_ii = Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
            inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
        )));

        // Save the old II pointer so we can free it after the test.
        let old_ii: *mut inverted_index::opaque::InvertedIndex =
            (test.test.context.tag_inverted_index() as *mut inverted_index::opaque::InvertedIndex)
                .cast();

        let tag_index = test.test.context.tag_index();

        // Delete the old entry then add the new one.
        // The iterator's reader holds a (now-dangling) raw pointer to the
        // original II, but `should_abort` only compares pointers via
        // `points_to_ii` (`std::ptr::eq`) without dereferencing it.
        // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
        // is a valid TrieMap.
        let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::opaque::TrieMap>() };
        let old_val = trie.remove(b"test_tag");
        assert!(old_val.is_some(), "test_tag should exist in the TrieMap");
        let prev = trie.insert(b"test_tag", new_ii as *mut c_void);
        assert!(prev.is_none(), "insert should return None for new entry");

        // Revalidate should return Aborted because the tag II no longer
        // points to the same index the reader was created from.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // SAFETY: `old_ii` was allocated by `NewInvertedIndex_Ex` (via `Box::new`)
        // and has not been freed. We are the sole owner after removing it from the TrieMap.
        // The new II will be freed when the TrieMap is freed during TagIndex cleanup.
        unsafe { drop(Box::from_raw(old_ii)) };
    }

    #[test]
    fn tag_revalidate_after_document_deleted() {
        let test = TagRevalidateTest::new(10);
        let mut it = test.create_iterator();
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.tag_inverted_index());

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
        let sctx = test.test.context.spec;
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Ok
        );

        // Save the old II pointer so we can free it after the test.
        let old_ii: *mut inverted_index::opaque::InvertedIndex =
            (test.test.context.tag_inverted_index() as *mut inverted_index::opaque::InvertedIndex)
                .cast();

        // Simulate the garbage collector removing the tag's inverted index
        // by deleting the TrieMap entry.
        let tag_index = test.test.context.tag_index();
        // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
        // is a valid TrieMap.
        let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::opaque::TrieMap>() };
        let old_val = trie.remove(b"test_tag");
        assert!(old_val.is_some(), "test_tag should exist in the TrieMap");

        // `should_abort` sees the tag value is missing and returns true.
        // SAFETY: test-only call with valid context
        assert_eq!(
            unsafe { it.revalidate(sctx) }.expect("revalidate failed"),
            RQEValidateStatus::Aborted
        );

        // SAFETY: `old_ii` was allocated by `NewInvertedIndex_Ex` (via `Box::new`)
        // and has not been freed. We are the sole owner after removing it from the TrieMap.
        unsafe { drop(Box::from_raw(old_ii)) };
    }

    /// Test that `reader()` returns a reference to the underlying reader.
    #[test]
    fn tag_reader_accessor() {
        let test = TagRevalidateTest::new(10);
        let it = test.create_iterator();

        let reader = it.reader();
        let ii = DocIdsOnly::from_opaque(test.test.context.tag_inverted_index());
        assert!(reader.points_to_ii(ii));
    }
}
