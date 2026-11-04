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
use index_spec::IndexSpecReadGuard;
use inverted_index::doc_ids_only::DocIdsOnly;
use query_term::RSQueryTerm;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, NoOpChecker, RQEIterator, inverted_index::Tag};
use rqe_iterators_test_utils::{ContractChecker, MockContext};

use crate::inverted_index::utils::BaseTest;

use iterators_ffi::inverted_index::CTagIndexLookup;

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

    fn create_iterator(&self) -> Tag<'_, DocIdsOnly, CTagIndexLookup, NoOpChecker> {
        let reader = self.test.ii.reader();
        let term = Self::create_term();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
        // that outlives the returned iterator. The TagIndex pointer points to a
        // zeroed struct which is fine since NoOpChecker doesn't trigger revalidation
        // lookups, and basic tests never revalidate.
        unsafe {
            Tag::new(
                reader,
                self.test.mock_ctx.sctx(),
                CTagIndexLookup::new(self.test.mock_ctx.tag_index()),
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
    let it = ContractChecker::new(test.create_iterator());
    assert_eq!(it.type_(), IteratorType::InvIdxTag);
}

#[test]
fn tag_read() {
    let test = TagBaseTest::new(100);
    let mut it = ContractChecker::new(test.create_iterator());
    test.test.read(&mut it, test.test.docs_ids_iter());
}

#[test]
fn tag_skip_to() {
    let test = TagBaseTest::new(10);
    let mut it = ContractChecker::new(test.create_iterator());
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
    let mut it = ContractChecker::new(unsafe {
        Tag::new(
            reader,
            mock_ctx.sctx(),
            CTagIndexLookup::new(mock_ctx.tag_index()),
            term,
            0.0,
            NoOpChecker,
        )
    });

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

        fn create_iterator(&self) -> Tag<'_, DocIdsOnly, CTagIndexLookup, NoOpChecker> {
            let ii = DocIdsOnly::from_opaque(self.test.context.tag_inverted_index());
            let tag_index = self.test.context.tag_index();
            let term = RSQueryTerm::new("test_tag", 0, 0);
            // SAFETY: `self.test.context` provides a valid `RedisSearchCtx` with a valid
            // `spec` and `TagIndex` that outlive the returned iterator.
            unsafe {
                Tag::new(
                    ii.reader(),
                    self.test.context.sctx,
                    CTagIndexLookup::new(tag_index),
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
        let mut it = ContractChecker::new(test.create_iterator());
        test.test.revalidate_basic(&mut it);
    }

    #[test]
    fn tag_revalidate_at_eof() {
        let test = TagRevalidateTest::new(10);
        let mut it = ContractChecker::new(test.create_iterator());
        test.test.revalidate_at_eof(&mut it);
    }

    #[test]
    fn tag_revalidate_at_eof_after_gc() {
        let test = TagRevalidateTest::new(10);
        let mut it = ContractChecker::new(test.create_iterator());
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.tag_inverted_index());

        test.test.revalidate_at_eof_after_gc(&mut it, ii);
    }

    #[test]
    fn tag_revalidate_after_index_disappears() {
        let test = TagRevalidateTest::new(10);
        let mut it = ContractChecker::new(test.create_iterator());

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
        // original II, but revalidation only compares it against the lookup's
        // result via `points_to_ii` (`std::ptr::eq`) without dereferencing it.
        // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
        // is a valid TrieMap.
        let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::TrieMapOpaque>() };
        let old_val = trie.remove(b"test_tag");
        assert!(old_val.is_some(), "test_tag should exist in the TrieMap");
        let prev = trie.insert(b"test_tag", new_ii as *mut c_void);
        assert!(prev.is_none(), "insert should return None for new entry");

        // Revalidate should return Aborted because the tag II no longer
        // points to the same index the reader was created from.
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);

        // SAFETY: `old_ii` was allocated by `NewInvertedIndex_Ex` (via `Box::new`)
        // and has not been freed. We are the sole owner after removing it from the TrieMap.
        // The new II will be freed when the TrieMap is freed during TagIndex cleanup.
        unsafe { drop(Box::from_raw(old_ii)) };
    }

    #[test]
    fn tag_revalidate_after_document_deleted() {
        let test = TagRevalidateTest::new(10);
        let mut it = ContractChecker::new(test.create_iterator());
        let ii = DocIdsOnly::from_mut_opaque(test.test.context.tag_inverted_index());

        test.test.revalidate_after_document_deleted(&mut it, ii);
    }

    /// Test that revalidation returns `Aborted` when the tag value is removed
    /// from the TagIndex's TrieMap, simulating the garbage collector removing
    /// all documents for this tag.
    #[test]
    fn tag_revalidate_after_triemap_entry_removed() {
        let test = TagRevalidateTest::new(10);
        let mut it = ContractChecker::new(test.create_iterator());

        // Read at least one document so the iterator has a position.
        assert!(it.read().expect("failed to read").is_some());
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Ok);

        // Save the old II pointer so we can free it after the test.
        let old_ii: *mut inverted_index::opaque::InvertedIndex =
            (test.test.context.tag_inverted_index() as *mut inverted_index::opaque::InvertedIndex)
                .cast();

        // Simulate the garbage collector removing the tag's inverted index
        // by deleting the TrieMap entry.
        let tag_index = test.test.context.tag_index();
        // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
        // is a valid TrieMap.
        let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::TrieMapOpaque>() };
        let old_val = trie.remove(b"test_tag");
        assert!(old_val.is_some(), "test_tag should exist in the TrieMap");

        // The lookup no longer resolves the tag value, so the iterator aborts.
        let status = it
            .revalidate(&*test.test.context.spec_read())
            .expect("revalidate failed");
        assert_eq!(status, RQEValidateStatus::Aborted);

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

    mod via_resume {
        use super::*;
        use crate::inverted_index::utils::via_resume::{
            revalidate_after_document_deleted, revalidate_at_eof, revalidate_at_eof_after_gc,
            revalidate_basic,
        };
        use rqe_iterators::{ResumeOutcome, TypeErasedRQEIterator};
        use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

        #[test]
        fn tag_revalidate_basic() {
            let test = TagRevalidateTest::new(10);
            let it = test.create_iterator();
            revalidate_basic(&test.test, Box::new(it));
        }

        #[test]
        fn tag_revalidate_at_eof() {
            let test = TagRevalidateTest::new(10);
            let it = test.create_iterator();
            revalidate_at_eof(&test.test, Box::new(it));
        }

        #[test]
        fn tag_revalidate_at_eof_after_gc() {
            let test = TagRevalidateTest::new(10);
            let it = test.create_iterator();
            let ii = DocIdsOnly::from_mut_opaque(test.test.context.tag_inverted_index());

            revalidate_at_eof_after_gc(&test.test, Box::new(it), ii);
        }

        #[test]
        fn tag_revalidate_after_index_disappears() {
            let test = TagRevalidateTest::new(10);
            let it = Box::new(test.create_iterator());

            // Verify the iterator works normally and read at least one document
            let guard = test.test.context.spec_read();
            let mut it = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test")
                .expect_ok();
            assert!(it.read().expect("failed to read").is_some());
            let it = revalidate_via_resume(it, &guard)
                .expect("resume should not fail in this test")
                .expect_ok();

            // Simulate the tag's inverted index being garbage collected and
            // recreated by replacing the TrieMap entry with a new inverted index.
            let new_ii =
                Box::into_raw(Box::new(inverted_index::opaque::InvertedIndex::DocIdsOnly(
                    inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
                )));

            // Save the old II pointer so we can free it after the test.
            let old_ii: *mut inverted_index::opaque::InvertedIndex =
                (test.test.context.tag_inverted_index()
                    as *mut inverted_index::opaque::InvertedIndex)
                    .cast();

            let tag_index = test.test.context.tag_index();

            // Delete the old entry then add the new one.
            // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
            // is a valid TrieMap.
            let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::TrieMapOpaque>() };
            let old_val = trie.remove(b"test_tag");
            assert!(old_val.is_some(), "test_tag should exist in the TrieMap");
            let prev = trie.insert(b"test_tag", new_ii as *mut c_void);
            assert!(prev.is_none(), "insert should return None for new entry");

            // Revalidate should return Aborted because the tag II no longer
            // points to the same index the reader was created from.
            let outcome =
                revalidate_via_resume(it, &guard).expect("resume should not fail in this test");
            assert!(matches!(outcome, ResumeOutcome::Aborted));

            // SAFETY: `old_ii` was allocated by `NewInvertedIndex_Ex` (via `Box::new`)
            // and has not been freed. We are the sole owner after removing it from the TrieMap.
            unsafe { drop(Box::from_raw(old_ii)) };
        }

        #[test]
        fn tag_revalidate_after_document_deleted() {
            let test = TagRevalidateTest::new(10);
            let it = test.create_iterator();
            let ii = DocIdsOnly::from_mut_opaque(test.test.context.tag_inverted_index());

            revalidate_after_document_deleted(&test.test, Box::new(it), ii);
        }

        /// Test that revalidation returns `Aborted` when the tag value is removed
        /// from the TagIndex's TrieMap, simulating the garbage collector removing
        /// all documents for this tag.
        #[test]
        fn tag_revalidate_after_triemap_entry_removed() {
            let test = TagRevalidateTest::new(10);
            let mut it = Box::new(test.create_iterator());

            // Read at least one document so the iterator has a position.
            assert!(it.read().expect("failed to read").is_some());
            let guard = test.test.context.spec_read();
            let it = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
                .expect("resume should not fail in this test")
                .expect_ok();

            // Save the old II pointer so we can free it after the test.
            let old_ii: *mut inverted_index::opaque::InvertedIndex =
                (test.test.context.tag_inverted_index()
                    as *mut inverted_index::opaque::InvertedIndex)
                    .cast();

            // Simulate the garbage collector removing the tag's inverted index
            // by deleting the TrieMap entry.
            let tag_index = test.test.context.tag_index();
            // SAFETY: `tag_index` is valid (created by `TagIndex_Ensure`), `values`
            // is a valid TrieMap.
            let trie = unsafe { &mut *tag_index.as_ref().values.cast::<trie_rs::TrieMapOpaque>() };
            let old_val = trie.remove(b"test_tag");
            assert!(old_val.is_some(), "test_tag should exist in the TrieMap");

            // The lookup no longer resolves the tag value, so the iterator aborts.
            let outcome =
                revalidate_via_resume(it, &guard).expect("resume should not fail in this test");
            assert!(matches!(outcome, ResumeOutcome::Aborted));

            // SAFETY: `old_ii` was allocated by `NewInvertedIndex_Ex` (via `Box::new`)
            // and has not been freed. We are the sole owner after removing it from the TrieMap.
            unsafe { drop(Box::from_raw(old_ii)) };
        }
    }
}

/// Resolves every tag to one inverted index through a raw pointer, the way the
/// production lookups reach the index without holding a reference across a
/// writer's `&mut`.
struct RawLookup(std::ptr::NonNull<inverted_index::InvertedIndex<DocIdsOnly>>);

impl rqe_iterators::inverted_index::TagLookup<DocIdsOnly> for RawLookup {
    fn find(&self, _tag: &[u8]) -> Option<&inverted_index::InvertedIndex<DocIdsOnly>> {
        // SAFETY: the tests below keep the index alive for as long as the lookup,
        // and only mutate it while the iterator is parked.
        Some(unsafe { self.0.as_ref() })
    }
}

/// Builds a tag iterator over the index `ii` points to, reads it up to
/// `parked_on`, then runs `write` on the index through a `&mut`, as a writer or
/// the garbage collector would while the query has released the spec lock.
///
/// The `&mut` invalidates the reference the reader was opened with, so the
/// iterator only stays sound if revalidation stops reading through it.
fn tag_parked_across_a_write<'index>(
    ii: *mut inverted_index::InvertedIndex<DocIdsOnly>,
    mock_ctx: &'index MockContext,
    parked_on: DocId,
    write: impl FnOnce(&mut inverted_index::InvertedIndex<DocIdsOnly>),
) -> Tag<'index, DocIdsOnly, RawLookup, NoOpChecker> {
    // SAFETY: `ii` is valid and not mutably aliased while the reader is built.
    let reader = unsafe { &*ii }.reader();
    // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec`
    // that outlives the iterator, and `ii` outlives the lookup.
    let mut it = unsafe {
        Tag::new(
            reader,
            mock_ctx.sctx(),
            RawLookup(std::ptr::NonNull::new(ii).unwrap()),
            RSQueryTerm::new("test_tag", 0, 0),
            0.0,
            NoOpChecker,
        )
    };
    for doc_id in 1..=parked_on {
        let record = it.read().expect("read failed").expect("doc expected");
        assert_eq!(record.doc_id, doc_id);
    }

    // SAFETY: the iterator is parked, per the revalidation protocol.
    write(unsafe { &mut *ii });

    it
}

fn new_tag_index(n_docs: DocId) -> *mut inverted_index::InvertedIndex<DocIdsOnly> {
    let mut ii = inverted_index::InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=n_docs {
        append(doc_id)(&mut ii);
    }
    Box::into_raw(Box::new(ii))
}

fn append(doc_id: DocId) -> impl FnOnce(&mut inverted_index::InvertedIndex<DocIdsOnly>) {
    move |ii| {
        ii.add_record(&TagBaseTest::expected_record(doc_id))
            .expect("append failed");
    }
}

/// Collects `doc_id` in place, the way a garbage-collection pass edits a posting
/// list without reallocating the index.
fn collect(doc_id: DocId) -> impl FnOnce(&mut inverted_index::InvertedIndex<DocIdsOnly>) {
    move |ii| {
        let delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &inverted_index::RepairContext<'_>)>,
            )
            .expect("scan_gc failed")
            .expect("the collected document is indexed, so a delta is produced");
        ii.apply_gc(delta);
    }
}

fn drain<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    let mut doc_ids = Vec::new();
    while let Some(record) = it.read().expect("read failed") {
        doc_ids.push(record.doc_id);
    }
    doc_ids
}

/// Revalidates `it`, checking it does not abort — the index it reads is still
/// the one the lookup resolves — and returns whether it kept its position
/// without a re-seek.
fn revalidate_in_place<'index>(
    it: &mut Tag<'index, DocIdsOnly, RawLookup, NoOpChecker>,
    mock_ctx: &MockContext,
) -> bool {
    let status = it
        .revalidate(&mock_ctx.spec_read())
        .expect("revalidate failed");
    assert!(
        !matches!(status, rqe_iterators::RQEValidateStatus::Aborted),
        "the index the reader holds is still registered, so it must not abort"
    );
    matches!(status, rqe_iterators::RQEValidateStatus::Ok)
}

/// A reader revalidated after a write to its index must not read through the
/// pointer it was opened with: the writer's `&mut` invalidated it, even though
/// the index kept its address and identity. Run under Miri to catch it.
#[test]
fn tag_revalidate_after_write_reads_through_a_live_pointer() {
    const N: DocId = 4;
    let mock_ctx = MockContext::new(N, N as usize);
    let ii = new_tag_index(N);

    let mut it = tag_parked_across_a_write(ii, &mock_ctx, 2, append(N + 1));
    revalidate_in_place(&mut it, &mock_ctx);
    assert_eq!(drain(&mut it), (3..=N + 1).collect::<Vec<_>>());

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}

/// As [`tag_revalidate_after_write_reads_through_a_live_pointer`], but the write
/// lands in a later block than the one the reader is parked in, so revalidation
/// finds nothing to re-seek and the reader reads on from the block buffer it
/// cached before the write.
#[test]
fn tag_revalidate_after_write_to_a_later_block_reads_on() {
    use inverted_index::Encoder;

    // One past a full block, so the index spans two and the reader parks in the
    // first while the append lands in the second.
    let n = DocId::from(DocIdsOnly::RECOMMENDED_BLOCK_ENTRIES) + 1;
    let mock_ctx = MockContext::new(n, n as usize);
    let ii = new_tag_index(n);

    let mut it = tag_parked_across_a_write(ii, &mock_ctx, 2, append(n + 1));
    assert!(
        revalidate_in_place(&mut it, &mock_ctx),
        "a write past the reader's block must not force a re-seek"
    );
    assert_eq!(drain(&mut it), (3..=n + 1).collect::<Vec<_>>());

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}

/// As [`tag_revalidate_after_write_reads_through_a_live_pointer`], with an
/// in-place garbage-collection pass as the write.
#[test]
fn tag_revalidate_after_gc_reads_through_a_live_pointer() {
    const N: DocId = 4;
    let mock_ctx = MockContext::new(N, N as usize);
    let ii = new_tag_index(N);

    let mut it = tag_parked_across_a_write(ii, &mock_ctx, 2, collect(3));
    revalidate_in_place(&mut it, &mock_ctx);
    assert_eq!(drain(&mut it), vec![4]);

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}

/// The suspend/resume counterpart of [`revalidate_in_place`]: suspends and
/// resumes `it`, checking it does not abort, and returns it with whether it kept
/// its position without a re-seek.
fn resume_parked<'index>(
    it: Tag<'index, DocIdsOnly, RawLookup, NoOpChecker>,
    guard: &IndexSpecReadGuard<'index>,
) -> (rqe_iterators::TypeErasedRQEIterator<'index>, bool) {
    use rqe_iterators::{ResumeOutcome, TypeErasedRQEIterator};
    use rqe_iterators_test_utils::revalidate_via_resume;

    match revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), guard)
        .expect("resume failed")
    {
        ResumeOutcome::Ok(it) => (it, true),
        ResumeOutcome::Moved(it) => (it, false),
        ResumeOutcome::Aborted => {
            panic!("the index the reader holds is still registered, so it must not abort")
        }
    }
}

/// The suspend/resume twin of
/// [`tag_revalidate_after_write_reads_through_a_live_pointer`].
#[test]
fn tag_resume_after_write_reads_through_a_live_pointer() {
    const N: DocId = 4;
    let mock_ctx = MockContext::new(N, N as usize);
    let ii = new_tag_index(N);

    let it = tag_parked_across_a_write(ii, &mock_ctx, 2, append(N + 1));
    let (mut it, _) = resume_parked(it, &mock_ctx.spec_read());
    assert_eq!(drain(&mut it), (3..=N + 1).collect::<Vec<_>>());

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}

/// The suspend/resume twin of
/// [`tag_revalidate_after_write_to_a_later_block_reads_on`].
#[test]
fn tag_resume_after_write_to_a_later_block_reads_on() {
    use inverted_index::Encoder;

    let n = DocId::from(DocIdsOnly::RECOMMENDED_BLOCK_ENTRIES) + 1;
    let mock_ctx = MockContext::new(n, n as usize);
    let ii = new_tag_index(n);

    let it = tag_parked_across_a_write(ii, &mock_ctx, 2, append(n + 1));
    let (mut it, kept_position) = resume_parked(it, &mock_ctx.spec_read());
    assert!(
        kept_position,
        "a write past the reader's block must not force a re-seek"
    );
    assert_eq!(drain(&mut it), (3..=n + 1).collect::<Vec<_>>());

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}

/// The suspend/resume twin of
/// [`tag_revalidate_after_gc_reads_through_a_live_pointer`].
#[test]
fn tag_resume_after_gc_reads_through_a_live_pointer() {
    const N: DocId = 4;
    let mock_ctx = MockContext::new(N, N as usize);
    let ii = new_tag_index(N);

    let it = tag_parked_across_a_write(ii, &mock_ctx, 2, collect(3));
    let (mut it, _) = resume_parked(it, &mock_ctx.spec_read());
    assert_eq!(drain(&mut it), vec![4]);

    drop(it);
    // SAFETY: allocated by `new_tag_index`; the iterator using it is gone.
    drop(unsafe { Box::from_raw(ii) });
}
