/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Suspend/resume of [`TagIterator`], for each encoding of the tag's inverted index.

#![cfg(not(miri))]

use field::FieldMaskOrIndex;
use index_result::{RSIndexResult, RSQueryTerm};
use inverted_index::{DecodedBy, Encoder, InvertedIndex, RepairContext};
use iterators_ffi::inverted_index::{CTagIndexLookup, TagIterator};
use rqe_core::DocId;
use rqe_iterators::{RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome};
use rqe_iterators_test_utils::{DocIdEncoding, GlobalGuard, TestContext};
use rstest::rstest;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A tag iterator over `test_tag` in `ctx`.
fn tag_iterator(ctx: &TestContext) -> Box<TagIterator<'_>> {
    // SAFETY: 1. and 2. `ctx` owns a valid tag index and its `values` TrieMap, which
    // outlive the iterator. 3. `TagIterator::new` matches the encoding to the index.
    let lookup = unsafe { CTagIndexLookup::new(ctx.tag_index()) };
    // SAFETY: 1. `ctx` owns the tag's inverted index, which outlives the iterator.
    // 2. `lookup` is over the tag index holding it. 3. `ctx.sctx` and its spec live as
    // long as `ctx`.
    Box::new(unsafe {
        TagIterator::new(
            ctx.tag_inverted_index(),
            lookup,
            ctx.sctx,
            FieldMaskOrIndex::mask_all(),
            RSQueryTerm::new("test_tag", 0, 0),
            1.0,
        )
    })
}

/// Garbage-collect `doc_id` from the tag's inverted index, whichever encoding it uses.
fn collect(ctx: &TestContext, doc_id: DocId) {
    fn collect_from<E: Encoder + DecodedBy>(ii: &mut InvertedIndex<E>, doc_id: DocId) {
        let delta = ii
            .scan_gc(
                |live| live != doc_id,
                None::<fn(&RSIndexResult, &RepairContext<'_>)>,
            )
            .expect("scan GC failed")
            .expect("no GC scan delta");
        assert_eq!(ii.apply_gc(delta).entries_removed, 1);
    }

    match ctx.tag_inverted_index() {
        inverted_index_ffi::InvertedIndex::DocIdsOnly(ii) => collect_from(ii, doc_id),
        inverted_index_ffi::InvertedIndex::RawDocIdsOnly(ii) => collect_from(ii, doc_id),
        _ => unreachable!("tag inverted indexes only store doc ids"),
    }
}

/// The address of the allocation behind `b`, whatever type it is currently viewed as.
const fn address<T>(b: &T) -> *const u8 {
    std::ptr::from_ref(b).cast()
}

/// Panics unless `it` is the variant that reads `encoding`.
#[track_caller]
fn assert_reads(it: &TagIterator<'_>, encoding: DocIdEncoding) {
    assert!(
        matches!(
            (encoding, it),
            (DocIdEncoding::Varint, TagIterator::Encoded(_))
                | (DocIdEncoding::Raw, TagIterator::Raw(_))
        ),
        "a {encoding:?} index is read by {it:?}"
    );
}

fn next_doc_id(it: &mut TagIterator<'_>) -> Option<DocId> {
    it.read().expect("read failed").map(|r| r.doc_id)
}

/// A tag whose index was not touched while suspended resumes where it left off, in the
/// same allocation and reading the same encoding.
#[rstest]
fn a_resumed_tag_iterator_keeps_its_variant_allocation_and_position(
    #[values(DocIdEncoding::Varint, DocIdEncoding::Raw)] encoding: DocIdEncoding,
) {
    let _guard = GlobalGuard::default();
    let ctx = TestContext::tag_with_encoding([1, 3, 5].into_iter(), encoding);
    let mut it = tag_iterator(&ctx);
    assert_eq!(next_doc_id(&mut it), Some(1));
    let estimate = it.num_estimated();
    let allocation = address(&*it);

    let suspended = it.suspend();
    assert_eq!(address(&*suspended), allocation);
    assert_eq!(suspended.last_doc_id(), 1);
    assert_eq!(suspended.num_estimated(), estimate);

    let spec = ctx.spec_read();
    let Ok(ResumeOutcome::Ok(mut it)) = suspended.resume(&spec) else {
        panic!("an untouched index resumes unchanged");
    };
    assert_eq!(address(&*it), allocation);
    assert_reads(&it, encoding);
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(next_doc_id(&mut it), Some(3));
    assert_eq!(next_doc_id(&mut it), Some(5));
    assert_eq!(next_doc_id(&mut it), None);
}

/// A tag whose current document was garbage-collected while suspended resumes on the next one.
#[rstest]
fn a_tag_iterator_whose_current_document_was_collected_moves_on_resume(
    #[values(DocIdEncoding::Varint, DocIdEncoding::Raw)] encoding: DocIdEncoding,
) {
    let _guard = GlobalGuard::default();
    let ctx = TestContext::tag_with_encoding([1, 3, 5].into_iter(), encoding);
    let mut it = tag_iterator(&ctx);
    assert_eq!(next_doc_id(&mut it), Some(1));
    assert_eq!(next_doc_id(&mut it), Some(3));
    let allocation = address(&*it);

    let suspended = it.suspend();
    collect(&ctx, 3);

    let spec = ctx.spec_read();
    let Ok(ResumeOutcome::Moved(mut it)) = suspended.resume(&spec) else {
        panic!("losing the current document moves the iterator");
    };
    assert_eq!(address(&*it), allocation);
    assert_reads(&it, encoding);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().map(|r| r.doc_id), Some(5));
    assert_eq!(next_doc_id(&mut it), None);
}

/// A tag whose inverted index was removed while suspended cannot be resumed.
#[rstest]
fn a_tag_iterator_whose_index_was_removed_aborts_on_resume(
    #[values(DocIdEncoding::Varint, DocIdEncoding::Raw)] encoding: DocIdEncoding,
) {
    let _guard = GlobalGuard::default();
    let ctx = TestContext::tag_with_encoding([1, 3, 5].into_iter(), encoding);
    let mut it = tag_iterator(&ctx);
    assert_eq!(next_doc_id(&mut it), Some(1));

    let suspended = it.suspend();
    let old_ii: *mut inverted_index_ffi::InvertedIndex = ctx.tag_inverted_index();
    // SAFETY: the tag index was created by the test context and outlives this borrow.
    let values = unsafe { ctx.tag_index().as_ref() }.values;
    // SAFETY: a tag index's `values` is a valid TrieMap, and nothing else borrows it.
    let trie = unsafe { values.cast::<trie_rs::TrieMapOpaque>().as_mut() }
        .expect("the tag index has a values TrieMap");
    assert!(trie.remove(b"test_tag").is_some());

    let spec = ctx.spec_read();
    assert!(matches!(
        suspended.resume(&spec),
        Ok(ResumeOutcome::Aborted)
    ));
    // SAFETY: removing the TrieMap entry made this test the sole owner of the inverted index,
    // which was heap-allocated by the test context.
    unsafe { drop(Box::from_raw(old_ii)) };
}
