/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`ElementTag`] over an index that spans several blocks: document `d` has the tag in the
//! elements that [`element_offsets`] gives it.

use std::{cell::Cell, ptr::NonNull};

use ffi::IndexFlags_Index_StoreTermOffsets;
use index_result::{RSIndexResult, RSOffsetSlice};
use inverted_index::{InvertedIndex, RepairContext, offsets_only::OffsetsOnly};
use query_term::RSQueryTerm;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{
    NoOpChecker, RQEIterator, RQEValidateStatus, SkipToOutcome,
    inverted_index::{ElementTag, Tag, TagElements, TagLookup},
};
use rqe_iterators_test_utils::MockContext;

/// More entries than three blocks of the index hold.
const NUM_DOCS: DocId = 350;

/// The stored form of the element numbers of document `doc_id`: the first element, then the
/// distance to the second one.
const fn element_offsets(doc_id: DocId) -> [u8; 2] {
    [(doc_id % 100) as u8, 3]
}

/// The field that the entries are written with, which is not every field.
const WRITTEN_FIELD_MASK: rqe_core::FieldMask = 0b1;

/// The index of one tag, which a test can change or remove while an iterator is parked on
/// it, as the garbage collector does while a query has released the spec lock.
struct ElementIndex {
    ii: NonNull<InvertedIndex<OffsetsOnly>>,
    removed: Cell<bool>,
}

impl ElementIndex {
    fn new() -> Self {
        let mut ii = InvertedIndex::<OffsetsOnly>::new(IndexFlags_Index_StoreTermOffsets);
        for doc_id in 1..=NUM_DOCS {
            let offsets = element_offsets(doc_id);
            let record = RSIndexResult::build_term()
                .borrowed_record(None, RSOffsetSlice::from_slice(&offsets))
                .doc_id(doc_id)
                .field_mask(WRITTEN_FIELD_MASK)
                .frequency(1)
                .build();
            ii.add_record(&record).expect("append failed");
        }
        assert!(ii.number_of_blocks() >= 3);
        Self {
            ii: NonNull::new(Box::into_raw(Box::new(ii))).expect("Box pointers are non-null"),
            removed: Cell::new(false),
        }
    }

    /// Collect the entry of `doc_id` in place.
    fn collect(&self, doc_id: DocId) {
        // SAFETY: the index is valid, and the iterators of the tests are parked.
        let ii = unsafe { &mut *self.ii.as_ptr() };
        let delta = ii
            .scan_gc(
                |d| d != doc_id,
                None::<fn(&RSIndexResult, &RepairContext<'_>)>,
            )
            .expect("scan_gc failed")
            .expect("the collected document is indexed");
        ii.apply_gc(delta);
    }

    fn remove(&self) {
        self.removed.set(true);
    }

    /// An iterator over the index that reports its results as `elements` says.
    fn iterator<'index>(
        &'index self,
        mock_ctx: &'index MockContext,
        elements: TagElements,
    ) -> ElementTag<'index, &'index Self, NoOpChecker> {
        // SAFETY: the index is valid and not mutably aliased while the reader is built.
        let reader = unsafe { self.ii.as_ref() }.reader();
        // SAFETY: `mock_ctx` provides a valid `RedisSearchCtx` with a valid `spec` that
        // outlives the iterator, and the index outlives the lookup.
        let tag = unsafe {
            Tag::new(
                reader,
                mock_ctx.sctx(),
                self,
                RSQueryTerm::new("tag", 0, 0),
                1.0,
                NoOpChecker,
            )
        };
        ElementTag::new(tag, elements)
    }
}

impl Drop for ElementIndex {
    fn drop(&mut self) {
        // SAFETY: allocated by `new`, and the iterators that borrow `self` are gone.
        drop(unsafe { Box::from_raw(self.ii.as_ptr()) });
    }
}

impl TagLookup<OffsetsOnly> for &ElementIndex {
    fn find(&self, _tag: &[u8]) -> Option<&InvertedIndex<OffsetsOnly>> {
        // SAFETY: the index is valid, and the tests only change it while the iterator is
        // parked.
        (!self.removed.get()).then(|| unsafe { self.ii.as_ref() })
    }
}

/// Checks that `result` is what a reader that asked for `elements` gets for `doc_id`.
#[track_caller]
fn assert_reported(result: &RSIndexResult<'_>, doc_id: DocId, elements: TagElements) {
    assert_eq!(result.doc_id, doc_id);
    let offsets = result.as_term().expect("a term result").offsets();
    match elements {
        TagElements::Reported => {
            assert_eq!(offsets, element_offsets(doc_id), "document {doc_id}");
        }
        TagElements::Hidden => {
            assert!(offsets.is_empty(), "document {doc_id}");
            assert_eq!(result.field_mask, RS_FIELDMASK_ALL, "document {doc_id}");
        }
    }
}

#[test]
#[cfg_attr(miri, ignore = "MockContext calls into the C library")]
fn read_reports_every_entry_of_every_block() {
    let index = ElementIndex::new();
    let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);
    for elements in [TagElements::Reported, TagElements::Hidden] {
        let mut it = index.iterator(&mock_ctx, elements);
        for doc_id in 1..=NUM_DOCS {
            let result = it.read().expect("read failed").expect("a document");
            assert_reported(result, doc_id, elements);
        }
        assert!(it.read().expect("read failed").is_none());
        assert!(it.at_eof());

        it.rewind();
        let result = it.read().expect("read failed").expect("a document");
        assert_reported(result, 1, elements);
    }
}

#[test]
#[cfg_attr(miri, ignore = "MockContext calls into the C library")]
fn skip_to_reports_the_entry_it_lands_on_in_a_later_block() {
    let index = ElementIndex::new();
    let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);
    for elements in [TagElements::Reported, TagElements::Hidden] {
        let mut it = index.iterator(&mock_ctx, elements);
        // One target per block, then one in the block that the iterator is already in.
        for doc_id in [50, 150, 320, 321] {
            match it.skip_to(doc_id).expect("skip_to failed") {
                Some(SkipToOutcome::Found(result)) => assert_reported(result, doc_id, elements),
                _ => panic!("document {doc_id} is indexed"),
            }
            let current = it.current().expect("positioned");
            assert_reported(current, doc_id, elements);
        }
        assert!(it.skip_to(NUM_DOCS + 1).expect("skip_to failed").is_none());
    }
}

/// The documents that the revalidation tests park on and collect, in one block.
const PARKED: DocId = 150;
const AFTER_PARKED: DocId = 151;

/// A collection that leaves the entry of the iterator in place still makes the iterator
/// decode it again.
#[test]
#[cfg_attr(miri, ignore = "MockContext calls into the C library")]
fn revalidate_reports_the_entry_again() {
    for elements in [TagElements::Reported, TagElements::Hidden] {
        let index = ElementIndex::new();
        let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);
        let guard = mock_ctx.spec_read();
        let mut it = index.iterator(&mock_ctx, elements);
        it.skip_to(PARKED)
            .expect("skip_to failed")
            .expect("indexed");
        index.collect(AFTER_PARKED);

        let status = it.revalidate(&guard).expect("revalidate failed");

        assert_eq!(status, RQEValidateStatus::Ok);
        assert_reported(it.current().expect("positioned"), PARKED, elements);
    }
}

#[test]
#[cfg_attr(miri, ignore = "MockContext calls into the C library")]
fn revalidate_reports_the_entry_it_moves_to() {
    for elements in [TagElements::Reported, TagElements::Hidden] {
        let index = ElementIndex::new();
        let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);
        let guard = mock_ctx.spec_read();
        let mut it = index.iterator(&mock_ctx, elements);
        it.skip_to(PARKED)
            .expect("skip_to failed")
            .expect("indexed");
        index.collect(PARKED);

        match it.revalidate(&guard).expect("revalidate failed") {
            RQEValidateStatus::Moved {
                current: Some(current),
            } => assert_reported(current, AFTER_PARKED, elements),
            other => panic!("expected a move to document {AFTER_PARKED}, got {other:?}"),
        }
    }
}

#[test]
#[cfg_attr(miri, ignore = "MockContext calls into the C library")]
fn revalidate_aborts_when_the_index_of_the_tag_is_gone() {
    let index = ElementIndex::new();
    let mock_ctx = MockContext::new(NUM_DOCS, NUM_DOCS as usize);
    let guard = mock_ctx.spec_read();
    let mut it = index.iterator(&mock_ctx, TagElements::Reported);
    it.skip_to(PARKED)
        .expect("skip_to failed")
        .expect("indexed");
    index.remove();

    let status = it.revalidate(&guard).expect("revalidate failed");

    assert_eq!(status, RQEValidateStatus::Aborted);
}
