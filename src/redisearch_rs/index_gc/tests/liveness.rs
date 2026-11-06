/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem;

use index_gc::liveness::{GoneIds, MAX_BITMAP_BYTES};
use index_spec::IndexSpecReadGuard;
use inverted_index::DocLiveness;
use rqe_core::DocId;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A spec whose document table holds `docs` (id, deleted) chained into
/// `buckets` buckets, laid out the way `DocTable_Put` would place them.
struct TestTable {
    spec: ffi::IndexSpec,
    _documents: Box<[ffi::RSDocumentMetadata]>,
    _buckets: Box<[ffi::DMDChain]>,
}

impl TestTable {
    fn new(buckets: usize, docs: impl IntoIterator<Item = (DocId, bool)>) -> Self {
        let mut documents: Box<[ffi::RSDocumentMetadata]> = docs
            .into_iter()
            .map(|(id, deleted)| {
                // SAFETY: zero is valid for the scalar fields and nullable pointers.
                let mut document: ffi::RSDocumentMetadata = unsafe { mem::zeroed() };
                document.id = id;
                if deleted {
                    document.set_flags(ffi::RSDocumentFlags_Document_Deleted);
                }
                document
            })
            .collect();
        let mut chains: Box<[ffi::DMDChain]> = (0..buckets)
            .map(|_| ffi::DMDChain {
                root: std::ptr::null_mut(),
            })
            .collect();
        for document in documents.iter_mut() {
            let chain = &mut chains[document.id as usize % buckets];
            document.nextInChain = chain.root;
            chain.root = document;
        }

        // SAFETY: a zeroed IndexSpec is valid for the document-table reads under test.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.docs.maxDocId = documents.iter().map(|d| d.id).max().unwrap_or(0);
        spec.docs.size = documents.iter().filter(|d| d.flags() == 0).count();
        spec.docs.maxSize = buckets as DocId;
        spec.docs.cap = buckets;
        spec.docs.buckets = chains.as_mut_ptr();
        Self {
            spec,
            _documents: documents,
            _buckets: chains,
        }
    }

    const fn guard(&self) -> mem::ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: the table owns `spec` and does not change it while the guard lives.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }
}

/// `exists` and `any_gone` on every ID and range of `ids` agree with `gone`, a
/// set the caller says is exact there.
fn assert_matches(
    set: &GoneIds,
    ids: std::ops::RangeInclusive<DocId>,
    gone: impl Fn(DocId) -> bool,
) {
    for id in ids.clone() {
        assert_eq!((&set).exists(id), !gone(id), "doc id {id}");
    }
    for first in ids.clone() {
        for last in first..=*ids.end() {
            let expected = (first..=last).any(&gone);
            assert_eq!(
                (&set).any_gone(first, last),
                expected,
                "range {first}..={last}"
            );
        }
    }
}

#[test]
fn a_set_answers_like_its_ids_as_bits_or_as_a_set() {
    let ids = [3, 4, 10, 63, 64, 65, 130];
    for (max_bitmap_bytes, bits) in [(0, false), (1 << 10, true)] {
        let set = GoneIds::from_set_with_cap(ids.into_iter().collect(), max_bitmap_bytes);
        assert_eq!(matches!(set, GoneIds::Bits(_)), bits);
        // Every range is checked, so Miri covers only the boundary between the first two
        // words to finish in time.
        let span = if cfg!(miri) { 56..=70 } else { 0..=140 };
        assert_matches(&set, span, |id| ids.contains(&id));
        assert_eq!(set.into_set().into_iter().collect::<Vec<_>>(), ids);
    }
}

#[test]
fn ids_outside_a_sets_window_are_live() {
    let set = GoneIds::from_set([7, 9].into_iter().collect());
    assert!(matches!(set, GoneIds::Bits(_)));
    assert!((&set).exists(6));
    assert!(!(&set).exists(7));
    assert!((&set).exists(10));
    assert!((&set).exists(DocId::MAX));
    assert!(!(&set).any_gone(0, 6));
    assert!(!(&set).any_gone(10, DocId::MAX));
    assert!((&set).any_gone(0, DocId::MAX));
}

#[test]
fn a_window_too_wide_for_the_cap_is_kept_as_a_set() {
    let set = GoneIds::from_set([1, 1 << 40].into_iter().collect());
    assert!(matches!(set, GoneIds::Set(_)));
    assert!(!(&set).exists(1));
    assert!((&set).exists((1 << 40) - 1));
    assert!(!(&set).exists(1 << 40));
}

/// A table's gone IDs are its gaps: the prefix below the first live doc, and the
/// deleted docs above it, as bits or, past the cap, as a set.
#[test]
#[cfg_attr(miri, ignore = "walks the doc table through C")]
fn a_table_is_gone_below_its_first_live_doc_and_in_its_gaps() {
    // Ten buckets chain ten docs each; the first 70 and every third are deleted, so
    // the first live doc sits past the first bitmap word.
    let table = TestTable::new(10, (1..=100).map(|id| (id, id <= 70 || id % 3 == 0)));
    let guard = table.guard();
    assert!(guard.live_doc_bitmap().is_some());

    for (max_bitmap_bytes, bits) in [(MAX_BITMAP_BYTES, true), (0, false)] {
        let set = GoneIds::of_table_with_cap(&guard, max_bitmap_bytes);
        assert_eq!(matches!(set, GoneIds::Bits(_)), bits);
        assert_matches(&set, 0..=100, |id| !guard.doc_exists(id));
        assert!((&set).exists(101), "the table holds nothing newer");
    }
}

#[test]
#[cfg_attr(miri, ignore = "walks the doc table through C")]
fn a_table_past_the_bitmap_cap_collects_its_live_ids() {
    // A maximum doc id past the C bitmap's cap (512M doc ids), with a deleted doc
    // chained among the live ones.
    const FAR: DocId = 600_000_000;
    let ids = [7, 11, 12, FAR - 10, FAR];
    let table = TestTable::new(10, ids.map(|id| (id, id == 11)));
    let guard = table.guard();
    assert!(guard.live_doc_bitmap().is_none());

    let set = GoneIds::of_table(&guard);
    assert!(matches!(set, GoneIds::Set(_)));
    let near = [0, 1, 6, 7, 11, 12, 13];
    let far = [FAR - 11, FAR - 10, FAR - 1, FAR];
    for id in near.into_iter().chain(far) {
        assert_eq!((&set).exists(id), guard.doc_exists(id), "doc id {id}");
    }
    assert!((&set).any_gone(11, 11));
    assert!(!(&set).any_gone(FAR - 10, FAR - 10));
}

#[test]
#[cfg_attr(miri, ignore = "walks the doc table through C")]
fn an_empty_table_has_every_id_up_to_its_max_gone() {
    let table = TestTable::new(4, (1..=8).map(|id| (id, true)));
    let set = GoneIds::of_table(&table.guard());
    assert!(!(&set).exists(8));
    assert!((&set).exists(9));
    assert!((&set).any_gone(1, 1));
    assert!(!(&set).any_gone(9, 100));
}

/// A sparse table kept as a set takes its long dead stretches as ranges, and still
/// answers every ID the way the table does.
#[test]
#[cfg_attr(miri, ignore = "walks the doc table through C")]
fn a_sparse_table_kept_as_a_set_matches_the_table() {
    const LAST: DocId = 1 << 20;
    let table = TestTable::new(10, [(3, false), (64, false), (65, true), (LAST, false)]);
    let guard = table.guard();
    let set = GoneIds::of_table_with_cap(&guard, 0);
    assert!(matches!(set, GoneIds::Set(_)));
    for id in [0, 2, 3, 4, 63, 64, 65, 66, 1000, LAST - 1, LAST, LAST + 1] {
        assert_eq!(
            (&set).exists(id),
            guard.doc_exists(id) || id > LAST,
            "doc id {id}"
        );
    }
    assert!((&set).any_gone(66, LAST - 1));
    assert!(!(&set).any_gone(LAST, LAST));
    assert_eq!(set.into_set().len(), LAST + 1 - 3);
}

/// A set built a roaring container at a time answers every ID the way the table
/// does, across a mixed container, an all-live one, one with no doc at all, and a
/// last one cut short at the table's maximum.
#[test]
#[cfg_attr(miri, ignore = "walks the doc table through C")]
fn a_dense_table_kept_as_a_set_matches_the_table() {
    const CONTAINER: DocId = 1 << 16;
    const LAST: DocId = 3 * CONTAINER + 1000;
    let docs = (1..=LAST)
        .filter(|id| !(2 * CONTAINER..3 * CONTAINER).contains(id))
        .map(|id| {
            (
                id,
                id < CONTAINER && id % 3 == 0 || id > 3 * CONTAINER && id % 5 == 0,
            )
        });
    let table = TestTable::new(1024, docs);
    let guard = table.guard();
    let set = GoneIds::of_table_with_cap(&guard, 0);
    assert!(matches!(set, GoneIds::Set(_)));
    for id in 0..=LAST + 70 {
        assert_eq!(
            (&set).exists(id),
            guard.doc_exists(id) || id > LAST,
            "doc id {id}"
        );
    }
}
