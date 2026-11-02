/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// The lookup fallback and the snapshot call `DocTable_Exists`,
// `DocTable_LiveBitmapWords`, `DocTable_FillLiveBitmap` and
// `DocTable_ForEachLiveId`, C functions miri cannot run.
#![cfg(not(miri))]

use std::mem;

use fork_gc::DocExistence;
use index_spec::IndexSpecReadGuard;
use roaring::RoaringTreemap;
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

#[test]
fn snapshot_of_chained_buckets_matches_the_lookup() {
    // Ten buckets for 100 docs chains ten docs into each; every third is deleted.
    let table = TestTable::new(10, (1..=100).map(|id| (id, id % 3 == 0)));
    let guard = table.guard();
    assert!(guard.live_doc_bitmap().is_some());

    let docs = DocExistence::snapshot(&guard);
    for id in 0..=130 {
        assert_eq!(docs.contains(id), guard.doc_exists(id), "doc id {id}");
    }
    assert!(docs.contains(1));
    assert!(!docs.contains(3));
}

#[test]
fn snapshot_of_a_sparse_table_keeps_the_bitmap() {
    // Two live docs under a maximum doc id of 1000.
    let table = TestTable::new(10, (1..=1000).map(|id| (id, id < 999)));
    let guard = table.guard();
    assert!(guard.live_doc_bitmap().is_some());

    let docs = DocExistence::snapshot(&guard);
    for id in [0, 1, 998, 999, 1000, 1001] {
        assert_eq!(docs.contains(id), guard.doc_exists(id), "doc id {id}");
    }
}

#[test]
fn snapshot_past_the_bitmap_cap_keeps_the_live_ids() {
    // A maximum doc id past the bitmap's byte cap (512M doc ids), with a
    // deleted doc chained among the live ones.
    const FAR: DocId = 600_000_000;
    let ids = [1, 7, 11, FAR - 10, FAR];
    let table = TestTable::new(10, ids.map(|id| (id, id == 11)));
    let guard = table.guard();
    assert!(guard.live_doc_bitmap().is_none());

    let docs = DocExistence::snapshot(&guard);
    let near = [0, 1, 2, 7, 11, 12];
    let far = [FAR - 11, FAR - 10, FAR - 1, FAR, FAR + 1];
    for id in near.into_iter().chain(far) {
        assert_eq!(docs.contains(id), guard.doc_exists(id), "doc id {id}");
    }
    assert!(docs.contains(FAR));
    assert!(!docs.contains(11));
}

#[test]
fn bitmap_answers_up_to_its_max_doc_id() {
    // The table is empty, so only the bitmap can report these docs as present.
    let table = TestTable::new(1, []);
    let guard = table.guard();
    let docs = DocExistence::with_bitmap(&guard, Box::new([0b10_0110]), 63);

    let present: Vec<DocId> = (0..64).filter(|&id| docs.contains(id)).collect();
    assert_eq!(present, [1, 2, 5]);
}

#[test]
fn ids_above_the_max_doc_id_fall_back_to_the_lookup() {
    let table = TestTable::new(1, []);
    let guard = table.guard();
    // Bits past the max doc id are set to show they are never read.
    let docs = DocExistence::with_bitmap(&guard, Box::new([u64::MAX]), 10);

    assert!(docs.contains(10));
    assert!(!docs.contains(11));
    assert!(!docs.contains(63));
    assert!(!docs.contains(64));
    assert!(!docs.contains(DocId::MAX));
}

#[test]
fn ids_past_the_bitmap_fall_back_to_the_lookup() {
    let table = TestTable::new(1, []);
    let guard = table.guard();
    // A max doc id beyond the bitmap's length must not index past it.
    let docs = DocExistence::with_bitmap(&guard, Box::new([u64::MAX]), DocId::MAX);

    assert!(docs.contains(63));
    assert!(!docs.contains(64));
    assert!(!docs.contains(DocId::MAX));
}

#[test]
fn ids_answer_up_to_their_max_doc_id() {
    // The table is empty, so only the set can report these docs as present.
    let table = TestTable::new(1, []);
    let guard = table.guard();
    let ids: RoaringTreemap = [2, 5, 1 << 40].into_iter().collect();
    let docs = DocExistence::with_ids(&guard, ids, 1 << 40);

    assert!(docs.contains(2));
    assert!(!docs.contains(3));
    assert!(docs.contains(1 << 40));
    assert!(!docs.contains((1 << 40) - 1));
}

#[test]
fn ids_above_the_max_doc_id_of_the_set_fall_back_to_the_lookup() {
    let table = TestTable::new(1, []);
    let guard = table.guard();
    // An id in the set but above the max doc id is never read from the set.
    let ids: RoaringTreemap = [5, 20].into_iter().collect();
    let docs = DocExistence::with_ids(&guard, ids, 10);

    assert!(docs.contains(5));
    assert!(!docs.contains(20));
}
