/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the sparse vector index and its query iterator.

// Mock implementations of C symbols that aren't provided by the static C
// libraries the build script links.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

// Only the proptest, which miri cannot run, uses these.
#[cfg(not(miri))]
use std::collections::BTreeMap;
use std::ptr::NonNull;

use index_result::{RSIndexResult, RSResultKind};
use inverted_index::RepairContext;
#[cfg(not(miri))]
use proptest::prelude::*;
use rqe_iterators::{IteratorType, RQEIterator, RQEValidateStatus, SkipToOutcome};
use rqe_iterators_test_utils::{ContractChecker, MockContext};
use sparse_vector_index::{
    SparseMetric, SparseVectorError, SparseVectorIndex, SparseVectorIterator, parse_blob, prepare,
};

const DIM: u64 = 1000;

fn blob(pairs: &[(u32, f32)]) -> Vec<u8> {
    pairs
        .iter()
        .flat_map(|&(id, w)| id.to_le_bytes().into_iter().chain(w.to_le_bytes()))
        .collect()
}

fn ip(entries: &[(u32, f32)]) -> Vec<(u32, f32)> {
    prepare(entries.to_vec(), DIM, SparseMetric::InnerProduct).unwrap()
}

/// Drain the iterator into `(doc_id, score)` pairs.
fn collect<'index>(it: &mut impl RQEIterator<'index>) -> Vec<(u64, f64)> {
    let mut out = Vec::new();
    while let Some(r) = it.read().unwrap() {
        out.push((r.doc_id, r.as_numeric().unwrap()));
    }
    out
}

/// An iterator over `index`, which the test does not write to while it lives.
fn iter<'index>(
    index: &'index SparseVectorIndex,
    query: &[(u32, f32)],
    min_union_iter_heap: usize,
) -> Option<SparseVectorIterator<'index>> {
    // SAFETY: `index` outlives the iterator and is not written while it lives.
    unsafe { SparseVectorIterator::new(NonNull::from(index), query, min_union_iter_heap) }
}

/// Build an index holding `docs`, each prepared for `metric`.
fn index_of(metric: SparseMetric, docs: &[(u64, Vec<(u32, f32)>)]) -> SparseVectorIndex {
    let mut index = SparseVectorIndex::new(DIM, metric);
    for (doc_id, entries) in docs {
        let prepared = prepare(entries.clone(), DIM, metric).unwrap();
        index.add(*doc_id, &prepared).unwrap();
    }
    index
}

#[test]
fn parse_blob_decodes_little_endian_pairs() {
    let pairs = [(3, 0.5), (70000, -1.25)];
    assert_eq!(parse_blob(&blob(&pairs)).unwrap(), pairs);
    assert_eq!(parse_blob(&[]).unwrap(), vec![]);
}

#[test]
fn parse_blob_rejects_partial_pairs() {
    assert_eq!(
        parse_blob(&[0; 12]),
        Err(SparseVectorError::MalformedBlob { len: 12 })
    );
}

#[test]
fn prepare_rejects_invalid_vectors() {
    let cases = [
        (
            vec![(5, 1.0), (5, 2.0)],
            SparseVectorError::NotIncreasing { dim_id: 5 },
        ),
        (
            vec![(5, 1.0), (4, 2.0)],
            SparseVectorError::NotIncreasing { dim_id: 4 },
        ),
        (
            vec![(DIM as u32, 1.0)],
            SparseVectorError::DimensionOutOfRange {
                dim_id: DIM as u32,
                dim: DIM,
            },
        ),
        (
            vec![(1, f32::NAN)],
            SparseVectorError::NonFinite { dim_id: 1 },
        ),
        (
            vec![(1, f32::INFINITY)],
            SparseVectorError::NonFinite { dim_id: 1 },
        ),
    ];
    for (entries, expected) in cases {
        assert_eq!(
            prepare(entries.clone(), DIM, SparseMetric::InnerProduct),
            Err(expected),
            "{entries:?}"
        );
    }
}

#[test]
fn prepare_drops_zero_weights() {
    assert_eq!(ip(&[(1, 0.0), (2, 3.0), (3, -0.0)]), vec![(2, 3.0)]);
}

#[test]
fn prepare_normalizes_under_cosine() {
    let v = prepare(vec![(1, 3.0), (2, 4.0)], DIM, SparseMetric::Cosine).unwrap();
    assert_eq!(v, vec![(1, 0.6), (2, 0.8)]);
}

#[test]
fn prepare_rejects_zero_norm_under_cosine() {
    for entries in [vec![], vec![(1, 0.0)]] {
        assert_eq!(
            prepare(entries, DIM, SparseMetric::Cosine),
            Err(SparseVectorError::ZeroNorm)
        );
    }
    // Under IP an empty vector is valid: it indexes nothing.
    assert_eq!(ip(&[]), vec![]);
}

#[test]
fn add_tracks_dimensions_and_entries() {
    let mut index = SparseVectorIndex::new(DIM, SparseMetric::InnerProduct);
    assert!(index.is_empty());
    let first = index.add(1, &ip(&[(10, 0.5), (20, 0.3)])).unwrap();
    assert_eq!(first.records, 2);
    assert!(first.bytes > 0);
    index.add(2, &ip(&[(20, 0.1), (30, 0.2)])).unwrap();
    assert_eq!(index.num_dimensions(), 3);
    assert_eq!(index.num_entries(), 4);
    assert!(index.memory_usage() > 0);
    assert_eq!(index.get_dimension(20).unwrap().unique_docs(), 2);
    assert!(index.get_dimension(40).is_none());
}

/// A document sharing one query dimension can outrank one sharing all of
/// them, so the candidate set must be the union.
#[test]
fn union_finds_partial_overlap() {
    let index = index_of(
        SparseMetric::InnerProduct,
        &[
            (1, vec![(1, 10.0)]),
            (2, vec![(1, 0.1), (2, 0.1), (3, 0.1)]),
            (3, vec![(9, 1.0)]),
        ],
    );
    let query = ip(&[(1, 1.0), (2, 1.0), (3, 1.0)]);
    let mut it = ContractChecker::new(iter(&index, &query, 10).unwrap());
    let got = collect(&mut it);
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, 1);
    assert!((got[0].1 - 10.0).abs() < 1e-6);
    assert_eq!(got[1].0, 2);
    assert!((got[1].1 - 0.3).abs() < 1e-6);
}

#[test]
fn query_with_only_missing_dimensions_matches_nothing() {
    let index = index_of(SparseMetric::InnerProduct, &[(1, vec![(1, 1.0)])]);
    assert!(iter(&index, &ip(&[(5, 1.0), (6, 1.0)]), 10).is_none());
}

#[test]
fn skip_to_scores_the_landing_document() {
    let index = index_of(
        SparseMetric::InnerProduct,
        &[
            (2, vec![(1, 1.0)]),
            (5, vec![(1, 2.0), (2, 3.0)]),
            (9, vec![(2, 1.0)]),
        ],
    );
    let query = ip(&[(1, 1.0), (2, 2.0)]);
    let mut it = iter(&index, &query, 10).unwrap();
    assert_eq!(it.type_(), IteratorType::SparseVector);
    // Its own result kind, so the sparse scorer never takes a dense distance for it.
    assert_eq!(
        it.read().unwrap().unwrap().kind(),
        RSResultKind::SparseScore
    );
    it.rewind();

    match it.skip_to(5).unwrap().unwrap() {
        SkipToOutcome::Found(r) => {
            assert_eq!(r.doc_id, 5);
            assert!((r.as_numeric().unwrap() - 8.0).abs() < 1e-6);
        }
        SkipToOutcome::NotFound(_) => panic!("doc 5 is in the union"),
    }
    match it.skip_to(6).unwrap().unwrap() {
        SkipToOutcome::NotFound(r) => {
            assert_eq!(r.doc_id, 9);
            assert!((r.as_numeric().unwrap() - 2.0).abs() < 1e-6);
        }
        SkipToOutcome::Found(_) => panic!("doc 6 is not in the union"),
    }
    assert!(it.skip_to(10).unwrap().is_none());
    assert!(it.at_eof());

    it.rewind();
    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);
}

#[test]
fn cosine_scores_are_cosine_similarities() {
    let index = index_of(
        SparseMetric::Cosine,
        &[(1, vec![(1, 3.0), (2, 4.0)]), (2, vec![(1, 100.0)])],
    );
    let query = prepare(vec![(1, 1.0)], DIM, SparseMetric::Cosine).unwrap();
    let got = collect(&mut iter(&index, &query, 10).unwrap());
    assert_eq!(got[0].0, 1);
    assert!((got[0].1 - 0.6).abs() < 1e-6);
    assert_eq!(got[1].0, 2);
    assert!((got[1].1 - 1.0).abs() < 1e-6);
}

#[cfg(not(miri))]
fn sparse_vector() -> impl Strategy<Value = Vec<(u32, f32)>> {
    prop::collection::btree_map(0u32..40, -4.0f32..4.0, 0..8)
        .prop_map(|m| m.into_iter().collect::<Vec<_>>())
}

// proptest reads the working directory, which miri's isolation forbids.
#[cfg(not(miri))]
proptest! {
    /// Every document sharing a dimension with the query is yielded once, in
    /// doc-ID order, with its exact dot product; the union variant (flat or
    /// heap) does not change the answer.
    #[test]
    fn matches_brute_force_dot_product(
        docs in prop::collection::vec(sparse_vector(), 0..30),
        query in sparse_vector(),
        min_union_iter_heap in 0usize..6,
    ) {
        let docs: Vec<(u64, Vec<(u32, f32)>)> = docs
            .into_iter()
            .enumerate()
            .map(|(i, v)| (i as u64 + 1, ip(&v)))
            .collect();
        let query = ip(&query);
        let index = index_of(SparseMetric::InnerProduct, &docs);

        let expected: BTreeMap<u64, f64> = docs
            .iter()
            .filter_map(|(doc_id, d)| {
                let d: BTreeMap<u32, f32> = d.iter().copied().collect();
                let shared: Vec<f64> = query
                    .iter()
                    .filter_map(|(i, q)| d.get(i).map(|w| f64::from(*q) * f64::from(*w)))
                    .collect();
                (!shared.is_empty()).then(|| (*doc_id, shared.iter().sum()))
            })
            .collect();

        let got = match iter(&index, &query, min_union_iter_heap) {
            Some(it) => collect(&mut ContractChecker::new(it)),
            None => vec![],
        };
        let got_ids: Vec<u64> = got.iter().map(|(d, _)| *d).collect();
        let expected_ids: Vec<u64> = expected.keys().copied().collect();
        prop_assert_eq!(got_ids, expected_ids);
        for (doc_id, score) in got {
            prop_assert!((score - expected[&doc_id]).abs() < 1e-4, "doc {doc_id}: {score} vs {}", expected[&doc_id]);
        }
    }
}

const NO_REPAIR: Option<fn(&RSIndexResult<'_>, &RepairContext<'_>)> = None;

/// Run a GC pass over every dimension of `index`, treating `gone` as deleted.
/// Returns how many dimensions were removed.
fn run_gc(index: &mut SparseVectorIndex, gone: &[u64]) -> usize {
    let live = |id: u64| !gone.contains(&id);
    let mut removed = 0;
    for dim_id in index.dimension_ids_after(None) {
        let list = index.get_dimension(dim_id).unwrap();
        let unique_id = u32::from(list.unique_id());
        let Some(delta) = list.scan_gc(live, NO_REPAIR).unwrap() else {
            continue;
        };
        let outcome = index.apply_gc(dim_id, unique_id, delta, None).unwrap();
        removed += usize::from(outcome.removed.is_some());
    }
    removed
}

#[test]
fn gc_collects_deleted_entries_and_drops_empty_dimensions() {
    let mut index = SparseVectorIndex::new(DIM, SparseMetric::InnerProduct);
    index.add(1, &ip(&[(1, 1.0), (2, 1.0)])).unwrap();
    index.add(2, &ip(&[(2, 1.0), (3, 1.0)])).unwrap();
    index.add(3, &ip(&[(1, 1.0)])).unwrap();
    let memory_before = index.memory_usage();
    assert_eq!(index.num_entries(), 5);

    // Doc 2 is the only one with dimension 3, so that dimension goes.
    assert_eq!(run_gc(&mut index, &[2]), 1);
    assert_eq!(index.num_entries(), 3);
    assert_eq!(index.dimension_ids_after(None), vec![1, 2]);
    assert!(index.memory_usage() < memory_before);

    let query = ip(&[(1, 1.0), (2, 1.0), (3, 1.0)]);
    let got = collect_results(&index, &query);
    assert_eq!(got.iter().map(|(d, _)| *d).collect::<Vec<_>>(), vec![1, 3]);

    // Collecting everything leaves an empty index with no dimension.
    run_gc(&mut index, &[1, 3]);
    assert!(index.is_empty());
    assert_eq!(index.num_dimensions(), 0);
    assert_eq!(index.memory_usage(), 0);
}

fn collect_results(index: &SparseVectorIndex, query: &[(u32, f32)]) -> Vec<(u64, f64)> {
    match iter(index, query, 10) {
        Some(mut it) => collect(&mut it),
        None => vec![],
    }
}

#[test]
fn gc_ignores_a_delta_for_a_replaced_posting_list() {
    let mut index = SparseVectorIndex::new(DIM, SparseMetric::InnerProduct);
    index.add(1, &ip(&[(1, 1.0)])).unwrap();
    index.add(2, &ip(&[(1, 1.0)])).unwrap();
    let list = index.get_dimension(1).unwrap();
    let wrong_id = u32::from(list.unique_id()).wrapping_add(1);
    let delta = list.scan_gc(|id: u64| id != 1, NO_REPAIR).unwrap().unwrap();
    assert!(index.apply_gc(1, wrong_id, delta, None).is_none());
    assert_eq!(index.num_entries(), 2);

    let delta = index
        .get_dimension(1)
        .unwrap()
        .scan_gc(|id: u64| id != 1, NO_REPAIR)
        .unwrap()
        .unwrap();
    assert!(
        index.apply_gc(7, 0, delta, None).is_none(),
        "no such dimension"
    );
}

/// GC freeing a dimension while a query is parked must not leave the
/// iterator reading the freed posting list: the dimension's child aborts and
/// the union carries on with the others.
#[test]
fn revalidate_drops_a_dimension_gc_removed() {
    let ctx = MockContext::new(0, 0);
    let index_ptr = Box::into_raw(Box::new(SparseVectorIndex::new(
        DIM,
        SparseMetric::InnerProduct,
    )));
    // SAFETY: `index_ptr` is a fresh, uniquely owned allocation, freed at the
    // end of the test; this borrow ends before the iterator is built.
    let index = unsafe { &mut *index_ptr };
    for (doc_id, entries) in [
        (1, vec![(1, 1.0)]),
        (2, vec![(2, 5.0)]),
        (3, vec![(1, 2.0)]),
        (4, vec![(2, 7.0)]),
    ] {
        index.add(doc_id, &ip(&entries)).unwrap();
    }
    let query = ip(&[(1, 1.0), (2, 1.0)]);
    // SAFETY: the index outlives the iterator, and is only written below while
    // the iterator is parked, as a GC pass would under the write lock.
    let mut it =
        unsafe { SparseVectorIterator::new(NonNull::new(index_ptr).unwrap(), &query, 10) }.unwrap();
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);

    // Docs 2 and 4 are deleted and collected: dimension 2 is removed and freed.
    // SAFETY: as above.
    assert_eq!(run_gc(unsafe { &mut *index_ptr }, &[2, 4]), 1);

    let status = it.revalidate(&ctx.spec_read()).unwrap();
    assert!(!matches!(status, RQEValidateStatus::Aborted));
    let rest: Vec<u64> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(rest, vec![3]);
    drop(it);
    // SAFETY: the iterator is gone and `index_ptr` came from `Box::into_raw`.
    drop(unsafe { Box::from_raw(index_ptr) });
}

#[test]
fn revalidate_aborts_when_every_dimension_is_removed() {
    let ctx = MockContext::new(0, 0);
    let index_ptr = Box::into_raw(Box::new(SparseVectorIndex::new(
        DIM,
        SparseMetric::InnerProduct,
    )));
    // SAFETY: `index_ptr` is a fresh, uniquely owned allocation, freed at the
    // end of the test; this borrow ends before the iterator is built.
    let index = unsafe { &mut *index_ptr };
    index.add(1, &ip(&[(1, 1.0)])).unwrap();
    index.add(2, &ip(&[(1, 1.0)])).unwrap();
    // SAFETY: as in `revalidate_drops_a_dimension_gc_removed`.
    let mut it = unsafe {
        SparseVectorIterator::new(NonNull::new(index_ptr).unwrap(), &ip(&[(1, 1.0)]), 10)
    }
    .unwrap();
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);

    // SAFETY: as above.
    run_gc(unsafe { &mut *index_ptr }, &[1, 2]);
    assert!(matches!(
        it.revalidate(&ctx.spec_read()).unwrap(),
        RQEValidateStatus::Aborted
    ));
    drop(it);
    // SAFETY: as in `revalidate_drops_a_dimension_gc_removed`.
    drop(unsafe { Box::from_raw(index_ptr) });
}

/// A GC pass that only shrinks a posting list leaves the parked iterator on
/// its position, reading the compacted list.
#[test]
fn revalidate_follows_a_compacted_posting_list() {
    let ctx = MockContext::new(0, 0);
    let index_ptr = Box::into_raw(Box::new(SparseVectorIndex::new(
        DIM,
        SparseMetric::InnerProduct,
    )));
    // SAFETY: `index_ptr` is a fresh, uniquely owned allocation, freed at the
    // end of the test; this borrow ends before the iterator is built.
    let index = unsafe { &mut *index_ptr };
    for doc_id in 1..=6 {
        index.add(doc_id, &ip(&[(1, doc_id as f32)])).unwrap();
    }
    // SAFETY: as in `revalidate_drops_a_dimension_gc_removed`.
    let mut it = unsafe {
        SparseVectorIterator::new(NonNull::new(index_ptr).unwrap(), &ip(&[(1, 1.0)]), 10)
    }
    .unwrap();
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);

    // SAFETY: as above.
    run_gc(unsafe { &mut *index_ptr }, &[3, 5]);
    assert!(!matches!(
        it.revalidate(&ctx.spec_read()).unwrap(),
        RQEValidateStatus::Aborted
    ));
    let rest: Vec<(u64, f64)> = std::iter::from_fn(|| {
        it.read()
            .unwrap()
            .map(|r| (r.doc_id, r.as_numeric().unwrap()))
    })
    .collect();
    assert_eq!(rest, vec![(4, 4.0), (6, 6.0)]);
    drop(it);
    // SAFETY: as in `revalidate_drops_a_dimension_gc_removed`.
    drop(unsafe { Box::from_raw(index_ptr) });
}
