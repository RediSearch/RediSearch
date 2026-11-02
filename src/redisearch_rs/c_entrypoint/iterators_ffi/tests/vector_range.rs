/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the lazy vector range iterator built by
//! [`NewLazyVectorRangeIteratorFromParams`](iterators_ffi::deferred::NewLazyVectorRangeIteratorFromParams)
//! and the [`IndexRef::range_query`] it runs.

mod common;

use std::ptr::NonNull;

use ffi::{VecSimQueryParams, VecSimQueryReply_Order_BY_ID, VecSimQueryReply_Order_BY_SCORE};
use rqe_iterators::{RQEIterator, c2rust::CRQEIterator};
use vecsim::{IndexRef, QueryVector, ReplyOrder};
use vector_score_source::test_utils::{TestIndex, collect_ids, uniform_blob};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Each result of `it` as its doc id and metric value, in read order.
fn collect_scored(it: &mut CRQEIterator) -> Vec<(u64, f64)> {
    std::iter::from_fn(|| {
        it.read().unwrap().map(|r| {
            let distance = r
                .as_numeric()
                .expect("a metric result carries its distance");
            (r.doc_id, distance)
        })
    })
    .collect()
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn id_only_and_metric_ranges_yield_the_same_ids_with_their_distances() {
    // Doc `i` is `[i; 4]`, so its L2 distance to `[50; 4]` is `4 * (50 - i)^2`: a radius of 400
    // matches exactly the ids within 10 of 50.
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(50.0, 4);
    let distance = |id: u64| 4.0 * (50.0 - id as f64).powi(2);
    for order in [
        VecSimQueryReply_Order_BY_ID,
        VecSimQueryReply_Order_BY_SCORE,
    ] {
        let ids = collect_ids(&mut *common::range_iterator(
            &index, &query, 400.0, order, false,
        ));
        let scored = collect_scored(&mut common::range_iterator(
            &index, &query, 400.0, order, true,
        ));

        let scored_ids: Vec<_> = scored.iter().map(|&(id, _)| id).collect();
        assert_eq!(ids, scored_ids, "order {order}");
        for &(id, d) in &scored {
            assert_eq!(d, distance(id), "order {order}, id {id}");
        }
        if order == VecSimQueryReply_Order_BY_SCORE {
            assert!(scored.is_sorted_by(|a, b| a.1 <= b.1), "{scored:?}");
        }
        let mut sorted = ids;
        sorted.sort_unstable();
        assert_eq!(sorted, (40..=60).collect::<Vec<_>>(), "order {order}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn range_matching_nothing_is_empty_once_read() {
    // Every doc is at a positive distance from `[0.5; 4]`, so a zero radius matches nothing.
    let index = TestIndex::flat(100, 4);
    let query = uniform_blob(0.5, 4);
    for yields_metric in [false, true] {
        let mut it = common::range_iterator(
            &index,
            &query,
            0.0,
            VecSimQueryReply_Order_BY_ID,
            yields_metric,
        );
        // Until the query runs, the estimate is the index size.
        assert_eq!(it.num_estimated(), 100, "yields_metric {yields_metric}");
        assert!(
            matches!(it.read(), Ok(None)),
            "yields_metric {yields_metric}"
        );
        assert_eq!(it.num_estimated(), 0, "yields_metric {yields_metric}");
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
#[should_panic(expected = "VecSim range query radius must not be negative")]
fn range_query_panics_on_a_negative_radius() {
    let index = TestIndex::flat(100, 4);
    let ptr = NonNull::new(index.as_ptr()).expect("the fixture index is non-null");
    // SAFETY: `index` outlives `index_ref`, which does not leave this function.
    let index_ref = unsafe { IndexRef::from_raw(ptr) };
    // SAFETY: the blob is sized for the fixture's 4-dimensional FLOAT32 index.
    let query = unsafe { QueryVector::new(index_ref, uniform_blob(50.0, 4)) };
    // SAFETY: all-zero is a valid `VecSimQueryParams`.
    let mut params: VecSimQueryParams = unsafe { std::mem::zeroed() };
    params.timeoutCtx = index.timeout_ptr().cast();
    // SAFETY: `timeoutCtx` is the fixture's timeout, valid for this call.
    let _ = unsafe { index_ref.range_query(&query, -1.0, &mut params, ReplyOrder::ById) };
}
