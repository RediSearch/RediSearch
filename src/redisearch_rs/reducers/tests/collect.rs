/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! End-to-end tests for the COLLECT reducer that drive
//! [`RemoteCollectReducer`] and [`LocalCollectReducer`] through
//! `add` → `finalize`. Pure comparator unit tests live inline in
//! `reducers/src/collect/storage.rs`. The
//! `RSGlobalConfig.maxAggregateResults` array-path cap is covered by the
//! Python E2E tests because mutating the process-global would require
//! serialising Rust tests.

extern crate redisearch_rs;

use std::ffi::CStr;

use reducers::collect::{
    LocalCollectCtx, LocalCollectReducer, RemoteCollectCtx, RemoteCollectReducer,
};
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{Map, SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Distinct keys in the same row need distinct `dstidx`es so they don't
/// alias the same `RLookupRow` slot.
fn mk_key(name: &'static CStr, dstidx: u16) -> RLookupKey<'static> {
    let mut key = RLookupKey::new(name, RLookupKeyFlags::empty());
    key.dstidx = dstidx;
    key
}

fn map_entries(value: &SharedValue) -> &Map {
    match &**value {
        Value::Map(map) => map,
        v => panic!("expected Value::Map, got {}", v.variant_name()),
    }
}

fn array_entries(value: &SharedValue) -> &[SharedValue] {
    match &**value {
        Value::Array(array) => array,
        v => panic!("expected Value::Array, got {}", v.variant_name()),
    }
}

fn make_row<'a>(
    field_keys: &[&RLookupKey<'_>],
    sort_keys: &[&RLookupKey<'_>],
    field_vals: &[SharedValue],
    sort_vals: &[SharedValue],
) -> RLookupRow<'a> {
    let mut row = RLookupRow::new();
    for (k, v) in field_keys.iter().zip(field_vals.iter()) {
        row.write_key(k, v.clone());
    }
    for (k, v) in sort_keys.iter().zip(sort_vals.iter()) {
        row.write_key(k, v.clone());
    }
    row
}

/// Drive a full `add` → `finalize` cycle on a standalone
/// (`is_internal = false`) [`RemoteCollectReducer`].
fn run_collect(
    field_keys: Box<[&RLookupKey<'_>]>,
    sort_keys: Box<[&RLookupKey<'_>]>,
    sort_asc_map: u64,
    limit: Option<(u64, u64)>,
    rows: Vec<(Vec<SharedValue>, Vec<SharedValue>)>,
) -> SharedValue {
    let r = RemoteCollectReducer::new(
        field_keys.clone(),
        false,
        sort_keys.clone(),
        sort_asc_map,
        limit,
        /* is_internal */ false,
    );
    let mut ctx = RemoteCollectCtx::new(&r);
    for (projected, sort_vals) in rows {
        let row = make_row(&field_keys, &sort_keys, &projected, &sort_vals);
        ctx.add(&r, &row);
    }
    ctx.finalize(&r)
}

fn extract_num_field(out: &SharedValue, name: &[u8]) -> Vec<f64> {
    array_entries(out)
        .iter()
        .map(|row_sv| {
            map_entries(row_sv)
                .get(name)
                .and_then(|v| v.as_num())
                .expect("missing or non-numeric field")
        })
        .collect()
}

/// One-column row where the projected and sort slots hold the same value.
/// Array-path callers pass empty `sort_keys`, leaving the sort slot unused.
fn num_row(v: f64) -> (Vec<SharedValue>, Vec<SharedValue>) {
    (vec![SharedValue::new_num(v)], vec![SharedValue::new_num(v)])
}

fn num_row_maybe_null(projected: f64, sort: Option<f64>) -> (Vec<SharedValue>, Vec<SharedValue>) {
    let sort_sv = match sort {
        Some(v) => SharedValue::new_num(v),
        None => SharedValue::null_static(),
    };
    (vec![SharedValue::new_num(projected)], vec![sort_sv])
}

// Part A — pre-merge tests, ported onto `RemoteCollectReducer`.

#[test]
fn array_no_sortby_no_limit_preserves_insertion_order() {
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        None,
        vec![num_row(3.0), num_row(1.0), num_row(2.0)],
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![3.0, 1.0, 2.0]);
}

#[test]
fn array_no_sortby_with_limit_takes_first_k() {
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 3)),
        (0..5).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn array_limit_offset_exceeds_len_yields_empty() {
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((10, 5)),
        (0..3).map(|i| num_row(i as f64)).collect(),
    );
    assert!(extract_num_field(&out, b"v").is_empty());
}

#[test]
fn array_limit_count_exceeds_remainder_no_padding() {
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 10)),
        (0..3).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn array_limit_with_offset_skips_correctly() {
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((2, 10)),
        (0..5).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![2.0, 3.0, 4.0]);
}

#[test]
fn heap_single_asc_with_limit_yields_top_k_sorted() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1, // ASC
        Some((0, 3)),
        [5.0, 2.0, 8.0, 1.0, 7.0, 3.0]
            .into_iter()
            .map(num_row)
            .collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![1.0, 2.0, 3.0]);
}

#[test]
fn heap_single_desc_with_limit_yields_top_k_sorted() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b0, // DESC
        Some((0, 3)),
        [5.0, 2.0, 8.0, 1.0, 7.0, 3.0]
            .into_iter()
            .map(num_row)
            .collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![8.0, 7.0, 5.0]);
}

#[test]
fn heap_multi_key_mixed_directions_sort_correctly() {
    // sort_asc_map = 0b01 ⇒ key 0 ASC, key 1 DESC.
    let v = mk_key(c"v", 0);
    let s0 = mk_key(c"s0", 1);
    let s1 = mk_key(c"s1", 2);
    let sort_keys: Box<[&RLookupKey]> = vec![&s0, &s1].into_boxed_slice();
    let mk = |proj: f64, a: f64, b: f64| {
        (
            vec![SharedValue::new_num(proj)],
            vec![SharedValue::new_num(a), SharedValue::new_num(b)],
        )
    };
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        sort_keys,
        0b01,
        Some((0, 4)),
        vec![
            mk(10.0, 1.0, 5.0),
            mk(20.0, 1.0, 7.0),
            mk(30.0, 2.0, 9.0),
            mk(40.0, 2.0, 4.0),
        ],
    );
    // ASC by s0, then DESC by s1:
    //   (1, 7) → 20, (1, 5) → 10, (2, 9) → 30, (2, 4) → 40.
    assert_eq!(extract_num_field(&out, b"v"), vec![20.0, 10.0, 30.0, 40.0]);
}

#[test]
fn heap_no_limit_caps_at_default_limit() {
    // Mirrors `collect::storage::DEFAULT_LIMIT`, hardcoded because it is
    // crate-private.
    const DEFAULT_LIMIT: usize = 10;
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        None,
        (0..15).map(|i| num_row(i as f64)).collect(),
    );
    let got = extract_num_field(&out, b"v");
    assert_eq!(got.len(), DEFAULT_LIMIT);
    assert_eq!(got, (0..10).map(|i| i as f64).collect::<Vec<_>>());
}

#[test]
fn heap_push_pop_max_keeps_top_k_after_eviction() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        Some((0, 3)),
        [9.0, 2.0, 7.0, 4.0, 1.0, 8.0, 3.0, 5.0, 6.0, 0.0]
            .into_iter()
            .map(num_row)
            .collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn heap_ties_keep_earliest_inserts_asc() {
    // Strict-less survival means tied candidates after the heap fills lose
    // to the residents; survivors are the first three inserts in some order.
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        Some((0, 3)),
        (0..5)
            .map(|i| {
                (
                    vec![SharedValue::new_num(i as f64)],
                    vec![SharedValue::new_num(42.0)],
                )
            })
            .collect(),
    );
    let mut ids = extract_num_field(&out, b"v");
    ids.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(ids, vec![0.0, 1.0, 2.0]);
}

#[test]
fn heap_ties_keep_earliest_inserts_desc() {
    // Direction doesn't matter when every comparison is `Equal`.
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b0,
        Some((0, 3)),
        (0..5)
            .map(|i| {
                (
                    vec![SharedValue::new_num(i as f64)],
                    vec![SharedValue::new_num(42.0)],
                )
            })
            .collect(),
    );
    let mut ids = extract_num_field(&out, b"v");
    ids.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(ids, vec![0.0, 1.0, 2.0]);
}

#[test]
fn heap_missing_sort_key_ranks_worst_under_asc_end_to_end() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        Some((0, 3)),
        vec![
            num_row_maybe_null(100.0, None),
            num_row_maybe_null(1.0, Some(1.0)),
            num_row_maybe_null(200.0, None),
            num_row_maybe_null(3.0, Some(3.0)),
            num_row_maybe_null(2.0, Some(2.0)),
        ],
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![1.0, 2.0, 3.0]);
}

#[test]
fn heap_missing_sort_key_ranks_worst_under_desc_end_to_end() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b0,
        Some((0, 3)),
        vec![
            num_row_maybe_null(100.0, None),
            num_row_maybe_null(1.0, Some(1.0)),
            num_row_maybe_null(200.0, None),
            num_row_maybe_null(3.0, Some(3.0)),
            num_row_maybe_null(2.0, Some(2.0)),
        ],
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![3.0, 2.0, 1.0]);
}

#[test]
fn heap_limit_offset_skips_best_entries() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        Some((2, 3)),
        (0..10).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![2.0, 3.0, 4.0]);
}

#[test]
fn heap_eviction_skips_projection_for_losing_rows() {
    // End-to-end check that losing rows don't survive; the closure-call
    // count itself is asserted by the storage-layer unit tests.
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let mut all_rows: Vec<(Vec<SharedValue>, Vec<SharedValue>)> = Vec::new();
    for sort in [1.0_f64, 2.0, 3.0] {
        all_rows.push(num_row(sort));
    }
    for sort in [100.0_f64, 101.0, 102.0, 103.0, 104.0] {
        all_rows.push(num_row(sort));
    }
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        0b1,
        Some((0, 3)),
        all_rows,
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![1.0, 2.0, 3.0]);
}

#[test]
fn array_overflow_skips_projection_beyond_cap() {
    // End-to-end check that the cap holds; the closure-call count itself
    // is asserted by the storage-layer unit tests.
    let v = mk_key(c"v", 0);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        Vec::new().into_boxed_slice(),
        0,
        Some((0, 3)),
        (0..7).map(|i| num_row(i as f64)).collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

// Part B — internal-mode and Local-reducer coverage on top of Part A.

#[test]
fn remote_internal_emits_sort_key_columns_alongside_fields() {
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = vec![&s].into_boxed_slice();
    let r = RemoteCollectReducer::new(
        field_keys.clone(),
        false,
        sort_keys.clone(),
        0b1, // ASC
        Some((0, 3)),
        /* is_internal */ true,
    );
    let mut ctx = RemoteCollectCtx::new(&r);
    for &v in &[5.0_f64, 2.0, 8.0, 1.0, 3.0] {
        let row = make_row(
            &field_keys,
            &sort_keys,
            &[SharedValue::new_num(v * 10.0)],
            &[SharedValue::new_num(v)],
        );
        ctx.add(&r, &row);
    }
    let out = ctx.finalize(&r);
    let rows = array_entries(&out);
    assert_eq!(rows.len(), 3, "top-3 by ASC sort key");
    let projected: Vec<(f64, f64)> = rows
        .iter()
        .map(|sv| {
            let m = map_entries(sv);
            let v = m.get(b"v").and_then(|x| x.as_num()).expect("missing v");
            let s = m.get(b"s").and_then(|x| x.as_num()).expect("missing s");
            (v, s)
        })
        .collect();
    // v == 10 * s for each input row, so the two columns must track each
    // other in the best-3-by-ASC output.
    assert_eq!(
        projected,
        vec![(10.0, 1.0), (20.0, 2.0), (30.0, 3.0)],
        "rows must include sort-key column tied to the same logical row"
    );
}

#[test]
fn remote_internal_mode_does_not_apply_limit_offset_locally() {
    // Regression canary for the contract documented on
    // `RemoteCollectReducer::is_internal`: if a future change rewrites the
    // shard wire's LIMIT to `(0, offset+count)` without flipping that gate,
    // the offset gets dropped twice and this test fails.
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = vec![&s].into_boxed_slice();
    let run_with_is_internal = |is_internal: bool| -> Vec<f64> {
        let r = RemoteCollectReducer::new(
            field_keys.clone(),
            false,
            sort_keys.clone(),
            0b1, // ASC
            Some((5, 3)),
            is_internal,
        );
        let mut ctx = RemoteCollectCtx::new(&r);
        for i in 0..10 {
            let row = make_row(
                &field_keys,
                &sort_keys,
                &[SharedValue::new_num(i as f64)],
                &[SharedValue::new_num(i as f64)],
            );
            ctx.add(&r, &row);
        }
        let out = ctx.finalize(&r);
        extract_num_field(&out, b"v")
    };

    let standalone = run_with_is_internal(false);
    let internal = run_with_is_internal(true);

    assert_eq!(
        standalone,
        vec![5.0, 6.0, 7.0],
        "standalone shard must apply skip(5).take(3) locally"
    );
    assert_eq!(
        internal,
        vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
        "internal shard must NOT apply skip(offset) locally"
    );
}

/// One shard payload (an `Array` of per-row `Map`s) under `input_key`.
fn local_row_with_payload<'a>(
    input_key: &'a RLookupKey<'a>,
    payload: Vec<SharedValue>,
) -> RLookupRow<'a> {
    let mut row = RLookupRow::new();
    row.write_key(input_key, SharedValue::new_array(payload));
    row
}

/// One per-row entry in the RESP3 `Map` shape; the RESP2 flat-pair `Array`
/// shape is exercised by `local_lookup_in_entry_handles_resp2_flat_array`.
fn shard_map_entry(fields: &[(&[u8], SharedValue)]) -> SharedValue {
    SharedValue::new_map(
        fields
            .iter()
            .map(|(name, val)| (SharedValue::new_string(name.to_vec()), val.clone()))
            .collect::<Vec<_>>(),
    )
}

#[test]
fn local_heap_merges_two_shard_payloads_top_k() {
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([b"v".to_vec().into_boxed_slice()]),
        Box::new([b"s".to_vec().into_boxed_slice()]),
        0b1, // ASC
        Some((0, 4)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let mk_entry = |proj: f64, sort: f64| {
        shard_map_entry(&[
            (b"v", SharedValue::new_num(proj)),
            (b"s", SharedValue::new_num(sort)),
        ])
    };
    let shard0 = local_row_with_payload(
        &input,
        vec![
            mk_entry(50.0, 5.0),
            mk_entry(10.0, 1.0),
            mk_entry(70.0, 7.0),
        ],
    );
    ctx.add(&r, &shard0);
    let shard1 = local_row_with_payload(
        &input,
        vec![
            mk_entry(30.0, 3.0),
            mk_entry(20.0, 2.0),
            mk_entry(80.0, 8.0),
            mk_entry(40.0, 4.0),
        ],
    );
    ctx.add(&r, &shard1);

    let out = ctx.finalize(&r);
    assert_eq!(extract_num_field(&out, b"v"), vec![10.0, 20.0, 30.0, 40.0]);
}

#[test]
fn local_heap_default_limit_no_limit() {
    // Mirrors `collect::storage::DEFAULT_LIMIT`, hardcoded because it is
    // crate-private.
    const DEFAULT_LIMIT: usize = 10;
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([b"v".to_vec().into_boxed_slice()]),
        Box::new([b"s".to_vec().into_boxed_slice()]),
        0b1,
        None,
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let payload: Vec<SharedValue> = (0..15)
        .map(|i| {
            shard_map_entry(&[
                (b"v", SharedValue::new_num(i as f64)),
                (b"s", SharedValue::new_num(i as f64)),
            ])
        })
        .collect();
    let row = local_row_with_payload(&input, payload);
    ctx.add(&r, &row);

    let out = ctx.finalize(&r);
    let got = extract_num_field(&out, b"v");
    assert_eq!(got.len(), DEFAULT_LIMIT);
    assert_eq!(got, (0..10).map(|i| i as f64).collect::<Vec<_>>());
}

#[test]
fn local_heap_offset_take() {
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([b"v".to_vec().into_boxed_slice()]),
        Box::new([b"s".to_vec().into_boxed_slice()]),
        0b1, // ASC
        Some((5, 3)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let payload: Vec<SharedValue> = (0..10)
        .map(|i| {
            shard_map_entry(&[
                (b"v", SharedValue::new_num(i as f64)),
                (b"s", SharedValue::new_num(i as f64)),
            ])
        })
        .collect();
    let row = local_row_with_payload(&input, payload);
    ctx.add(&r, &row);

    let out = ctx.finalize(&r);
    assert_eq!(extract_num_field(&out, b"v"), vec![5.0, 6.0, 7.0]);
}

#[test]
fn local_array_limit_concatenates_then_caps() {
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([b"v".to_vec().into_boxed_slice()]),
        Box::new([]),
        0,
        Some((0, 3)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let shard0 = local_row_with_payload(
        &input,
        (0..3)
            .map(|i| shard_map_entry(&[(b"v", SharedValue::new_num(i as f64))]))
            .collect(),
    );
    let shard1 = local_row_with_payload(
        &input,
        (3..5)
            .map(|i| shard_map_entry(&[(b"v", SharedValue::new_num(i as f64))]))
            .collect(),
    );
    ctx.add(&r, &shard0);
    ctx.add(&r, &shard1);

    let out = ctx.finalize(&r);
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn local_lookup_in_entry_handles_resp2_flat_array() {
    // RESP2 serialises remote rows as flat `[k, v, k, v, …]` arrays.
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([
            b"v".to_vec().into_boxed_slice(),
            // Requested but absent from every payload, so each output row's
            // `missing` slot must materialise as the static null sentinel.
            b"missing".to_vec().into_boxed_slice(),
        ]),
        Box::new([b"s".to_vec().into_boxed_slice()]),
        0b1, // ASC
        Some((0, 3)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let mk_flat = |proj: f64, sort: f64| {
        SharedValue::new_array([
            SharedValue::new_string(b"v".to_vec()),
            SharedValue::new_num(proj),
            SharedValue::new_string(b"s".to_vec()),
            SharedValue::new_num(sort),
        ])
    };
    let payload = vec![mk_flat(50.0, 5.0), mk_flat(10.0, 1.0), mk_flat(30.0, 3.0)];
    let row = local_row_with_payload(&input, payload);
    ctx.add(&r, &row);

    let out = ctx.finalize(&r);
    let rows = array_entries(&out);
    assert_eq!(rows.len(), 3, "top-3 ASC by s");

    let projected_v: Vec<f64> = rows
        .iter()
        .map(|sv| map_entries(sv).get(b"v").and_then(|v| v.as_num()).unwrap())
        .collect();
    assert_eq!(projected_v, vec![10.0, 30.0, 50.0]);

    for sv in rows {
        let m = map_entries(sv);
        let missing = m
            .get(b"missing")
            .expect("`missing` key must be present in the output map");
        assert!(missing.is_null_static());
        assert!(
            m.get(b"s").is_none(),
            "local reducer must not emit sort-key columns"
        );
    }
}
