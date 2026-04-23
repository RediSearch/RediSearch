/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! End-to-end tests for the COLLECT reducer that drive the public
//! `CollectReducer` / `CollectCtx` surface through `insert_entry` →
//! `finalize`. Pure comparator tests on `EntryKey::cmp` (which touches
//! private module internals) live inline in `reducers/src/collect.rs`.
//!
//! Array-path coverage: SORTBY absent. Heap-path coverage: SORTBY present,
//! exercising `push_pop_max` eviction, `DEFAULT_LIMIT` fallback,
//! missing-worst policy at the pipeline level, and tie-handling (ties
//! compare equal — survivors are unspecified among tied entries). The
//! array-path `RSGlobalConfig.maxAggregateResults` cap is intentionally
//! deferred to the Python E2E tests (Task 11) because mutating the
//! process-global would require serialising Rust tests.

use std::ffi::CStr;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use reducers::collect::{CollectCtx, CollectReducer};
use rlookup::{RLookupKey, RLookupKeyFlags};
use value::{SharedValue, Value};

/// Build a single [`RLookupKey`] borrowing a static C-string name.
fn mk_key(name: &'static CStr) -> RLookupKey<'static> {
    RLookupKey::new(name, RLookupKeyFlags::empty())
}

/// Drive a full `insert_entry` → `finalize` cycle and return the
/// resulting [`SharedValue`] (always `Value::Array<Value::Map>`).
fn run_collect(
    field_keys: Box<[&RLookupKey<'_>]>,
    sort_keys: Box<[&RLookupKey<'_>]>,
    sort_asc_map: u64,
    limit: Option<(u64, u64)>,
    rows: Vec<(Vec<SharedValue>, Vec<SharedValue>)>,
) -> SharedValue {
    let r = CollectReducer::new(field_keys, false, sort_keys, sort_asc_map, limit);
    let mut ctx = CollectCtx::new(&r);
    for (projected, sort_vals) in rows {
        ctx.insert_entry(&r, &sort_vals, || projected.into_boxed_slice());
    }
    ctx.finalize(&r)
}

/// Extract the numeric value stored under `name` in each row-map of the
/// finalised [`Value::Array<Value::Map>`].
fn extract_num_field(out: &SharedValue, name: &[u8]) -> Vec<f64> {
    let array = match &**out {
        Value::Array(a) => a,
        v => panic!("expected Value::Array, got {}", v.variant_name()),
    };
    array
        .iter()
        .map(|row_sv| {
            let map = match &**row_sv {
                Value::Map(m) => m,
                v => panic!("expected Value::Map, got {}", v.variant_name()),
            };
            map.get(name)
                .and_then(|v| v.as_num())
                .expect("missing or non-numeric field")
        })
        .collect()
}

/// Build a one-column `projected`/`sort_vals` row where both columns hold
/// the same numeric value. The sort column is elided on the array path.
fn num_row(v: f64) -> (Vec<SharedValue>, Vec<SharedValue>) {
    (vec![SharedValue::new_num(v)], vec![SharedValue::new_num(v)])
}

/// Row variant with a distinct projected identifier and a possibly-null
/// sort value. Used for the end-to-end "missing ranks worst" tests.
fn num_row_maybe_null(projected: f64, sort: Option<f64>) -> (Vec<SharedValue>, Vec<SharedValue>) {
    let sort_sv = match sort {
        Some(v) => SharedValue::new_num(v),
        None => SharedValue::null_static(),
    };
    (vec![SharedValue::new_num(projected)], vec![sort_sv])
}

#[test]
fn array_no_sortby_no_limit_preserves_insertion_order() {
    let v = mk_key(c"v");
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
    let v = mk_key(c"v");
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
    let v = mk_key(c"v");
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
    let v = mk_key(c"v");
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
    let v = mk_key(c"v");
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
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    let v = mk_key(c"v");
    let s0 = mk_key(c"s0");
    let s1 = mk_key(c"s1");
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
    // Mirrors `DEFAULT_LIMIT` in `collect.rs`; hardcoded here because
    // the constant is module-private.
    const DEFAULT_LIMIT: usize = 10;
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // Insert strictly more than `offset + count` entries and verify the
    // surviving set is the `count` best-by-sort; this exercises the
    // `push_pop_max` branch in `insert_entry`.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // All sort values equal → ties compare `Equal` and the strict-less
    // survival check rejects every candidate after the heap fills. So
    // the three survivors are the first three inserts (ids 0, 1, 2),
    // though their emitted order is unspecified.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // DESC on all-equal sort values: the direction does not matter
    // because every comparison is `Equal`. Survivors are still the
    // first three inserts; emitted order is unspecified.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // Mixed null/present sort values; LIMIT 0 3 must pick the three
    // smallest present values and drop all nulls.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // SORTBY ASC LIMIT 2 3 ⇒ skip the two smallest, return the next
    // three in ascending order.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
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
    // SORTBY ASC LIMIT 0 3 fills the heap with 3 winning rows (sort vals
    // 1, 2, 3). The next 5 rows carry large sort vals (100..=104) that
    // cannot beat the current `peek_max`, so `insert_entry` must drop
    // them without ever invoking the projection closure.
    let v = mk_key(c"v");
    let s = mk_key(c"s");
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = vec![&s].into_boxed_slice();
    let r = CollectReducer::new(field_keys, false, sort_keys, 0b1, Some((0, 3)));
    let mut ctx = CollectCtx::new(&r);
    let counter = AtomicUsize::new(0);

    for sort in [1.0, 2.0, 3.0] {
        let sort_vals = vec![SharedValue::new_num(sort)];
        ctx.insert_entry(&r, &sort_vals, || {
            counter.fetch_add(1, AtomicOrdering::SeqCst);
            vec![SharedValue::new_num(sort)].into_boxed_slice()
        });
    }
    assert_eq!(
        counter.load(AtomicOrdering::SeqCst),
        3,
        "cap-filling rows must each invoke the projection closure"
    );

    for sort in [100.0, 101.0, 102.0, 103.0, 104.0] {
        let sort_vals = vec![SharedValue::new_num(sort)];
        ctx.insert_entry(&r, &sort_vals, || {
            counter.fetch_add(1, AtomicOrdering::SeqCst);
            vec![SharedValue::new_num(sort)].into_boxed_slice()
        });
    }
    assert_eq!(
        counter.load(AtomicOrdering::SeqCst),
        3,
        "losing rows must not invoke the projection closure"
    );
}

#[test]
fn array_overflow_skips_projection_beyond_cap() {
    // No SORTBY ⇒ array path; LIMIT 0 3 caps the array at 3 entries.
    // The first 3 inserts project; subsequent inserts overflow the cap
    // and must drop before the projection closure runs.
    let v = mk_key(c"v");
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = Vec::new().into_boxed_slice();
    let r = CollectReducer::new(field_keys, false, sort_keys, 0, Some((0, 3)));
    let mut ctx = CollectCtx::new(&r);
    let counter = AtomicUsize::new(0);

    for i in 0..7u64 {
        let sort_vals: Vec<SharedValue> = Vec::new();
        ctx.insert_entry(&r, &sort_vals, || {
            counter.fetch_add(1, AtomicOrdering::SeqCst);
            vec![SharedValue::new_num(i as f64)].into_boxed_slice()
        });
    }
    assert_eq!(
        counter.load(AtomicOrdering::SeqCst),
        3,
        "array path must not invoke projection beyond cap"
    );
}
