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
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupRow};
use value::{Map, SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Distinct keys in the same row need distinct `dstidx`es so they don't
/// alias the same `RLookupRow` slot.
fn mk_key(name: &'static CStr, dstidx: u16) -> RLookupKey<'static> {
    let mut key = RLookupKey::new(name, RLookupKeyFlags::empty());
    key.dstidx = dstidx;
    key
}

fn string_value(s: &str) -> SharedValue {
    SharedValue::new_string(s.as_bytes().to_vec())
}

fn map_entries(value: &SharedValue) -> &Map {
    let Value::Map(map) = &**value else {
        panic!("expected map, got {value:?}");
    };
    map
}

fn array_entries(value: &SharedValue) -> &[SharedValue] {
    let Value::Array(array) = &**value else {
        panic!("expected array, got {value:?}");
    };
    array
}

struct RemoteCollectFixture {
    name_key: RLookupKey<'static>,
    sweetness_key: RLookupKey<'static>,
}

impl RemoteCollectFixture {
    fn new() -> Self {
        Self {
            name_key: mk_key(c"name", 0),
            sweetness_key: mk_key(c"sweetness", 1),
        }
    }

    /// Builds the remote half of:
    ///
    /// REDUCE COLLECT 6
    ///   FIELDS 1 @name
    ///   SORTBY 1 @sweetness
    fn reducer(&self, include_sort_keys: bool) -> RemoteCollectReducer<'_> {
        RemoteCollectReducer::new(
            Box::new([&self.name_key]),
            None,
            Box::new([&self.sweetness_key]),
            0,
            None,
            include_sort_keys,
        )
    }

    fn row(&self, name: &str, sweetness: f64) -> RLookupRow<'_> {
        let mut row = RLookupRow::new();
        row.write_key(&self.name_key, string_value(name));
        row.write_key(&self.sweetness_key, SharedValue::new_num(sweetness));
        row
    }
}

#[test]
fn remote_external_collect_emits_only_requested_fields() {
    let fixture = RemoteCollectFixture::new();
    let reducer = fixture.reducer(false);
    let mut ctx = RemoteCollectCtx::new(&reducer);
    let row = fixture.row("apple", 10.0);

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let row = map_entries(&rows[0]);
    assert_eq!(
        row.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    assert!(row.get(b"sweetness").is_none());
}

#[test]
fn remote_internal_collect_includes_sort_fields_for_coordinator_merge() {
    let fixture = RemoteCollectFixture::new();
    let reducer = fixture.reducer(true);
    let mut ctx = RemoteCollectCtx::new(&reducer);
    let row = fixture.row("apple", 10.0);

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let row = map_entries(&rows[0]);
    assert_eq!(
        row.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    let sweetness = row.get(b"sweetness").unwrap();
    assert_eq!(sweetness.as_num(), Some(10.0));
}

#[test]
fn remote_finalize_dedupes_overlapping_field_and_sort_key() {
    let fixture = RemoteCollectFixture::new();
    // Both `field_keys` and `sort_keys` reference the same `name_key`
    // (same dstidx 0), with internal mode on so sort keys participate in
    // emission.
    let reducer = RemoteCollectReducer::new(
        Box::new([&fixture.name_key]),
        None,
        Box::new([&fixture.name_key]),
        0,
        None,
        true, // include_sort_keys
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);
    let row = fixture.row("apple", 10.0);

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let map = map_entries(&rows[0]);
    assert_eq!(
        map.len(),
        1,
        "overlapping field/sort key must be emitted exactly once"
    );
    assert_eq!(
        map.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
}

#[test]
fn remote_external_omits_keys_missing_on_row() {
    let fixture = RemoteCollectFixture::new();
    let reducer = RemoteCollectReducer::new(
        Box::new([&fixture.name_key, &fixture.sweetness_key]),
        None,
        Box::new([]),
        0,
        None,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    let row_a = fixture.row("apple", 4.0);
    let mut row_b = RLookupRow::new();
    row_b.write_key(&fixture.name_key, string_value("lemon"));

    ctx.add(&reducer, &row_a);
    ctx.add(&reducer, &row_b);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 2);

    let map_a = map_entries(&rows[0]);
    assert_eq!(
        map_a.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice()),
    );
    assert_eq!(map_a.get(b"sweetness").and_then(|v| v.as_num()), Some(4.0));

    let map_b = map_entries(&rows[1]);
    assert_eq!(
        map_b.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"lemon".as_slice()),
    );
    assert!(
        map_b.get(b"sweetness").is_none(),
        "row B was missing `sweetness`; the requested-fields map must omit the entry entirely"
    );
}

#[test]
fn remote_finalize_hoists_name_allocations() {
    let fixture = RemoteCollectFixture::new();
    let reducer = fixture.reducer(false);
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &fixture.row("apple", 10.0));
    ctx.add(&reducer, &fixture.row("banana", 20.0));
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 2);

    let map0 = map_entries(&rows[0]);
    let map1 = map_entries(&rows[1]);
    assert_eq!(map0.len(), 1);
    assert_eq!(map1.len(), 1);

    // The "name" key SharedValue should be the same Arc across rows
    // (allocated once per `finalize`, cloned per row).
    let name_key0 = &map0[0].0;
    let name_key1 = &map1[0].0;
    assert!(
        SharedValue::ptr_eq(name_key0, name_key1),
        "field-name SharedValue must be allocated once per finalize and shared across rows"
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn local_collect_projects_remote_maps_and_fills_missing_fields_with_null() {
    let input_key = mk_key(c"generatedalias", 0);
    let reducer = LocalCollectReducer::new(
        &input_key,
        Box::new([
            b"name".to_vec().into_boxed_slice(),
            b"missing".to_vec().into_boxed_slice(),
        ]),
        false,
        Box::new([]),
        0,
        None,
    );
    let mut ctx = LocalCollectCtx::new(&reducer);

    let remote_row = SharedValue::new_map([
        (string_value("name"), string_value("apple")),
        (string_value("sweetness"), SharedValue::new_num(10.0)),
    ]);
    let mut row = RLookupRow::new();
    row.write_key(&input_key, SharedValue::new_array([remote_row]));

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let row = map_entries(&rows[0]);
    assert_eq!(
        row.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    assert!(row.get(b"sweetness").is_none());
    assert!(
        row.get(b"missing")
            .expect("requested key absent from payload must be null-filled")
            .is_null_static()
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn local_collect_accepts_resp2_flat_array_payloads() {
    let input_key = mk_key(c"generatedalias", 0);
    let reducer = LocalCollectReducer::new(
        &input_key,
        Box::new([b"name".to_vec().into_boxed_slice()]),
        false,
        Box::new([]),
        0,
        None,
    );
    let mut ctx = LocalCollectCtx::new(&reducer);

    let remote_row = SharedValue::new_array([
        string_value("name"),
        string_value("apple"),
        string_value("sweetness"),
        SharedValue::new_num(10.0),
    ]);
    let mut row = RLookupRow::new();
    row.write_key(&input_key, SharedValue::new_array([remote_row]));

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let row = map_entries(&rows[0]);
    assert_eq!(
        row.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    assert!(row.get(b"sweetness").is_none());
}

/// Fixture for load-all (`FIELDS *`) tests. Owns a real [`RLookup`] so the
/// reducer's live walk has something to iterate. Pre-registers three visible
/// keys (`name`, `color`, `sweetness`) plus one [`RLookupKeyFlag::Hidden`]
/// key (`__hidden`) so the "skip hidden" assertion has a target.
struct RemoteCollectLoadAllFixture {
    lookup: RLookup<'static>,
}

impl RemoteCollectLoadAllFixture {
    fn new() -> Self {
        let mut lookup = RLookup::new();
        let _ = lookup
            .get_key_write(c"name", RLookupKeyFlags::empty())
            .expect("`name` is a fresh key");
        let _ = lookup
            .get_key_write(c"color", RLookupKeyFlags::empty())
            .expect("`color` is a fresh key");
        let _ = lookup
            .get_key_write(c"sweetness", RLookupKeyFlags::empty())
            .expect("`sweetness` is a fresh key");
        let _ = lookup
            .get_key_write(c"__hidden", RLookupKeyFlag::Hidden.into())
            .expect("`__hidden` is a fresh key");
        Self { lookup }
    }

    /// Build a reducer with `srclookup = Some(&self.lookup)` and no explicit
    /// field/sort keys (matching the parser's "load-all alone" assumption).
    fn reducer(&self) -> RemoteCollectReducer<'_> {
        RemoteCollectReducer::new(
            Box::new([]),
            Some(&self.lookup),
            Box::new([]),
            0,
            None,
            false,
        )
    }
}

#[test]
fn remote_load_all_emits_all_lookup_keys_present_on_row() {
    let mut fixture = RemoteCollectLoadAllFixture::new();
    let mut row = RLookupRow::new();
    row.write_key_by_name(&mut fixture.lookup, c"name", string_value("apple"));
    row.write_key_by_name(&mut fixture.lookup, c"color", string_value("red"));
    row.write_key_by_name(&mut fixture.lookup, c"sweetness", SharedValue::new_num(4.0));

    let reducer = fixture.reducer();
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let map = map_entries(&rows[0]);
    assert_eq!(
        map.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    assert_eq!(
        map.get(b"color").and_then(|v| v.as_str_bytes()),
        Some(b"red".as_slice())
    );
    assert_eq!(map.get(b"sweetness").and_then(|v| v.as_num()), Some(4.0));
    assert!(
        map.get(b"__hidden").is_none(),
        "Hidden keys must never be emitted"
    );
}

#[test]
fn remote_load_all_omits_keys_missing_on_row() {
    let mut fixture = RemoteCollectLoadAllFixture::new();

    let mut row_a = RLookupRow::new();
    row_a.write_key_by_name(&mut fixture.lookup, c"name", string_value("apple"));
    row_a.write_key_by_name(&mut fixture.lookup, c"color", string_value("red"));
    row_a.write_key_by_name(&mut fixture.lookup, c"sweetness", SharedValue::new_num(4.0));

    // Row B is missing `color` entirely — the load-all map must drop the
    // entry instead of padding with `null_static`.
    let mut row_b = RLookupRow::new();
    row_b.write_key_by_name(&mut fixture.lookup, c"name", string_value("lemon"));
    row_b.write_key_by_name(&mut fixture.lookup, c"sweetness", SharedValue::new_num(2.0));

    let reducer = fixture.reducer();
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row_a);
    ctx.add(&reducer, &row_b);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 2);

    let map_a = map_entries(&rows[0]);
    assert_eq!(
        map_a.get(b"color").and_then(|v| v.as_str_bytes()),
        Some(b"red".as_slice()),
        "row A had `color`; the load-all map must include it"
    );

    let map_b = map_entries(&rows[1]);
    assert!(
        map_b.get(b"color").is_none(),
        "row B was missing `color`; the load-all map must omit the entry entirely (no null_static padding)"
    );
    assert_eq!(
        map_b.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"lemon".as_slice())
    );
    assert_eq!(map_b.get(b"sweetness").and_then(|v| v.as_num()), Some(2.0));
}

#[test]
fn remote_load_all_skips_hidden_keys_even_when_row_has_value() {
    let mut fixture = RemoteCollectLoadAllFixture::new();
    let mut row = RLookupRow::new();
    row.write_key_by_name(&mut fixture.lookup, c"name", string_value("apple"));
    // Populate the Hidden key on the row to prove the filter happens at the
    // lookup-walk level, not at "no value" — the value is present.
    row.write_key_by_name(&mut fixture.lookup, c"__hidden", string_value("internal"));

    let reducer = fixture.reducer();
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row);
    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 1);

    let map = map_entries(&rows[0]);
    assert_eq!(
        map.get(b"name").and_then(|v| v.as_str_bytes()),
        Some(b"apple".as_slice())
    );
    assert!(
        map.get(b"__hidden").is_none(),
        "Hidden keys must be excluded from the load-all emission template"
    );
}

// ---------------------------------------------------------------------------
// Free helpers used by the LIMIT-focused tests below.
// ---------------------------------------------------------------------------

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
/// (`include_sort_keys = false`) [`RemoteCollectReducer`].
fn run_collect(
    field_keys: Box<[&RLookupKey<'_>]>,
    sort_keys: Box<[&RLookupKey<'_>]>,
    sort_asc_map: u64,
    limit: Option<(u64, u64)>,
    rows: Vec<(Vec<SharedValue>, Vec<SharedValue>)>,
) -> SharedValue {
    let r = RemoteCollectReducer::new(
        field_keys.clone(),
        None,
        sort_keys.clone(),
        sort_asc_map,
        limit,
        /* include_sort_keys */ false,
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

// Part A — pre-merge tests, ported onto `RemoteCollectReducer`.

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
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
fn remote_internal_mode_does_not_apply_limit_offset_locally() {
    // Regression canary for the contract documented on
    // `RemoteCollectReducer::include_sort_keys`: if a future change rewrites
    // the shard wire's LIMIT to `(0, offset+count)` without flipping that
    // gate, the offset gets dropped twice and this test fails.
    let v = mk_key(c"v", 0);
    let s = mk_key(c"s", 1);
    let field_keys: Box<[&RLookupKey]> = vec![&v].into_boxed_slice();
    let sort_keys: Box<[&RLookupKey]> = vec![&s].into_boxed_slice();
    let run_with_include_sort_keys = |include_sort_keys: bool| -> Vec<f64> {
        let r = RemoteCollectReducer::new(
            field_keys.clone(),
            None,
            sort_keys.clone(),
            0b1, // ASC
            Some((5, 3)),
            include_sort_keys,
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

    let standalone = run_with_include_sort_keys(false);
    let internal = run_with_include_sort_keys(true);

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
fn local_array_limit_concatenates_then_caps() {
    let input = mk_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Box::new([b"v".to_vec().into_boxed_slice()]),
        false,
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
        false,
        Box::new([]),
        0,
        Some((0, 3)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let mk_flat = |proj: f64| {
        SharedValue::new_array([
            SharedValue::new_string(b"v".to_vec()),
            SharedValue::new_num(proj),
        ])
    };
    let payload = vec![mk_flat(50.0), mk_flat(10.0), mk_flat(30.0)];
    let row = local_row_with_payload(&input, payload);
    ctx.add(&r, &row);

    let out = ctx.finalize(&r);
    let rows = array_entries(&out);
    assert_eq!(rows.len(), 3, "first 3 in insertion order");

    let projected_v: Vec<f64> = rows
        .iter()
        .map(|sv| map_entries(sv).get(b"v").and_then(|v| v.as_num()).unwrap())
        .collect();
    assert_eq!(projected_v, vec![50.0, 10.0, 30.0]);

    for sv in rows {
        let m = map_entries(sv);
        let missing = m
            .get(b"missing")
            .expect("`missing` key must be present in the output map");
        assert!(missing.is_null_static());
    }
}
