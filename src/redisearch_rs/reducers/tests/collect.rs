/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;

use reducers::collect::{
    LocalCollectCtx, LocalCollectReducer, RemoteCollectCtx, RemoteCollectReducer,
};
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{Map, SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn key(name: &'static std::ffi::CStr, dstidx: u16) -> RLookupKey<'static> {
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
            name_key: key(c"name", 0),
            sweetness_key: key(c"sweetness", 1),
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
            false,
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
        false,
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
fn local_collect_projects_remote_maps_and_fills_missing_fields_with_null() {
    let input_key = key(c"generatedalias", 0);
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
    assert!(row.get(b"missing").unwrap().is_null_static());
}

#[test]
fn local_collect_accepts_resp2_flat_array_payloads() {
    let input_key = key(c"generatedalias", 0);
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
