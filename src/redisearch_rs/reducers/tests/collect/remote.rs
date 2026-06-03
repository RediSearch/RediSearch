/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::collect::{RemoteCollectCtx, RemoteCollectReducer};
use rlookup::RLookupRow;
use value::SharedValue;

use super::helpers::{
    RemoteCollectFixture, RemoteCollectLoadAllFixture, array_entries, map_entries, string_value,
};

#[test]
fn remote_external_collect_emits_only_requested_fields() {
    let fixture = RemoteCollectFixture::new();
    let reducer = fixture.reducer(false);
    let mut ctx = RemoteCollectCtx::new(&reducer);
    let row = fixture.row("apple", 10.0);

    ctx.add(&reducer, &row, 0);
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

    ctx.add(&reducer, &row, 0);
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
        true, // is_internal
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);
    let row = fixture.row("apple", 10.0);

    ctx.add(&reducer, &row, 0);
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
fn remote_collect_uses_raw_doc_id_to_break_equal_sortby_values() {
    // Models the C-side flow: the grouper passes doc ids out-of-band to the
    // remote COLLECT reducer, and the heap comparator uses them only after all
    // user sort keys compare equal.
    let fixture = RemoteCollectFixture::new();
    let reducer = RemoteCollectReducer::new(
        Box::new([&fixture.name_key]),
        None,
        Box::new([&fixture.sweetness_key]),
        0b1,
        Some((0, 2)),
        false,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    let larger_doc_id: ffi::t_docId = (1_u64 << 53) + 1;
    let smaller_doc_id: ffi::t_docId = 1_u64 << 53;

    let larger = fixture.row("larger-docid", 10.0);
    let smaller = fixture.row("smaller-docid", 10.0);

    // Insert worse-first so a broken comparator would surface as wrong order.
    ctx.add(&reducer, &larger, larger_doc_id);
    ctx.add(&reducer, &smaller, smaller_doc_id);

    let output = ctx.finalize(&reducer);
    let rows = array_entries(&output);
    assert_eq!(rows.len(), 2);
    let names: Vec<&[u8]> = rows
        .iter()
        .map(|r| {
            map_entries(r)
                .get(b"name")
                .and_then(|v| v.as_str_bytes())
                .expect("row must carry a `name` field")
        })
        .collect();
    assert_eq!(
        names,
        [b"smaller-docid".as_slice(), b"larger-docid".as_slice()]
    );
}

#[test]
fn remote_finalize_hoists_name_allocations() {
    let fixture = RemoteCollectFixture::new();
    let reducer = fixture.reducer(false);
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &fixture.row("apple", 10.0), 0);
    ctx.add(&reducer, &fixture.row("banana", 20.0), 0);
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
fn remote_external_omits_keys_missing_on_row() {
    let fixture = RemoteCollectFixture::new();
    let reducer = RemoteCollectReducer::new(
        Box::new([&fixture.name_key, &fixture.sweetness_key]),
        None,
        Box::new([]),
        0,
        Some((0, 100)),
        false,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    let row_a = fixture.row("apple", 4.0);
    let mut row_b = RLookupRow::new();
    row_b.write_key(&fixture.name_key, string_value("lemon"));

    ctx.add(&reducer, &row_a, 0);
    ctx.add(&reducer, &row_b, 0);
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
fn remote_load_all_emits_all_lookup_keys_present_on_row() {
    let mut fixture = RemoteCollectLoadAllFixture::new();
    let mut row = RLookupRow::new();
    row.write_key_by_name(&mut fixture.lookup, c"name", string_value("apple"));
    row.write_key_by_name(&mut fixture.lookup, c"color", string_value("red"));
    row.write_key_by_name(&mut fixture.lookup, c"sweetness", SharedValue::new_num(4.0));

    let reducer = RemoteCollectReducer::new(
        Box::new([]),
        Some(&fixture.lookup),
        Box::new([]),
        0,
        Some((0, 100)),
        false,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row, 0);
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

    let reducer = RemoteCollectReducer::new(
        Box::new([]),
        Some(&fixture.lookup),
        Box::new([]),
        0,
        Some((0, 100)),
        false,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row_a, 0);
    ctx.add(&reducer, &row_b, 0);
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

    let reducer = RemoteCollectReducer::new(
        Box::new([]),
        Some(&fixture.lookup),
        Box::new([]),
        0,
        Some((0, 100)),
        false,
        false,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    ctx.add(&reducer, &row, 0);
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
