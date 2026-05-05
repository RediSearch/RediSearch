/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use reducers::collect::{LocalCollectCtx, LocalCollectReducer};
use rlookup::{RLookupKey, RLookupRow};
use value::SharedValue;

use super::helpers::{array_entries, extract_num_field, make_key, map_entries, string_value};

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn local_collect_projects_remote_maps_and_omits_missing_fields() {
    let input_key = make_key(c"generatedalias", 0);
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
    assert!(row.get(b"missing").is_none());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn local_collect_accepts_resp2_flat_array_payloads() {
    let input_key = make_key(c"generatedalias", 0);
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
    let input = make_key(c"__shard_payload", 0);
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
    let input = make_key(c"__shard_payload", 0);
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
        assert!(m.get(b"missing").is_none());
    }
}
