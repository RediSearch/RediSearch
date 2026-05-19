/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! SORTBY (heap-path) coverage for the COLLECT reducer.

use reducers::collect::{
    LocalCollectCtx, LocalCollectReducer, RemoteCollectCtx, RemoteCollectReducer,
};
use rlookup::RLookupRow;
use std::ffi::CString;
use value::SharedValue;

use super::helpers::{
    array_entries, extract_num_field, make_key, make_row, map_entries, num_row, run_collect,
    string_value,
};
use crate::common::{SORT_ASC, SORT_DESC};

#[test]
fn remote_sortby_asc_keeps_smallest_top_k() {
    let v = make_key(c"v", 0);
    let s = make_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        SORT_ASC,
        Some((0, 3)),
        [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0]
            .iter()
            .map(|n| num_row(*n))
            .collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 1.0, 2.0]);
}

#[test]
fn remote_sortby_desc_keeps_largest_top_k() {
    let v = make_key(c"v", 0);
    let s = make_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        SORT_DESC,
        Some((0, 3)),
        [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0]
            .iter()
            .map(|n| num_row(*n))
            .collect(),
    );
    assert_eq!(extract_num_field(&out, b"v"), vec![5.0, 4.0, 3.0]);
}

#[test]
fn remote_sortby_applies_offset_after_sort() {
    let v = make_key(c"v", 0);
    let s = make_key(c"s", 1);
    let out = run_collect(
        vec![&v].into_boxed_slice(),
        vec![&s].into_boxed_slice(),
        SORT_ASC,
        Some((1, 2)),
        [4.0_f64, 1.0, 5.0, 2.0, 0.0, 3.0]
            .iter()
            .map(|n| num_row(*n))
            .collect(),
    );
    // Top-3 ASC = [0, 1, 2]; skip(1).take(2) → [1, 2].
    assert_eq!(extract_num_field(&out, b"v"), vec![1.0, 2.0]);
}

#[test]
fn remote_sortby_mixed_directions() {
    // Two sort keys: sort key 0 ASC, sort key 1 DESC.
    // Bit 0 set, bit 1 clear → 0b01 = 1.
    let v = make_key(c"v", 0);
    let s0 = make_key(c"s0", 1);
    let s1 = make_key(c"s1", 2);

    let r = RemoteCollectReducer::new(
        vec![&v].into_boxed_slice(),
        None,
        vec![&s0, &s1].into_boxed_slice(),
        0b01,
        Some((0, 3)),
        false,
        None,
    );
    let mut ctx = RemoteCollectCtx::new(&r);

    // Tied on s0=1.0; tiebreak by s1 DESC → larger s1 wins.
    // Tied on s0=2.0; tiebreak by s1 DESC → larger s1 wins.
    let entries: &[(f64, f64, f64)] = &[
        (10.0, 1.0, 5.0), // s0=1, s1=5
        (20.0, 1.0, 9.0), // s0=1, s1=9 — best so far (smaller s0 group, then larger s1)
        (30.0, 2.0, 7.0), // s0=2, s1=7
        (40.0, 2.0, 3.0), // s0=2, s1=3
        (50.0, 0.0, 1.0), // s0=0, s1=1 — lowest s0 wins outright
    ];
    for (vv, s0v, s1v) in entries {
        let mut row = RLookupRow::new();
        row.write_key(&v, SharedValue::new_num(*vv));
        row.write_key(&s0, SharedValue::new_num(*s0v));
        row.write_key(&s1, SharedValue::new_num(*s1v));
        ctx.add(&r, &row);
    }
    let out = ctx.finalize(&r);
    // Expected best→worst: (s0=0,s1=1), (s0=1,s1=9), (s0=1,s1=5).
    assert_eq!(extract_num_field(&out, b"v"), vec![50.0, 20.0, 10.0]);
}

#[test]
fn remote_sortby_internal_emits_sort_snapshot() {
    let v = make_key(c"v", 0);
    let s = make_key(c"s", 1);
    let r = RemoteCollectReducer::new(
        vec![&v].into_boxed_slice(),
        None,
        vec![&s].into_boxed_slice(),
        SORT_ASC,
        Some((0, 2)),
        true, // is_internal
        None,
    );
    let mut ctx = RemoteCollectCtx::new(&r);
    for n in [3.0_f64, 1.0, 4.0, 2.0] {
        let row = make_row(
            &[&v],
            &[&s],
            &[SharedValue::new_num(n * 10.0)],
            &[SharedValue::new_num(n)],
        );
        ctx.add(&r, &row);
    }
    let out = ctx.finalize(&r);
    let rows = array_entries(&out);
    assert_eq!(rows.len(), 2, "internal-mode emits the cap, not the slice");

    // Top-2 ASC: s=1 (v=10), s=2 (v=20). The `s` column on each emitted
    // row must come from the heap's sort-key snapshot.
    let pairs: Vec<(f64, f64)> = rows
        .iter()
        .map(|sv| {
            let m = map_entries(sv);
            (
                m.get(b"v").and_then(|x| x.as_num()).unwrap(),
                m.get(b"s").and_then(|x| x.as_num()).unwrap(),
            )
        })
        .collect();
    assert_eq!(pairs, vec![(10.0, 1.0), (20.0, 2.0)]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "reads `ffi::RSGlobalConfig` extern static, unsupported by miri"
)]
fn local_sortby_keeps_top_k_across_shards() {
    let input = make_key(c"__shard_payload", 0);
    let r = LocalCollectReducer::new(
        &input,
        Some(Box::new([CString::new("v").unwrap()])),
        Box::new([CString::new("s").unwrap()]),
        SORT_ASC,
        Some((0, 3)),
    );
    let mut ctx = LocalCollectCtx::new(&r);

    let shard_map_entry = |fields: &[(&[u8], SharedValue)]| -> SharedValue {
        SharedValue::new_map(
            fields
                .iter()
                .map(|(name, val)| {
                    (
                        string_value(std::str::from_utf8(name).unwrap()),
                        val.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };
    let local_row_with_payload = |payload: Vec<SharedValue>| -> RLookupRow<'_> {
        let mut row = RLookupRow::new();
        row.write_key(&input, SharedValue::new_array(payload));
        row
    };
    let make_payload = |entries: &[(f64, f64)]| -> Vec<SharedValue> {
        entries
            .iter()
            .map(|(vv, sv)| {
                shard_map_entry(&[
                    (b"v", SharedValue::new_num(*vv)),
                    (b"s", SharedValue::new_num(*sv)),
                ])
            })
            .collect()
    };
    // Shard 0: (v=40,s=4), (v=10,s=1), (v=50,s=5)
    // Shard 1: (v=20,s=2), (v=00,s=0), (v=30,s=3)
    // Combined top-3 ASC by s: s=0 (v=00), s=1 (v=10), s=2 (v=20).
    let shard0 = local_row_with_payload(make_payload(&[(40.0, 4.0), (10.0, 1.0), (50.0, 5.0)]));
    let shard1 = local_row_with_payload(make_payload(&[(20.0, 2.0), (0.0, 0.0), (30.0, 3.0)]));
    ctx.add(&r, &shard0);
    ctx.add(&r, &shard1);

    let out = ctx.finalize(&r);
    assert_eq!(extract_num_field(&out, b"v"), vec![0.0, 10.0, 20.0]);
}
