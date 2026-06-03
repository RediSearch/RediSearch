/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Unit tests for the DISTINCT dedup key
//! ([`reducers::collect::distinct`]): the canonical byte encoding that backs
//! `DistinctRow`'s `Hash`/`Eq`, and the `Hash`/`Eq` contract over real rows.

extern crate redisearch_rs;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use std::cell::Cell;
use std::ffi::CStr;

use reducers::collect::distinct::{DistinctRow, encode_value_refs, encode_values};
use reducers::collect::storage::Storage;
use reducers::collect::{RemoteCollectCtx, RemoteCollectReducer};
use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::{SharedValue, Value};

redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// `sort_asc_map` for a single DESC sort key (bit 0 clear): larger sort value
/// is "better" (ranks first).
const SORT_DESC: u64 = 0b0;

fn s(bytes: &str) -> Option<SharedValue> {
    Some(SharedValue::new_string(bytes.as_bytes().to_vec()))
}

fn n(v: f64) -> Option<SharedValue> {
    Some(SharedValue::new_num(v))
}

fn enc(values: &[Option<SharedValue>]) -> Box<[u8]> {
    encode_values(values)
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn identical_rows_collapse() {
    assert_eq!(enc(&[s("apple"), n(5.0)]), enc(&[s("apple"), n(5.0)]));
}

#[test]
fn differing_strings_do_not_collapse() {
    assert_ne!(enc(&[s("apple")]), enc(&[s("banana")]));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn differing_numbers_do_not_collapse() {
    assert_ne!(enc(&[n(5.0)]), enc(&[n(6.0)]));
}

#[test]
fn field_order_matters() {
    assert_ne!(enc(&[s("a"), s("b")]), enc(&[s("b"), s("a")]));
}

#[test]
fn length_prefix_prevents_boundary_ambiguity() {
    // "ab" | "c"  must not collide with  "a" | "bc".
    assert_ne!(enc(&[s("ab"), s("c")]), enc(&[s("a"), s("bc")]));
}

#[test]
fn absent_null_and_empty_string_are_distinct() {
    let absent = enc(&[None]);
    let null = enc(&[Some(SharedValue::null_static())]);
    let empty = enc(&[s("")]);
    assert_ne!(absent, null);
    assert_ne!(absent, empty);
    assert_ne!(null, empty);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn number_and_numeric_string_are_distinct() {
    // Accepted divergence from RSValue_Cmp: no number↔string coercion.
    assert_ne!(enc(&[n(5.0)]), enc(&[s("5")]));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn integral_floats_normalize() {
    // 5 and 5.0 are the same f64 and num_to_str renders both as "5".
    assert_eq!(enc(&[n(5.0)]), enc(&[n(5.0)]));
    // Distinct numbers stay distinct.
    assert_ne!(enc(&[n(5.0)]), enc(&[n(5.5)]));
}

#[test]
fn maps_dedup_by_content_not_all_collapse() {
    // The `compare` map-stub treats all maps as equal; our encoding must not.
    let m1 = Some(SharedValue::new_map(vec![(
        SharedValue::new_string(b"k".to_vec()),
        SharedValue::new_string(b"v1".to_vec()),
    )]));
    let m2 = Some(SharedValue::new_map(vec![(
        SharedValue::new_string(b"k".to_vec()),
        SharedValue::new_string(b"v2".to_vec()),
    )]));
    let m1b = Some(SharedValue::new_map(vec![(
        SharedValue::new_string(b"k".to_vec()),
        SharedValue::new_string(b"v1".to_vec()),
    )]));
    assert_ne!(enc(std::slice::from_ref(&m1)), enc(&[m2]));
    assert_eq!(enc(&[m1]), enc(&[m1b]));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes numbers via `num_to_str`, which calls C `snprintf`"
)]
fn arrays_encode_elementwise() {
    let a1 = Some(SharedValue::new_array(vec![
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
    ]));
    let a2 = Some(SharedValue::new_array(vec![
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
    ]));
    let a3 = Some(SharedValue::new_array(vec![
        SharedValue::new_num(2.0),
        SharedValue::new_num(1.0),
    ]));
    assert_eq!(enc(std::slice::from_ref(&a1)), enc(&[a2]));
    assert_ne!(enc(&[a1]), enc(&[a3]));
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_row_eq_and_hash_agree_over_real_rows() {
    fn distinct_row(key: &RLookupKey<'_>, v: f64) -> DistinctRow {
        let mut row = RLookupRow::new();
        row.write_key(key, SharedValue::new_num(v));
        let canon = encode_values(row.dyn_values());
        DistinctRow::from_parts(row, canon)
    }

    fn hash_of(dr: &DistinctRow) -> u64 {
        let mut hasher = DefaultHasher::new();
        dr.hash(&mut hasher);
        hasher.finish()
    }

    let key = RLookupKey::new(c"v", RLookupKeyFlags::empty());
    let a = distinct_row(&key, 7.0);
    let b = distinct_row(&key, 7.0);
    let c = distinct_row(&key, 8.0);

    // Equal projected value ⇒ equal and same hash (Hash/Eq contract).
    // `DistinctRow` intentionally has no `Debug`, so compare with `==`/`!=`
    // rather than `assert_eq!`/`assert_ne!`.
    assert!(a == b);
    assert_eq!(hash_of(&a), hash_of(&b));
    // Different value ⇒ not equal.
    assert!(a != c);
}

// ---------------------------------------------------------------------------
// `Storage::DistinctHeap` integration: dedup-keep-best, bounded eviction,
// deferred projection, and best→worst drain. These model the remote explicit
// `FIELDS` config (projected field `g`, separate SORTBY key `s`): the dedup
// identity is `g` only, while `s` drives ranking.
// ---------------------------------------------------------------------------

fn make_key(name: &'static CStr, dstidx: u16) -> RLookupKey<'static> {
    let mut key = RLookupKey::new(name, RLookupKeyFlags::empty());
    key.dstidx = dstidx;
    key
}

/// Build a row with a projected group field `g` and a sort field `s`.
fn group_row(
    g: &RLookupKey<'_>,
    group: &str,
    s: &RLookupKey<'_>,
    sort: f64,
) -> RLookupRow<'static> {
    let mut row = RLookupRow::new();
    row.write_key(g, SharedValue::new_string(group.as_bytes().to_vec()));
    row.write_key(s, SharedValue::new_num(sort));
    row
}

/// Read back the projected group field of a drained row.
fn group_of(g: &RLookupKey<'_>, row: &RLookupRow<'static>) -> String {
    let bytes = row.get(g).unwrap().as_str_bytes().unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Drive one DISTINCT insert: SORTBY key `s` is the priority; the dedup
/// identity is the projected field `g` only (sort key excluded).
fn distinct_insert(
    storage: &mut Storage<()>,
    g: &RLookupKey<'static>,
    s: &RLookupKey<'static>,
    group: &str,
    sort: f64,
) {
    let group = group.to_owned();
    storage.insert_distinct_entry(
        || vec![Some(SharedValue::new_num(sort))].into_boxed_slice(),
        (),
        || group_row(g, &group, s, sort),
        |row| encode_value_refs([row.get(g).map(|v| &**v)]),
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_keeps_best_representative_per_sort_key() {
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    let mut storage = Storage::new_distinct(Some((0, 2)), SORT_DESC);

    // Group A appears twice; its better sort key (9) must be the one retained.
    // With (group, sort): A:1, A:9, B:5, C:3 and K=2, the top-2 by sort DESC
    // are A(9) and B(5). Had the dedup kept A:1, A would lose to C:3 and the
    // result would be [B, C] — so [A, B] proves best-key retention.
    for (group, sort) in [("A", 1.0), ("A", 9.0), ("B", 5.0), ("C", 3.0)] {
        distinct_insert(&mut storage, &g, &s, group, sort);
    }

    let drained: Vec<_> = storage.drain().collect();
    let groups: Vec<String> = drained.iter().map(|r| group_of(&g, r)).collect();
    assert_eq!(groups, vec!["A".to_string(), "B".to_string()]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_keeps_best_when_better_arrives_first() {
    // Mirror of `distinct_keeps_best_representative_per_sort_key` but with the
    // BETTER representative inserted FIRST, then the worse duplicate. This is
    // the arrival order the coordinator sees, and exercises whether
    // `push_increase` correctly *keeps* the incumbent when the new key is worse.
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    let mut storage = Storage::new_distinct(Some((0, 2)), SORT_DESC);

    for (group, sort) in [("a", 5.0), ("a", 1.0), ("b", 3.0)] {
        distinct_insert(&mut storage, &g, &s, group, sort);
    }

    let drained: Vec<_> = storage.drain().collect();
    let groups: Vec<String> = drained.iter().map(|r| group_of(&g, r)).collect();
    // a kept its best (5) ⇒ ranks above b(3): [a, b].
    assert_eq!(groups, vec!["a".to_string(), "b".to_string()]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_collapses_duplicate_projected_fields() {
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    let mut storage = Storage::new_distinct(Some((0, 10)), SORT_DESC);

    // Same projected field, three different sort keys → one survivor.
    for sort in [1.0, 2.0, 3.0] {
        distinct_insert(&mut storage, &g, &s, "X", sort);
    }

    let drained: Vec<_> = storage.drain().collect();
    assert_eq!(drained.len(), 1);
    assert_eq!(group_of(&g, &drained[0]), "X");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_bounded_eviction_keeps_top_k_groups() {
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    let mut storage = Storage::new_distinct(Some((0, 2)), SORT_DESC);

    // Four distinct groups; only the top-2 by sort DESC survive.
    for (group, sort) in [("A", 4.0), ("B", 3.0), ("C", 2.0), ("D", 1.0)] {
        distinct_insert(&mut storage, &g, &s, group, sort);
    }

    let drained: Vec<_> = storage.drain().collect();
    let groups: Vec<String> = drained.iter().map(|r| group_of(&g, r)).collect();
    assert_eq!(groups, vec!["A".to_string(), "B".to_string()]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "seed insert encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_doomed_candidate_skips_projection() {
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    let mut storage = Storage::new_distinct(Some((0, 1)), SORT_DESC); // K = 1

    // Seed the single slot with a strong sort key.
    distinct_insert(&mut storage, &g, &s, "A", 5.0);

    // A candidate worse than the current worst (1 < 5) is doomed: neither
    // `project` nor `dedup_from_row` may run (deferred projection).
    let projected = Cell::new(false);
    let dedup_called = Cell::new(false);
    storage.insert_distinct_entry(
        || vec![Some(SharedValue::new_num(1.0))].into_boxed_slice(),
        (),
        || {
            projected.set(true);
            group_row(&g, "B", &s, 1.0)
        },
        |row| {
            dedup_called.set(true);
            encode_value_refs([row.get(&g).map(|v| &**v)])
        },
    );

    assert!(!projected.get(), "doomed candidate must not be projected");
    assert!(!dedup_called.get(), "doomed candidate must not be hashed");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn distinct_drain_applies_offset_and_count() {
    let g = make_key(c"g", 0);
    let s = make_key(c"s", 1);
    // offset 1, count 2 → cap = 3 survivors retained, window skips the best.
    let mut storage = Storage::new_distinct(Some((1, 2)), SORT_DESC);

    for (group, sort) in [("A", 4.0), ("B", 3.0), ("C", 2.0), ("D", 1.0)] {
        distinct_insert(&mut storage, &g, &s, group, sort);
    }

    // Survivors best→worst are A(4), B(3), C(2); skip(1).take(2) → [B, C].
    let drained: Vec<_> = storage.drain().collect();
    let groups: Vec<String> = drained.iter().map(|r| group_of(&g, r)).collect();
    assert_eq!(groups, vec!["B".to_string(), "C".to_string()]);
}

// ---------------------------------------------------------------------------
// Reducer-level wiring: DISTINCT through the real `RemoteCollectReducer`
// `add` → `finalize` path, exercising the `distinct` flag, storage selection, the
// projected-only dedup identity, and the `@__key` skip.
// ---------------------------------------------------------------------------

/// Extract a string field from each row of a `finalize` output (`[Map, …]`).
fn output_field(out: &SharedValue, field: &[u8]) -> Vec<String> {
    let Value::Array(rows) = &**out else {
        panic!("expected array output");
    };
    rows.iter()
        .map(|row| {
            let Value::Map(map) = &**row else {
                panic!("expected map row");
            };
            let v = map.get(field).expect("field present");
            String::from_utf8(v.as_str_bytes().expect("string field").to_vec()).unwrap()
        })
        .collect()
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn reducer_distinct_dedups_on_field_excluding_sort_key() {
    // REDUCE COLLECT FIELDS 1 @name SORTBY 1 @sweetness DESC DISTINCT
    let name = make_key(c"name", 0);
    let sweetness = make_key(c"sweetness", 1);
    let reducer = RemoteCollectReducer::new(
        Box::new([&name]),
        None,
        Box::new([&sweetness]),
        SORT_DESC,
        None,
        /* is_internal */ false,
        true,
    );

    let mut ctx = RemoteCollectCtx::new(&reducer);
    // "apple" appears twice with different sweetness; dedup on @name only keeps
    // the best (9). SORTBY sweetness DESC ⇒ apple(9) ranks above banana(5).
    for (doc_id, (n, s)) in [("apple", 1.0), ("apple", 9.0), ("banana", 5.0)]
        .into_iter()
        .enumerate()
    {
        let mut row = RLookupRow::new();
        row.write_key(&name, SharedValue::new_string(n.as_bytes().to_vec()));
        row.write_key(&sweetness, SharedValue::new_num(s));
        ctx.add(&reducer, &row, doc_id as ffi::t_docId);
    }

    let out = ctx.finalize(&reducer);
    assert_eq!(
        output_field(&out, b"name"),
        vec!["apple".to_string(), "banana".to_string()]
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "encodes a number via `num_to_str`, which calls C `snprintf`"
)]
fn reducer_distinct_internal_emits_winning_sort_value() {
    // Regression: on the shard→coordinator path (`is_internal`) the emitted
    // payload carries the SORTBY columns. When a better duplicate replaces an
    // earlier one, the emitted sort value must be the WINNER's, not the
    // first-seen loser's — otherwise the coordinator re-sorts/dedups on a stale
    // value. (Plain `push_increase` updates only the priority and would leave
    // the loser's row, so this guards the remove+push replacement.)
    let cat = make_key(c"cat", 0);
    let score = make_key(c"score", 1);
    let reducer = RemoteCollectReducer::new(
        Box::new([&cat]),
        None,
        Box::new([&score]),
        SORT_DESC,
        None,
        /* is_internal */ true,
        true,
    );
    let mut ctx = RemoteCollectCtx::new(&reducer);

    // Loser (1) inserted first, then the winner (5).
    for (doc_id, (c, s)) in [("a", 1.0), ("a", 5.0)].into_iter().enumerate() {
        let mut row = RLookupRow::new();
        row.write_key(&cat, SharedValue::new_string(c.as_bytes().to_vec()));
        row.write_key(&score, SharedValue::new_num(s));
        ctx.add(&reducer, &row, doc_id as ffi::t_docId);
    }

    let out = ctx.finalize(&reducer);
    let Value::Array(rows) = &*out else {
        panic!("expected array");
    };
    assert_eq!(rows.len(), 1, "the two `a` rows must dedup to one");
    let Value::Map(m) = &*rows[0] else {
        panic!("expected map");
    };
    assert_eq!(
        m.get(b"cat").and_then(|v| v.as_str_bytes()),
        Some(b"a".as_slice())
    );
    // The surviving row must carry the winning score 5, not the loser's 1.
    assert_eq!(m.get(b"score").and_then(|v| v.as_num()), Some(5.0));
}

#[test]
fn reducer_distinct_skipped_when_key_field_projected() {
    // FIELDS include @__key ⇒ DISTINCT is a provable no-op (design doc §6), so
    // the reducer keeps the plain Heap and does NOT collapse rows that share
    // all projected fields.
    let g = make_key(c"g", 0);
    let key_field = make_key(c"__key", 1);
    let sweetness = make_key(c"sweetness", 2);
    let reducer = RemoteCollectReducer::new(
        Box::new([&g, &key_field]),
        None,
        Box::new([&sweetness]),
        SORT_DESC,
        None,
        /* is_internal */ false,
        true,
    );

    let mut ctx = RemoteCollectCtx::new(&reducer);
    // Two rows identical across all projected fields. DistinctHeap would
    // collapse them to one; with the @__key skip the Heap keeps both.
    for doc_id in 0..2 {
        let mut row = RLookupRow::new();
        row.write_key(&g, SharedValue::new_string(b"x".to_vec()));
        row.write_key(&key_field, SharedValue::new_string(b"doc:1".to_vec()));
        row.write_key(&sweetness, SharedValue::new_num(5.0));
        ctx.add(&reducer, &row, doc_id as ffi::t_docId);
    }

    let out = ctx.finalize(&reducer);
    assert_eq!(
        output_field(&out, b"g"),
        vec!["x".to_string(), "x".to_string()]
    );
}
