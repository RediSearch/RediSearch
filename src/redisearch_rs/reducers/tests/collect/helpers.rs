/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use reducers::collect::{RemoteCollectCtx, RemoteCollectReducer};
use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupRow};
use value::{Map, SharedValue, Value};

/// Distinct keys in the same row need distinct `dstidx`es so they don't
/// alias the same `RLookupRow` slot.
pub(super) fn make_key(name: &'static CStr, dstidx: u16) -> RLookupKey<'static> {
    let mut key = RLookupKey::new(name, RLookupKeyFlags::empty());
    key.dstidx = dstidx;
    key
}

pub(super) fn string_value(s: &str) -> SharedValue {
    SharedValue::new_string(s.as_bytes().to_vec())
}

pub(super) fn map_entries(value: &SharedValue) -> &Map {
    let Value::Map(map) = &**value else {
        panic!("expected map, got {value:?}");
    };
    map
}

pub(super) fn array_entries(value: &SharedValue) -> &[SharedValue] {
    let Value::Array(array) = &**value else {
        panic!("expected array, got {value:?}");
    };
    array
}

pub(super) struct RemoteCollectFixture {
    pub(super) name_key: RLookupKey<'static>,
    pub(super) sweetness_key: RLookupKey<'static>,
}

impl RemoteCollectFixture {
    pub(super) fn new() -> Self {
        Self {
            name_key: make_key(c"name", 0),
            sweetness_key: make_key(c"sweetness", 1),
        }
    }

    /// Builds the remote half of:
    ///
    /// REDUCE COLLECT 6
    ///   FIELDS 1 @name
    ///   SORTBY 1 @sweetness
    pub(super) fn reducer(&self, is_internal: bool) -> RemoteCollectReducer<'_> {
        RemoteCollectReducer::new(
            Box::new([&self.name_key]),
            None,
            Box::new([&self.sweetness_key]),
            0,
            None,
            is_internal,
        )
    }

    pub(super) fn row(&self, name: &str, sweetness: f64) -> RLookupRow<'_> {
        let mut row = RLookupRow::new();
        row.write_key(&self.name_key, string_value(name));
        row.write_key(&self.sweetness_key, SharedValue::new_num(sweetness));
        row
    }
}

pub(super) fn make_row<'a>(
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
pub(super) fn run_collect(
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
        /* is_internal */ false,
    );
    let mut ctx = RemoteCollectCtx::new(&r);
    for (projected, sort_vals) in rows {
        let row = make_row(&field_keys, &sort_keys, &projected, &sort_vals);
        ctx.add(&r, &row);
    }
    ctx.finalize(&r)
}

pub(super) fn extract_num_field(out: &SharedValue, name: &[u8]) -> Vec<f64> {
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
pub(super) fn num_row(v: f64) -> (Vec<SharedValue>, Vec<SharedValue>) {
    (vec![SharedValue::new_num(v)], vec![SharedValue::new_num(v)])
}

/// Fixture for load-all (`FIELDS *`) tests. Owns a real [`RLookup`] so the
/// reducer's live walk has something to iterate. Pre-registers three visible
/// keys (`name`, `color`, `sweetness`) plus one [`RLookupKeyFlag::Hidden`]
/// key (`__hidden`) so the "skip hidden" assertion has a target.
pub(super) struct RemoteCollectLoadAllFixture {
    pub(super) lookup: RLookup<'static>,
}

impl RemoteCollectLoadAllFixture {
    pub(super) fn new() -> Self {
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
}
