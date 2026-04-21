/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{Array, Map, SharedRsValue};

fn make_map(pairs: &[(&str, f64)]) -> Map {
    let entries: Vec<_> = pairs
        .iter()
        .map(|(k, v)| {
            (
                SharedRsValue::new_string(k.as_bytes().to_vec()),
                SharedRsValue::new_num(*v),
            )
        })
        .collect();
    Map::new(entries.into_boxed_slice())
}

fn make_flat_map_array(pairs: &[(&str, f64)]) -> Array {
    let elements: Vec<SharedRsValue> = pairs
        .iter()
        .flat_map(|(k, v)| {
            [
                SharedRsValue::new_string(k.as_bytes().to_vec()),
                SharedRsValue::new_num(*v),
            ]
        })
        .collect();
    Array::new(elements.into_boxed_slice())
}

fn assert_get(map: &Map, arr: &Array, key: &[u8], expected: Option<f64>) {
    assert_eq!(map.get(key).map(|v| v.as_num().unwrap()), expected);
    assert_eq!(arr.map_get(key).map(|v| v.as_num().unwrap()), expected);
}

// -- Shared scenarios: Map::get and Array::map_get behave identically --

#[test]
fn found_key() {
    let pairs = &[("price", 9.99), ("quantity", 3.0)];
    let map = make_map(pairs);
    let arr = make_flat_map_array(pairs);

    assert_get(&map, &arr, b"price", Some(9.99));
    assert_get(&map, &arr, b"quantity", Some(3.0));
}

#[test]
fn missing_key() {
    let pairs = &[("price", 9.99)];
    let map = make_map(pairs);
    let arr = make_flat_map_array(pairs);

    assert_get(&map, &arr, b"missing", None);
}

#[test]
fn empty() {
    let map = Map::new(Box::new([]));
    let arr = Array::new(Box::new([]));

    assert_get(&map, &arr, b"anything", None);
}

#[test]
fn non_string_keys_skipped() {
    let map = Map::new(Box::new([
        (SharedRsValue::new_num(42.0), SharedRsValue::new_num(1.0)),
        (SharedRsValue::new_num(99.0), SharedRsValue::new_num(2.0)),
    ]));
    let arr = Array::new(Box::new([
        SharedRsValue::new_num(42.0),
        SharedRsValue::new_num(1.0),
        SharedRsValue::new_num(99.0),
        SharedRsValue::new_num(2.0),
    ]));

    assert_get(&map, &arr, b"42", None);
}

#[test]
fn first_match_wins() {
    let pairs = &[("key", 1.0), ("key", 2.0)];
    let map = make_map(pairs);
    let arr = make_flat_map_array(pairs);

    assert_get(&map, &arr, b"key", Some(1.0));
}

// -- Array-only: odd-length array panics in debug builds --

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "map_get called on an odd-length array")]
fn array_map_get_odd_length_panics_in_debug() {
    let arr = Array::new(Box::new([
        SharedRsValue::new_string(b"price".to_vec()),
        SharedRsValue::new_num(9.99),
        SharedRsValue::new_string(b"orphan".to_vec()),
    ]));

    let _ = arr.map_get(b"price");
}
