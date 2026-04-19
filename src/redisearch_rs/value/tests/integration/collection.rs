/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{Map, SharedRsValue};

fn make_string_key_value(key: &str, num: f64) -> (SharedRsValue, SharedRsValue) {
    (
        SharedRsValue::new_string(key.as_bytes().to_vec()),
        SharedRsValue::new_num(num),
    )
}

#[test]
fn map_get_found_key() {
    let map = Map::new(Box::new([
        make_string_key_value("price", 9.99),
        make_string_key_value("quantity", 3.0),
    ]));

    let result = map.get(b"price");
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_num(), Some(9.99));
}

#[test]
fn map_get_missing_key() {
    let map = Map::new(Box::new([
        make_string_key_value("price", 9.99),
    ]));

    assert!(map.get(b"missing").is_none());
}

#[test]
fn map_get_empty_map() {
    let map = Map::new(Box::new([]));

    assert!(map.get(b"anything").is_none());
}

#[test]
fn map_get_non_string_keys_skipped() {
    let map = Map::new(Box::new([
        (SharedRsValue::new_num(42.0), SharedRsValue::new_num(1.0)),
        (SharedRsValue::new_num(99.0), SharedRsValue::new_num(2.0)),
    ]));

    assert!(map.get(b"42").is_none());
}

#[test]
fn map_get_first_match_wins() {
    let map = Map::new(Box::new([
        make_string_key_value("key", 1.0),
        make_string_key_value("key", 2.0),
    ]));

    let result = map.get(b"key");
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_num(), Some(1.0));
}
