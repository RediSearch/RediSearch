/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for MapBuilder functionality.

use crate::{capture_single_reply, init};

#[test]
fn test_map_builder() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_long_long(c"count", 42);
        map.kv_double(c"average", 1.5);
    });
    insta::assert_debug_snapshot!(reply, @r#"{"count": 42, "average": 1.5}"#);
}

#[test]
fn test_map_builder_empty() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let _map = replier.map();
        // No KV pairs added - should produce empty map
    });
    insta::assert_debug_snapshot!(reply, @"{}");
}

#[test]
fn test_map_with_nested_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_long_long(c"total", 100);
        {
            let mut arr = map.kv_array(c"items");
            arr.long_long(1);
            arr.long_long(2);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"{"total": 100, "items": [1, 2]}"#);
}

#[test]
fn test_map_with_nested_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut outer = replier.map();
        {
            let mut inner = outer.kv_map(c"nested");
            inner.kv_long_long(c"a", 1);
            inner.kv_long_long(c"b", 2);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"{"nested": {"a": 1, "b": 2}}"#);
}

#[test]
fn test_kv_empty_array_in_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_empty_array(c"empty");
        map.kv_long_long(c"count", 0);
    });
    insta::assert_debug_snapshot!(reply, @r#"{"empty": [], "count": 0}"#);
}

#[test]
fn test_kv_empty_map_in_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_empty_map(c"empty");
    });
    insta::assert_debug_snapshot!(reply, @r#"{"empty": {}}"#);
}

#[test]
fn test_map_builder_kv_fixed_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_long_long(c"before", 0);
        {
            let mut fixed_arr = map.kv_fixed_array(c"fixed", 3);
            fixed_arr.long_long(1);
            fixed_arr.long_long(2);
            fixed_arr.long_long(3);
        }
        map.kv_long_long(c"after", 4);
    });
    insta::assert_debug_snapshot!(reply, @r#"{"before": 0, "fixed": [1, 2, 3], "after": 4}"#);
}

#[test]
fn test_map_builder_kv_fixed_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        map.kv_long_long(c"before", 0);
        {
            let mut fixed_map = map.kv_fixed_map(c"fixed", 2);
            fixed_map.kv_long_long(c"x", 10);
            fixed_map.kv_long_long(c"y", 20);
        }
        map.kv_long_long(c"after", 1);
    });
    insta::assert_debug_snapshot!(reply, @r#"{"before": 0, "fixed": {"x": 10, "y": 20}, "after": 1}"#);
}

#[test]
fn test_deeply_nested_structures() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut outer_arr = replier.array();
        {
            let mut map = outer_arr.nested_map();
            {
                let mut inner_arr = map.kv_array(c"data");
                {
                    let mut inner_map = inner_arr.nested_map();
                    inner_map.kv_long_long(c"value", 42);
                }
            }
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"data": [{"value": 42}]}]"#);
}

#[test]
fn test_map_builder_multiple_nested_arrays() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut map = replier.map();
        {
            let mut arr1 = map.kv_array(c"first");
            arr1.long_long(1);
            arr1.long_long(2);
        }
        {
            let mut arr2 = map.kv_array(c"second");
            arr2.long_long(3);
            arr2.long_long(4);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"{"first": [1, 2], "second": [3, 4]}"#);
}
