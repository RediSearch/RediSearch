/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for FixedArrayBuilder and FixedMapBuilder functionality.

use crate::{capture_single_reply, init};
use redis_mock::reply::capture_replies;

// ============================================================================
// FixedArrayBuilder Tests
// ============================================================================

#[test]
fn test_fixed_array_zero_length() {
    let mut replier = init();
    let replies = capture_replies(|| {
        // Create zero-length fixed array, then emit another value after it
        replier.fixed_array(0);
        replier.long_long(42);
    });
    // The zero-length array should be empty, followed by 42 as a separate reply.
    insta::assert_debug_snapshot!(replies, @r"
[
    [],
    42,
]
");
}

#[test]
fn test_fixed_array_builder() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_array(3);
            fixed.long_long(10);
            fixed.long_long(20);
            fixed.long_long(30);
        }
    });
    insta::assert_debug_snapshot!(reply, @"[[10, 20, 30]]");
}

#[test]
fn test_fixed_array_single_element() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_array(1);
            fixed.long_long(42);
        }
    });
    insta::assert_debug_snapshot!(reply, @"[[42]]");
}

#[test]
fn test_fixed_array_all_element_types() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_array(5);
            fixed.long_long(42);
            fixed.double(1.5);
            fixed.simple_string(c"test");
            fixed.empty_array();
            fixed.empty_map();
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[[42, 1.5, "test", [], {}]]"#);
}

#[test]
fn test_fixed_array_kv_long_long() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            // kv_long_long adds 2 elements
            let mut fixed = arr.nested_fixed_array(4);
            fixed.kv_long_long(c"a", 1);
            fixed.kv_long_long(c"b", 2);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[["a", 1, "b", 2]]"#);
}

#[test]
fn test_fixed_array_kv_double() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            // kv_double adds 2 elements
            let mut fixed = arr.nested_fixed_array(2);
            fixed.kv_double(c"ratio", 0.5);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[["ratio", 0.5]]"#);
}

#[test]
fn test_fixed_array_nested_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_array(3);
            fixed.long_long(1);
            {
                let mut inner = fixed.nested_array();
                inner.long_long(2);
                inner.long_long(3);
            }
            fixed.long_long(4);
        }
    });
    insta::assert_debug_snapshot!(reply, @"[[1, [2, 3], 4]]");
}

#[test]
fn test_fixed_array_nested_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_array(2);
            {
                let mut inner = fixed.nested_map();
                inner.kv_long_long(c"key", 42);
            }
            fixed.long_long(1);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[[{"key": 42}, 1]]"#);
}

#[test]
fn test_fixed_array_nested_fixed_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut outer_fixed = arr.nested_fixed_array(2);
            {
                let mut inner_fixed = outer_fixed.nested_fixed_array(2);
                inner_fixed.long_long(1);
                inner_fixed.long_long(2);
            }
            outer_fixed.long_long(3);
        }
    });
    insta::assert_debug_snapshot!(reply, @"[[[1, 2], 3]]");
}

#[test]
fn test_fixed_array_nested_fixed_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut outer_fixed = arr.nested_fixed_array(2);
            {
                let mut inner_fixed = outer_fixed.nested_fixed_map(1);
                inner_fixed.kv_long_long(c"nested", 42);
            }
            outer_fixed.long_long(1);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[[{"nested": 42}, 1]]"#);
}

// ============================================================================
// FixedMapBuilder Tests
// ============================================================================

#[test]
fn test_fixed_map_builder() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(2);
            fixed.kv_long_long(c"x", 1);
            fixed.kv_long_long(c"y", 2);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"x": 1, "y": 2}]"#);
}

#[test]
fn test_fixed_map_single_pair() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(1);
            fixed.kv_long_long(c"key", 42);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"key": 42}]"#);
}

#[test]
fn test_fixed_map_all_value_types() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(2);
            fixed.kv_long_long(c"integer", 42);
            fixed.kv_double(c"float", 1.5);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"integer": 42, "float": 1.5}]"#);
}

#[test]
fn test_fixed_map_kv_empty_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(2);
            fixed.kv_empty_array(c"empty_arr");
            fixed.kv_long_long(c"count", 0);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"empty_arr": [], "count": 0}]"#);
}

#[test]
fn test_fixed_map_kv_empty_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(2);
            fixed.kv_empty_map(c"empty_map");
            fixed.kv_long_long(c"count", 0);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"empty_map": {}, "count": 0}]"#);
}

#[test]
fn test_fixed_map_kv_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(2);
            {
                let mut inner_arr = fixed.kv_array(c"items");
                inner_arr.long_long(1);
                inner_arr.long_long(2);
            }
            fixed.kv_long_long(c"total", 2);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"items": [1, 2], "total": 2}]"#);
}

#[test]
fn test_fixed_map_kv_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(1);
            {
                let mut inner_map = fixed.kv_map(c"nested");
                inner_map.kv_long_long(c"a", 1);
                inner_map.kv_long_long(c"b", 2);
            }
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"nested": {"a": 1, "b": 2}}]"#);
}

#[test]
fn test_fixed_map_kv_fixed_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(1);
            {
                let mut inner_fixed = fixed.kv_fixed_array(c"fixed_arr", 2);
                inner_fixed.long_long(10);
                inner_fixed.long_long(20);
            }
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"fixed_arr": [10, 20]}]"#);
}

#[test]
fn test_fixed_map_kv_fixed_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(1);
            {
                let mut inner_fixed = fixed.kv_fixed_map(c"fixed_map", 1);
                inner_fixed.kv_long_long(c"deep", 42);
            }
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"fixed_map": {"deep": 42}}]"#);
}

#[test]
fn test_fixed_map_deeply_nested() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.nested_fixed_map(1);
            {
                let mut arr1 = fixed.kv_array(c"data");
                {
                    let mut map1 = arr1.nested_map();
                    {
                        let mut fixed2 = map1.kv_fixed_map(c"inner", 1);
                        fixed2.kv_long_long(c"value", 100);
                    }
                }
            }
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"data": [{"inner": {"value": 100}}]}]"#);
}
