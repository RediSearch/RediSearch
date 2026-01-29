/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for ArrayBuilder functionality.

use crate::{capture_single_reply, init};

#[test]
fn test_array_builder() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(1);
        arr.long_long(2);
        arr.long_long(3);
    });
    insta::assert_debug_snapshot!(reply, @"[1, 2, 3]");
}

#[test]
fn test_array_builder_empty() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let _arr = replier.array();
        // No elements added - should produce empty array
    });
    insta::assert_debug_snapshot!(reply, @"[]");
}

#[test]
fn test_array_builder_mixed_types() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(42);
        arr.double(1.5);
        arr.simple_string(c"test");
    });
    insta::assert_debug_snapshot!(reply, @r#"[42, 1.5, "test"]"#);
}

#[test]
fn test_array_builder_empty_array_element() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(1);
        arr.empty_array();
        arr.long_long(2);
    });
    insta::assert_debug_snapshot!(reply, @"[1, [], 2]");
}

#[test]
fn test_array_builder_empty_map_element() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(1);
        arr.empty_map();
        arr.long_long(2);
    });
    insta::assert_debug_snapshot!(reply, @"[1, {}, 2]");
}

#[test]
fn test_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut outer = replier.array();
        outer.long_long(1);
        {
            let mut inner = outer.array();
            inner.long_long(2);
            inner.long_long(3);
        }
        outer.long_long(4);
    });
    insta::assert_debug_snapshot!(reply, @"[1, [2, 3], 4]");
}

#[test]
fn test_array_builder_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(1);
        {
            let mut map = arr.map();
            map.kv_long_long(c"key", 42);
        }
        arr.long_long(2);
    });
    insta::assert_debug_snapshot!(reply, @r#"[1, {"key": 42}, 2]"#);
}

#[test]
fn test_array_builder_fixed_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(0);
        {
            let mut fixed = arr.fixed_array(2);
            fixed.long_long(1);
            fixed.long_long(2);
        }
        arr.long_long(3);
    });
    insta::assert_debug_snapshot!(reply, @"[0, [1, 2], 3]");
}

#[test]
fn test_array_builder_fixed_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        arr.long_long(0);
        {
            let mut fixed = arr.fixed_map(2);
            fixed.kv_long_long(c"a", 1);
            fixed.kv_long_long(c"b", 2);
        }
        arr.long_long(3);
    });
    insta::assert_debug_snapshot!(reply, @r#"[0, {"a": 1, "b": 2}, 3]"#);
}

#[test]
fn test_array_builder_multiple_nested() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut inner1 = arr.array();
            inner1.long_long(1);
        }
        {
            let mut inner2 = arr.array();
            inner2.long_long(2);
        }
        {
            let mut inner3 = arr.array();
            inner3.long_long(3);
        }
    });
    insta::assert_debug_snapshot!(reply, @"[[1], [2], [3]]");
}
