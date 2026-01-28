/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for basic Replier functionality.

use redis_mock::reply::capture_replies;

use crate::{capture_single_reply, init};

#[test]
fn test_replier_long_long() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.long_long(42);
    });
    insta::assert_debug_snapshot!(reply, @"42");
}

#[test]
fn test_replier_long_long_min() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.long_long(i64::MIN);
    });
    insta::assert_debug_snapshot!(reply, @"-9223372036854775808");
}

#[test]
fn test_replier_long_long_max() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.long_long(i64::MAX);
    });
    insta::assert_debug_snapshot!(reply, @"9223372036854775807");
}

#[test]
fn test_replier_double() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.double(1.5);
    });
    insta::assert_debug_snapshot!(reply, @"1.5");
}

#[test]
fn test_replier_double_nan() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.double(f64::NAN);
    });
    insta::assert_debug_snapshot!(reply, @"NaN");
}

#[test]
fn test_replier_double_infinity() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.double(f64::INFINITY);
    });
    insta::assert_debug_snapshot!(reply, @"inf");
}

#[test]
fn test_replier_double_neg_infinity() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.double(f64::NEG_INFINITY);
    });
    insta::assert_debug_snapshot!(reply, @"-inf");
}

#[test]
fn test_replier_double_neg_zero() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.double(-0.0);
    });
    // -0.0 and 0.0 are equal in IEEE 754, but may be represented differently
    insta::assert_debug_snapshot!(reply, @"-0");
}

#[test]
fn test_replier_simple_string() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.simple_string(c"hello world");
    });
    insta::assert_debug_snapshot!(reply, @r#""hello world""#);
}

#[test]
fn test_replier_simple_string_empty() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.simple_string(c"");
    });
    insta::assert_debug_snapshot!(reply, @r#""""#);
}

#[test]
fn test_replier_empty_array() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.empty_array();
    });
    insta::assert_debug_snapshot!(reply, @"[]");
}

#[test]
fn test_replier_empty_map() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.empty_map();
    });
    insta::assert_debug_snapshot!(reply, @"{}");
}

#[test]
fn test_replier_kv_long_long() {
    let mut replier = init();
    let replies = capture_replies(|| {
        replier.kv_long_long(c"count", 100);
    });
    insta::assert_debug_snapshot!(replies, @r#"
    [
        "count",
        100,
    ]
    "#);
}

#[test]
fn test_replier_kv_double() {
    let mut replier = init();
    let replies = capture_replies(|| {
        replier.kv_double(c"ratio", 0.75);
    });
    insta::assert_debug_snapshot!(replies, @r#"
    [
        "ratio",
        0.75,
    ]
    "#);
}

#[test]
fn test_replier_fixed_array_direct() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.fixed_array(3);
        replier.long_long(1);
        replier.long_long(2);
        replier.long_long(3);
    });
    insta::assert_debug_snapshot!(reply, @"[1, 2, 3]");
}

#[test]
fn test_replier_fixed_map_direct() {
    let mut replier = init();
    let reply = capture_single_reply(|| {
        replier.fixed_map(2);
        replier.kv_long_long(c"a", 1);
        replier.kv_long_long(c"b", 2);
    });
    insta::assert_debug_snapshot!(reply, @r#"{"a": 1, "b": 2}"#);
}
