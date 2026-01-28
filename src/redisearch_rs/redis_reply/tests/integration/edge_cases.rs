/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Edge case tests and panic tests for Fixed*Builder length validation.

use redis_mock::reply::capture_replies;

use crate::init;

// ============================================================================
// Panic Tests - FixedArrayBuilder
// ============================================================================

#[test]
#[should_panic(expected = "declared length 3 but added 2 elements")]
fn test_fixed_array_underflow_panics() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.fixed_array(3);
            fixed.long_long(1);
            fixed.long_long(2);
            // Missing third element - should panic on drop
        }
    });
}

#[test]
#[should_panic(expected = "declared length 2 but added 3 elements")]
fn test_fixed_array_overflow_panics() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.fixed_array(2);
            fixed.long_long(1);
            fixed.long_long(2);
            fixed.long_long(3); // Extra element - should panic on drop
        }
    });
}

#[test]
#[should_panic(expected = "declared length 1 but added 0 elements")]
fn test_fixed_array_empty_when_expecting_one() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let _fixed = arr.fixed_array(1);
            // No elements added when expecting 1 - should panic on drop
        }
    });
}

// ============================================================================
// Panic Tests - FixedMapBuilder
// ============================================================================

#[test]
#[should_panic(expected = "declared length 3 but added 2 key-value pairs")]
fn test_fixed_map_underflow_panics() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.fixed_map(3);
            fixed.kv_long_long(c"a", 1);
            fixed.kv_long_long(c"b", 2);
            // Missing third KV pair - should panic on drop
        }
    });
}

#[test]
#[should_panic(expected = "declared length 2 but added 3 key-value pairs")]
fn test_fixed_map_overflow_panics() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.fixed_map(2);
            fixed.kv_long_long(c"a", 1);
            fixed.kv_long_long(c"b", 2);
            fixed.kv_long_long(c"c", 3); // Extra KV pair - should panic on drop
        }
    });
}

#[test]
#[should_panic(expected = "declared length 1 but added 0 key-value pairs")]
fn test_fixed_map_empty_when_expecting_one() {
    let mut replier = init();
    let _ = capture_replies(|| {
        let mut arr = replier.array();
        {
            let _fixed = arr.fixed_map(1);
            // No KV pairs added when expecting 1 - should panic on drop
        }
    });
}

// ============================================================================
// Edge Cases - Count Verification
// ============================================================================

#[test]
fn test_fixed_map_exact_with_nested() {
    // Verify nested structures count as exactly 1 KV pair
    let mut replier = init();
    let reply = crate::capture_single_reply(|| {
        let mut arr = replier.array();
        {
            let mut fixed = arr.fixed_map(2);
            {
                // This nested array is 1 KV pair
                let mut inner = fixed.kv_array(c"items");
                inner.long_long(1);
                inner.long_long(2);
                inner.long_long(3);
            }
            // This is 1 KV pair
            fixed.kv_long_long(c"count", 3);
        }
    });
    insta::assert_debug_snapshot!(reply, @r#"[{"items": [1, 2, 3], "count": 3}]"#);
}

// ============================================================================
// Property-Based Tests
// ============================================================================

mod property_based {
    use super::*;

    proptest::proptest! {
        #[test]
        fn array_length_matches_element_count(count in 0usize..20) {
            let mut replier = init();
            let reply = crate::capture_single_reply(|| {
                let mut arr = replier.array();
                for i in 0..count {
                    arr.long_long(i as i64);
                }
            });
            // Verify the array has exactly `count` elements
            match reply {
                redis_mock::reply::ReplyValue::Array(elements) => {
                    proptest::prop_assert_eq!(elements.len(), count);
                }
                _ => proptest::prop_assert!(false, "Expected array reply"),
            }
        }

        #[test]
        fn map_length_matches_pair_count(count in 0usize..20) {
            let mut replier = init();
            let reply = crate::capture_single_reply(|| {
                let mut map = replier.map();
                for i in 0..count {
                    // Create unique keys for each iteration
                    let key_bytes = format!("key{i}\0");
                    let key = std::ffi::CStr::from_bytes_with_nul(key_bytes.as_bytes()).unwrap();
                    map.kv_long_long(key, i as i64);
                }
            });
            // Verify the map has exactly `count` KV pairs
            match reply {
                redis_mock::reply::ReplyValue::Map(pairs) => {
                    proptest::prop_assert_eq!(pairs.len(), count);
                }
                _ => proptest::prop_assert!(false, "Expected map reply"),
            }
        }

        #[test]
        fn fixed_array_accepts_exact_count(count in 1u32..20) {
            // Note: count=0 is tested separately in test_fixed_array_zero_length
            let mut replier = init();
            // This should not panic - we add exactly `count` elements
            let reply = crate::capture_single_reply(|| {
                let mut arr = replier.array();
                {
                    let mut fixed = arr.fixed_array(count);
                    for i in 0..count {
                        fixed.long_long(i as i64);
                    }
                }
            });
            // Verify structure
            match reply {
                redis_mock::reply::ReplyValue::Array(outer) => {
                    proptest::prop_assert_eq!(outer.len(), 1);
                    match &outer[0] {
                        redis_mock::reply::ReplyValue::Array(inner) => {
                            proptest::prop_assert_eq!(inner.len(), count as usize);
                        }
                        _ => proptest::prop_assert!(false, "Expected inner array"),
                    }
                }
                _ => proptest::prop_assert!(false, "Expected array reply"),
            }
        }

        #[test]
        fn fixed_map_accepts_exact_count(count in 1u32..20) {
            // Note: count=0 is tested separately in test_fixed_map_zero_length
            let mut replier = init();
            // This should not panic - we add exactly `count` KV pairs
            let reply = crate::capture_single_reply(|| {
                let mut arr = replier.array();
                {
                    let mut fixed = arr.fixed_map(count);
                    for i in 0..count {
                        let key_bytes = format!("k{i}\0");
                        let key = std::ffi::CStr::from_bytes_with_nul(key_bytes.as_bytes()).unwrap();
                        fixed.kv_long_long(key, i as i64);
                    }
                }
            });
            // Verify structure
            match reply {
                redis_mock::reply::ReplyValue::Array(outer) => {
                    proptest::prop_assert_eq!(outer.len(), 1);
                    match &outer[0] {
                        redis_mock::reply::ReplyValue::Map(pairs) => {
                            proptest::prop_assert_eq!(pairs.len(), count as usize);
                        }
                        _ => proptest::prop_assert!(false, "Expected inner map"),
                    }
                }
                _ => proptest::prop_assert!(false, "Expected array reply"),
            }
        }
    }
}
