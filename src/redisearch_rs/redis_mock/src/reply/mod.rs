/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations for Redis reply functions.
//!
//! This module provides mock versions of the Redis module reply API that capture
//! replies into an in-memory sink for testing purposes.

mod c_functions;
mod capture;
mod value;

pub use c_functions::*;
pub use capture::capture_replies;
pub use value::ReplyValue;

#[cfg(test)]
mod tests {
    use std::ffi::c_longlong;

    use super::*;

    #[test]
    fn test_capture_long_long() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                RedisModule_ReplyWithLongLong(std::ptr::null_mut(), 42);
            }
        });
        assert_eq!(replies, vec![ReplyValue::LongLong(42)]);
    }

    #[test]
    fn test_capture_double() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                RedisModule_ReplyWithDouble(std::ptr::null_mut(), 3.14);
            }
        });
        assert_eq!(replies, vec![ReplyValue::Double(3.14)]);
    }

    #[test]
    fn test_capture_simple_string() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                RedisModule_ReplyWithSimpleString(std::ptr::null_mut(), c"hello".as_ptr());
            }
        });
        assert_eq!(replies, vec![ReplyValue::SimpleString("hello".to_string())]);
    }

    #[test]
    fn test_capture_empty_array() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                RedisModule_ReplyWithEmptyArray(std::ptr::null_mut());
            }
        });
        assert_eq!(replies, vec![ReplyValue::Array(vec![])]);
    }

    #[test]
    fn test_capture_array_with_elements() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                let ctx = std::ptr::null_mut();
                RedisModule_ReplyWithArray(ctx, ffi::REDISMODULE_POSTPONED_ARRAY_LEN as c_longlong);
                RedisModule_ReplyWithLongLong(ctx, 1);
                RedisModule_ReplyWithLongLong(ctx, 2);
                RedisModule_ReplyWithLongLong(ctx, 3);
                RedisModule_ReplySetArrayLength(ctx, 3);
            }
        });
        assert_eq!(
            replies,
            vec![ReplyValue::Array(vec![
                ReplyValue::LongLong(1),
                ReplyValue::LongLong(2),
                ReplyValue::LongLong(3),
            ])]
        );
    }

    #[test]
    fn test_capture_nested_arrays() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                let ctx = std::ptr::null_mut();
                // Outer array
                RedisModule_ReplyWithArray(ctx, ffi::REDISMODULE_POSTPONED_ARRAY_LEN as c_longlong);
                RedisModule_ReplyWithLongLong(ctx, 1);
                // Inner array
                RedisModule_ReplyWithArray(ctx, ffi::REDISMODULE_POSTPONED_ARRAY_LEN as c_longlong);
                RedisModule_ReplyWithLongLong(ctx, 2);
                RedisModule_ReplyWithLongLong(ctx, 3);
                RedisModule_ReplySetArrayLength(ctx, 2);
                // Back to outer
                RedisModule_ReplyWithLongLong(ctx, 4);
                RedisModule_ReplySetArrayLength(ctx, 3);
            }
        });
        assert_eq!(
            replies,
            vec![ReplyValue::Array(vec![
                ReplyValue::LongLong(1),
                ReplyValue::Array(vec![ReplyValue::LongLong(2), ReplyValue::LongLong(3),]),
                ReplyValue::LongLong(4),
            ])]
        );
    }

    #[test]
    fn test_capture_map() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                let ctx = std::ptr::null_mut();
                RedisModule_ReplyWithMap(ctx, ffi::REDISMODULE_POSTPONED_LEN as c_longlong);
                RedisModule_ReplyWithSimpleString(ctx, c"key1".as_ptr());
                RedisModule_ReplyWithLongLong(ctx, 100);
                RedisModule_ReplyWithSimpleString(ctx, c"key2".as_ptr());
                RedisModule_ReplyWithDouble(ctx, 3.14);
                RedisModule_ReplySetMapLength(ctx, 2);
            }
        });
        assert_eq!(
            replies,
            vec![ReplyValue::Map(vec![
                (
                    ReplyValue::SimpleString("key1".to_string()),
                    ReplyValue::LongLong(100)
                ),
                (
                    ReplyValue::SimpleString("key2".to_string()),
                    ReplyValue::Double(3.14)
                ),
            ])]
        );
    }

    #[test]
    fn test_capture_empty_map() {
        let replies = capture_replies(|| {
            // SAFETY: Context is ignored in mock mode
            unsafe {
                RedisModule_ReplyWithMap(std::ptr::null_mut(), 0);
            }
        });
        assert_eq!(replies, vec![ReplyValue::Map(vec![])]);
    }
}
