/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use ffi::{
    REDISMODULE_POSTPONED_ARRAY_LEN, REDISMODULE_POSTPONED_LEN, RedisModule_ReplyWithArray,
    RedisModule_ReplyWithDouble, RedisModule_ReplyWithEmptyArray, RedisModule_ReplyWithLongLong,
    RedisModule_ReplyWithMap, RedisModule_ReplyWithSimpleString,
};

pub use ffi::RedisModuleCtx;

use crate::array::ArrayBuilder;
use crate::map::MapBuilder;

/// A wrapper for Redis module reply functions.
///
/// Validates the context once at construction and provides ergonomic
/// methods for building Redis protocol replies.
pub struct Replier {
    pub(crate) ctx: *mut RedisModuleCtx,
}

impl Replier {
    /// Create a new Replier.
    ///
    /// # Safety
    ///
    /// - `ctx` must be a valid Redis module context for the lifetime of this Replier.
    pub const unsafe fn new(ctx: *mut RedisModuleCtx) -> Self {
        debug_assert!(!ctx.is_null(), "ctx cannot be NULL");
        Self { ctx }
    }

    /// Reply with a 64-bit signed integer.
    pub fn long_long(&mut self, value: i64) {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithLongLong.expect("RedisModule_ReplyWithLongLong")(self.ctx, value);
        }
    }

    /// Reply with a double-precision floating point number.
    pub fn double(&mut self, value: f64) {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithDouble.expect("RedisModule_ReplyWithDouble")(self.ctx, value);
        }
    }

    /// Reply with a simple string.
    pub fn simple_string(&mut self, s: &CStr) {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithSimpleString.expect("RedisModule_ReplyWithSimpleString")(
                self.ctx,
                s.as_ptr(),
            );
        }
    }

    /// Reply with an empty array.
    pub fn empty_array(&mut self) {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithEmptyArray.expect("RedisModule_ReplyWithEmptyArray")(self.ctx);
        }
    }

    /// Reply with an empty map.
    pub fn empty_map(&mut self) {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithMap.expect("RedisModule_ReplyWithMap")(self.ctx, 0);
        }
    }

    /// Start building a dynamic array.
    ///
    /// The array length is automatically set when the returned [`ArrayBuilder`] is dropped.
    pub fn array(&mut self) -> ArrayBuilder<'_> {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithArray.expect("RedisModule_ReplyWithArray")(
                self.ctx,
                REDISMODULE_POSTPONED_ARRAY_LEN as i64,
            );
        }
        ArrayBuilder {
            replier: self,
            len: 0,
            expected_len: None,
        }
    }

    /// Start building a dynamic map.
    ///
    /// The map length is automatically set when the returned [`MapBuilder`] is dropped.
    /// Note: Map length counts key-value pairs, not individual elements.
    pub fn map(&mut self) -> MapBuilder<'_> {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithMap.expect("RedisModule_ReplyWithMap")(
                self.ctx,
                REDISMODULE_POSTPONED_LEN as i64,
            );
        }
        MapBuilder {
            replier: self,
            len: 0,
            expected_len: None,
        }
    }

    /// Start building a fixed-size array.
    ///
    /// The length is declared upfront. The returned [`ArrayBuilder`] validates
    /// on drop that the actual element count matches the declared length.
    pub fn fixed_array(&mut self, len: u32) -> ArrayBuilder<'_> {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithArray.expect("RedisModule_ReplyWithArray")(
                self.ctx,
                i64::from(len),
            );
        }
        ArrayBuilder {
            replier: self,
            len: 0,
            expected_len: Some(len),
        }
    }

    /// Start building a fixed-size map.
    ///
    /// The length is declared upfront. The returned [`MapBuilder`] validates
    /// on drop that the actual key-value pair count matches the declared length.
    pub fn fixed_map(&mut self, len: u32) -> MapBuilder<'_> {
        // SAFETY: ctx is validated at construction
        unsafe {
            RedisModule_ReplyWithMap.expect("RedisModule_ReplyWithMap")(self.ctx, i64::from(len));
        }
        MapBuilder {
            replier: self,
            len: 0,
            expected_len: Some(len),
        }
    }
}
