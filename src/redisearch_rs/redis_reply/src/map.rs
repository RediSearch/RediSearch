/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use ffi::RedisModule_ReplySetMapLength;

use crate::array::{ArrayBuilder, FixedArrayBuilder};
use crate::replier::Replier;

/// Builder for Redis maps with automatic length tracking.
///
/// When this builder is dropped, it automatically sets the map length
/// based on the number of key-value pairs added.
///
/// Note: Unlike arrays, map length counts key-value pairs, not individual elements.
pub struct MapBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) len: u32,
}

impl MapBuilder<'_> {
    /// Add a key-value pair where the value is a 64-bit signed integer.
    pub fn kv_long_long(&mut self, key: &CStr, value: i64) {
        self.replier.simple_string(key);
        self.replier.long_long(value);
        self.len += 1;
    }

    /// Add a key-value pair where the value is a double.
    pub fn kv_double(&mut self, key: &CStr, value: f64) {
        self.replier.simple_string(key);
        self.replier.double(value);
        self.len += 1;
    }

    /// Add a key with an empty array as value.
    pub fn kv_empty_array(&mut self, key: &CStr) {
        self.replier.simple_string(key);
        self.replier.empty_array();
        self.len += 1;
    }

    /// Add a key with an empty map as value.
    pub fn kv_empty_map(&mut self, key: &CStr) {
        self.replier.simple_string(key);
        self.replier.empty_map();
        self.len += 1;
    }

    /// Add a key with a nested array as value.
    ///
    /// Returns an [`ArrayBuilder`] for the nested array.
    pub fn kv_array(&mut self, key: &CStr) -> ArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.array()
    }

    /// Add a key with a nested map as value.
    ///
    /// Returns a [`MapBuilder`] for the nested map.
    pub fn kv_map(&mut self, key: &CStr) -> MapBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.map()
    }

    /// Add a key with a fixed-size nested map as value.
    ///
    /// Use when you know the nested map's length upfront.
    /// Returns a [`FixedMapBuilder`] that validates the element count on drop.
    pub fn kv_fixed_map(&mut self, key: &CStr, len: u32) -> FixedMapBuilder<'_> {
        self.replier.simple_string(key);
        self.replier.fixed_map(len);
        self.len += 1;
        FixedMapBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }

    /// Add a key with a fixed-size nested array as value.
    ///
    /// Use when you know the nested array's length upfront.
    /// Returns a [`FixedArrayBuilder`] that validates the element count on drop.
    pub fn kv_fixed_array(&mut self, key: &CStr, len: u32) -> FixedArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.replier.fixed_array(len);
        self.len += 1;
        FixedArrayBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }
}

impl Drop for MapBuilder<'_> {
    fn drop(&mut self) {
        // SAFETY: ctx is validated at Replier construction
        unsafe {
            RedisModule_ReplySetMapLength.expect("RedisModule_ReplySetMapLength")(
                self.replier.ctx,
                i64::from(self.len),
            );
        }
    }
}

/// Builder for Redis maps with a fixed, known-upfront length.
///
/// This builder tracks the expected length and panics in Drop if the actual
/// number of key-value pairs added doesn't match the declared length.
///
/// # Panics
///
/// Panics when dropped if the number of key-value pairs added doesn't match
/// the declared length.
pub struct FixedMapBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) expected_len: u32,
    pub(crate) actual_len: u32,
}

impl FixedMapBuilder<'_> {
    /// Add a key-value pair where the value is a 64-bit signed integer.
    pub fn kv_long_long(&mut self, key: &CStr, value: i64) {
        self.replier.simple_string(key);
        self.replier.long_long(value);
        self.actual_len += 1;
    }

    /// Add a key-value pair where the value is a double.
    pub fn kv_double(&mut self, key: &CStr, value: f64) {
        self.replier.simple_string(key);
        self.replier.double(value);
        self.actual_len += 1;
    }

    /// Add a key with an empty array as value.
    pub fn kv_empty_array(&mut self, key: &CStr) {
        self.replier.simple_string(key);
        self.replier.empty_array();
        self.actual_len += 1;
    }

    /// Add a key with an empty map as value.
    pub fn kv_empty_map(&mut self, key: &CStr) {
        self.replier.simple_string(key);
        self.replier.empty_map();
        self.actual_len += 1;
    }

    /// Add a key with a nested array as value.
    ///
    /// Returns an [`ArrayBuilder`] for the nested array.
    pub fn kv_array(&mut self, key: &CStr) -> ArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.actual_len += 1;
        self.replier.array()
    }

    /// Add a key with a nested map as value.
    ///
    /// Returns a [`MapBuilder`] for the nested map.
    pub fn kv_map(&mut self, key: &CStr) -> MapBuilder<'_> {
        self.replier.simple_string(key);
        self.actual_len += 1;
        self.replier.map()
    }

    /// Add a key with a fixed-size nested array as value.
    ///
    /// Use when you know the nested array's length upfront.
    pub fn kv_fixed_array(&mut self, key: &CStr, len: u32) -> FixedArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.replier.fixed_array(len);
        self.actual_len += 1;
        FixedArrayBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }

    /// Add a key with a fixed-size nested map as value.
    ///
    /// Use when you know the nested map's length upfront.
    pub fn kv_fixed_map(&mut self, key: &CStr, len: u32) -> FixedMapBuilder<'_> {
        self.replier.simple_string(key);
        self.replier.fixed_map(len);
        self.actual_len += 1;
        FixedMapBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }
}

impl Drop for FixedMapBuilder<'_> {
    fn drop(&mut self) {
        assert_eq!(
            self.actual_len, self.expected_len,
            "FixedMapBuilder: declared length {} but added {} key-value pairs",
            self.expected_len, self.actual_len
        );
    }
}
