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

use crate::array::ArrayBuilder;
use crate::replier::Replier;

/// Builder for Redis maps.
///
/// Operates in two modes based on construction:
/// - **Dynamic** (via [`Replier::map`]): Length is set on drop via Redis API
/// - **Fixed** (via [`Replier::fixed_map`]): Length was declared upfront; validates on drop
///
/// Note: Unlike arrays, map length counts key-value pairs, not individual elements.
///
/// # Panics
///
/// In fixed mode, panics when dropped if the number of key-value pairs added doesn't match
/// the declared length.
pub struct MapBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) len: u32,
    /// `None` = dynamic (call ReplySetMapLength on drop)
    /// `Some(n)` = fixed (assert len == n on drop)
    pub(crate) expected_len: Option<u32>,
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

    /// Add a key with a nested dynamic array as value.
    ///
    /// Returns an [`ArrayBuilder`] for the nested array.
    pub fn kv_array(&mut self, key: &CStr) -> ArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.array()
    }

    /// Add a key with a nested dynamic map as value.
    ///
    /// Returns a [`MapBuilder`] for the nested map.
    pub fn kv_map(&mut self, key: &CStr) -> MapBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.map()
    }

    /// Add a key with a fixed-size nested array as value.
    ///
    /// Use when you know the nested array's length upfront.
    /// The returned builder validates the element count on drop.
    pub fn kv_fixed_array(&mut self, key: &CStr, len: u32) -> ArrayBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.fixed_array(len)
    }

    /// Add a key with a fixed-size nested map as value.
    ///
    /// Use when you know the nested map's length upfront.
    /// The returned builder validates the element count on drop.
    pub fn kv_fixed_map(&mut self, key: &CStr, len: u32) -> MapBuilder<'_> {
        self.replier.simple_string(key);
        self.len += 1;
        self.replier.fixed_map(len)
    }
}

impl Drop for MapBuilder<'_> {
    fn drop(&mut self) {
        if let Some(expected) = self.expected_len {
            // Fixed mode: validate count matches declaration
            assert_eq!(
                self.len, expected,
                "MapBuilder: declared length {} but added {} key-value pairs",
                expected, self.len
            );
        } else {
            // Dynamic mode: tell Redis the final length
            // SAFETY: ctx is validated at Replier construction
            unsafe {
                RedisModule_ReplySetMapLength.expect("RedisModule_ReplySetMapLength")(
                    self.replier.ctx,
                    i64::from(self.len),
                );
            }
        }
    }
}
