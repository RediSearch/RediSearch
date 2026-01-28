/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use ffi::RedisModule_ReplySetArrayLength;

use crate::map::{FixedMapBuilder, MapBuilder};
use crate::replier::Replier;

/// Builder for Redis arrays with automatic length tracking.
///
/// When this builder is dropped, it automatically sets the array length
/// based on the number of elements added.
pub struct ArrayBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) len: u32,
}

impl ArrayBuilder<'_> {
    /// Add a 64-bit signed integer to the array.
    pub fn long_long(&mut self, value: i64) {
        self.replier.long_long(value);
        self.len += 1;
    }

    /// Add a double-precision floating point number to the array.
    pub fn double(&mut self, value: f64) {
        self.replier.double(value);
        self.len += 1;
    }

    /// Add a simple string to the array.
    pub fn simple_string(&mut self, s: &CStr) {
        self.replier.simple_string(s);
        self.len += 1;
    }

    /// Add an empty array to the array.
    pub fn empty_array(&mut self) {
        self.replier.empty_array();
        self.len += 1;
    }

    /// Add an empty map to the array.
    pub fn empty_map(&mut self) {
        self.replier.empty_map();
        self.len += 1;
    }

    /// Start a nested array within this array.
    ///
    /// The nested array counts as 1 element in the parent array.
    pub fn array(&mut self) -> ArrayBuilder<'_> {
        self.len += 1;
        self.replier.array()
    }

    /// Start a nested map within this array.
    ///
    /// The nested map counts as 1 element in the parent array.
    pub fn map(&mut self) -> MapBuilder<'_> {
        self.len += 1;
        self.replier.map()
    }

    /// Add a fixed-size nested array within this array.
    ///
    /// Use when you know the nested array's length upfront.
    /// Returns a [`FixedArrayBuilder`] that validates the element count on drop.
    pub fn fixed_array(&mut self, len: u32) -> FixedArrayBuilder<'_> {
        self.len += 1;
        self.replier.fixed_array(len);
        FixedArrayBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }

    /// Add a fixed-size nested map within this array.
    ///
    /// Use when you know the nested map's length upfront.
    /// Returns a [`FixedMapBuilder`] that validates the element count on drop.
    pub fn fixed_map(&mut self, len: u32) -> FixedMapBuilder<'_> {
        self.len += 1;
        self.replier.fixed_map(len);
        FixedMapBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }
}

impl Drop for ArrayBuilder<'_> {
    fn drop(&mut self) {
        // SAFETY: ctx is validated at Replier construction
        unsafe {
            RedisModule_ReplySetArrayLength.expect("RedisModule_ReplySetArrayLength")(
                self.replier.ctx,
                i64::from(self.len),
            );
        }
    }
}

/// Builder for Redis arrays with a fixed, known-upfront length.
///
/// This builder tracks the expected length and panics in Drop if the actual
/// number of elements added doesn't match the declared length.
///
/// # Panics
///
/// Panics when dropped if the number of elements added doesn't match the
/// declared length.
pub struct FixedArrayBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) expected_len: u32,
    pub(crate) actual_len: u32,
}

impl FixedArrayBuilder<'_> {
    /// Add a 64-bit signed integer to the array.
    pub fn long_long(&mut self, value: i64) {
        self.replier.long_long(value);
        self.actual_len += 1;
    }

    /// Add a double-precision floating point number to the array.
    pub fn double(&mut self, value: f64) {
        self.replier.double(value);
        self.actual_len += 1;
    }

    /// Add a simple string to the array.
    pub fn simple_string(&mut self, s: &CStr) {
        self.replier.simple_string(s);
        self.actual_len += 1;
    }

    /// Add an empty array to the array.
    pub fn empty_array(&mut self) {
        self.replier.empty_array();
        self.actual_len += 1;
    }

    /// Add an empty map to the array.
    pub fn empty_map(&mut self) {
        self.replier.empty_map();
        self.actual_len += 1;
    }

    /// Start a nested array within this array.
    ///
    /// The nested array counts as 1 element in the parent array.
    pub fn array(&mut self) -> ArrayBuilder<'_> {
        self.actual_len += 1;
        self.replier.array()
    }

    /// Start a nested map within this array.
    ///
    /// The nested map counts as 1 element in the parent array.
    pub fn map(&mut self) -> MapBuilder<'_> {
        self.actual_len += 1;
        self.replier.map()
    }

    /// Add a fixed-size nested array within this array.
    ///
    /// Use when you know the nested array's length upfront.
    pub fn fixed_array(&mut self, len: u32) -> FixedArrayBuilder<'_> {
        self.actual_len += 1;
        self.replier.fixed_array(len);
        FixedArrayBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }

    /// Add a fixed-size nested map within this array.
    ///
    /// Use when you know the nested map's length upfront.
    pub fn fixed_map(&mut self, len: u32) -> FixedMapBuilder<'_> {
        self.actual_len += 1;
        self.replier.fixed_map(len);
        FixedMapBuilder {
            replier: self.replier,
            expected_len: len,
            actual_len: 0,
        }
    }
}

impl Drop for FixedArrayBuilder<'_> {
    fn drop(&mut self) {
        assert_eq!(
            self.actual_len, self.expected_len,
            "FixedArrayBuilder: declared length {} but added {} elements",
            self.expected_len, self.actual_len
        );
    }
}
