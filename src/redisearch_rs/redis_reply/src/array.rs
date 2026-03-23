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

use crate::map::MapBuilder;
use crate::replier::Replier;

/// Builder for Redis arrays.
///
/// Operates in two modes based on construction:
/// - **Dynamic** (via [`Replier::array`]): Length is set on drop via Redis API
/// - **Fixed** (via [`Replier::fixed_array`]): Length was declared upfront; validates on drop
///
/// # Panics
///
/// In fixed mode, panics when dropped if the number of elements added doesn't match
/// the declared length.
pub struct ArrayBuilder<'a> {
    pub(crate) replier: &'a mut Replier,
    pub(crate) len: u32,
    /// `None` = dynamic (call ReplySetArrayLength on drop)
    /// `Some(n)` = fixed (assert len == n on drop)
    pub(crate) expected_len: Option<u32>,
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

    /// Start a nested dynamic array.
    ///
    /// The nested array counts as 1 element in the parent array.
    pub fn array(&mut self) -> ArrayBuilder<'_> {
        self.len += 1;
        self.replier.array()
    }

    /// Start a nested dynamic map.
    ///
    /// The nested map counts as 1 element in the parent array.
    pub fn map(&mut self) -> MapBuilder<'_> {
        self.len += 1;
        self.replier.map()
    }

    /// Start a nested fixed-size array.
    ///
    /// Use when you know the nested array's length upfront.
    /// The returned builder validates the element count on drop.
    pub fn fixed_array(&mut self, len: u32) -> ArrayBuilder<'_> {
        self.len += 1;
        self.replier.fixed_array(len)
    }

    /// Start a nested fixed-size map.
    ///
    /// Use when you know the nested map's length upfront.
    /// The returned builder validates the element count on drop.
    pub fn fixed_map(&mut self, len: u32) -> MapBuilder<'_> {
        self.len += 1;
        self.replier.fixed_map(len)
    }
}

impl Drop for ArrayBuilder<'_> {
    fn drop(&mut self) {
        if let Some(expected) = self.expected_len {
            // Fixed mode: validate count matches declaration
            assert_eq!(
                self.len, expected,
                "ArrayBuilder: declared length {} but added {} elements",
                expected, self.len
            );
        } else {
            // Dynamic mode: tell Redis the final length
            // SAFETY: ctx is validated at Replier construction
            unsafe {
                RedisModule_ReplySetArrayLength.expect("RedisModule_ReplySetArrayLength")(
                    self.replier.ctx,
                    i64::from(self.len),
                );
            }
        }
    }
}
