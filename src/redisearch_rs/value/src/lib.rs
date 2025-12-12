/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    shared::SharedRsValue,
    strings::{ConstString, RedisString, RmAllocString, RsValueString},
    trio::RsValueTrio,
};
use ffi::RedisModuleString;
use std::{ffi::c_char, fmt::Debug, ptr::NonNull};

/// Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
/// Rust port of the RSValue type.
#[cfg(feature = "c_ffi_impl")]
mod rs_value_ffi;
#[cfg(feature = "c_ffi_impl")]
pub use rs_value_ffi::*;

#[cfg(feature = "test_utils")]
mod test_utils;
#[cfg(feature = "test_utils")]
pub use test_utils::RSValueMock;

pub mod shared;
pub mod strings;
pub mod trio;

/// An actual [`RsValue`] object
#[derive(Debug, Clone)]
pub enum RsValue {
    /// Undefined, not holding a value.
    Undefined,
    /// Null value
    Null,
    /// Numeric value
    Number(f64),
    /// String value backed by a rm_alloc'd string
    RmAllocString(RmAllocString),
    /// String value backed by a constant C string
    ConstString(ConstString),
    /// String value backed by a Redis string
    RedisString(RedisString),
    /// String value
    String(Box<RsValueString>),
    /// Array value
    Array(Vec<SharedRsValue>),
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(Vec<(SharedRsValue, SharedRsValue)>),
}

impl Value for RsValue {
    fn from_value(value: RsValue) -> Self {
        value
    }
}

pub trait Value: Sized {
    /// Create a new value from an [`RsValue`]
    fn from_value(value: RsValue) -> Self;

    /// Create a new undefined value
    fn undefined() -> Self {
        Self::from_value(RsValue::Undefined)
    }

    /// Create a new NULL value
    fn null() -> Self {
        Self::from_value(RsValue::Null)
    }

    /// Create a new numeric value given the passed number
    fn number(n: f64) -> Self {
        Self::from_value(RsValue::Number(n))
    }

    /// Create a new string value
    fn string(s: RsValueString) -> Self {
        Self::from_value(RsValue::String(Box::new(s)))
    }

    /// Create a new reference value
    fn reference(reference: SharedRsValue) -> Self {
        Self::from_value(RsValue::Ref(reference))
    }

    /// Create a new trio value
    fn trio(left: SharedRsValue, middle: SharedRsValue, right: SharedRsValue) -> Self {
        Self::from_value(RsValue::Trio(RsValueTrio::new(left, middle, right)))
    }

    /// Create a new string value backed by an rm_alloc'd string.
    /// Takes ownership of the passed string.
    ///
    /// # Safety
    /// See [`RmAllocString::take_unchecked`]
    unsafe fn take_rm_alloc_string(str: NonNull<c_char>, len: u32) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`RmAllocString::take_unchecked`]
        Self::from_value(RsValue::RmAllocString(unsafe {
            RmAllocString::take_unchecked(str, len)
        }))
    }

    /// Create a new string value backed by an rm_alloc'd string
    /// that is copied from the passed data. Does not take ownership
    /// of the passed string.
    ///
    /// # Safety
    /// See [`RmAllocString::copy_from_string`]
    unsafe fn copy_rm_alloc_string(str: *const c_char, len: u32) -> Self {
        debug_assert!(!str.is_null(), "`str` must not be NULL");
        // Safety: caller must uphold the safety requirements of
        // [`RmAllocString::copy_from_string`].
        Self::from_value(RsValue::RmAllocString(unsafe {
            RmAllocString::copy_from_string(str, len)
        }))
    }

    /// Create a new value backed by a string constant.
    /// Does not take ownership of the string.
    ///
    /// # Safety
    /// See [`ConstString::new`]
    unsafe fn const_string(str: *const c_char, len: u32) -> Self {
        debug_assert!(!str.is_null(), "`str` must not be NULL");
        // Safety: caller must uphold the safety requirements of
        // [`ConstString::new`].
        Self::from_value(RsValue::ConstString(unsafe { ConstString::new(str, len) }))
    }

    /// Create a new value backed by a [`RedisModuleString`].
    /// Does not increment the reference count of the backing string
    /// and as such takes ownership.
    ///
    /// # Safety
    /// See [`RedisString::take`]
    unsafe fn redis_string(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`RedisString::take`].
        Self::from_value(RsValue::RedisString(unsafe { RedisString::take(str) }))
    }

    /// Create a new array value
    fn array(arr: Vec<SharedRsValue>) -> Self {
        Self::from_value(RsValue::Array(arr))
    }

    /// Create a new map value
    fn map(map: Vec<(SharedRsValue, SharedRsValue)>) -> Self {
        Self::from_value(RsValue::Map(map))
    }
}

#[cfg(test)]
redis_mock::bind_redis_alloc_symbols_to_mock_impl!();
