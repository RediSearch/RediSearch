/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::c_char,
    fmt::{self, Debug},
    ptr::NonNull,
};

use crate::{
    collection::{RsValueArray, RsValueMap, RsValueMapEntry},
    dynamic::DynRsValueRef,
    shared::SharedRsValue,
    strings::{ConstString, OwnedRedisString, OwnedRmAllocString, RedisStringRef, RsValueString},
    trio::RsValueTrio,
};
use ffi::RedisModuleString;

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

pub mod collection;
pub mod dynamic;
pub mod shared;
pub mod strings;
pub mod trio;

/// Internal storage of [`RsValue`] and [`SharedRsValue`]
#[derive(Debug, Clone)]
pub enum RsValueInternal {
    /// Null value
    Null,
    /// Numeric value
    Number(f64),
    /// String value backed by a rm_alloc'd string
    RmAllocString(OwnedRmAllocString),
    /// String value backed by a constant C string
    ConstString(ConstString),
    /// String value backed by an owned Redis string
    OwnedRedisString(OwnedRedisString),
    /// String value backed by a borrowd Redis string
    BorrowedRedisString(RedisStringRef),
    /// String value
    String(RsValueString),
    /// Array value
    Array(RsValueArray),
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(RsValueMap),
}

/// A stack-allocated RediSearch dynamic value.
/// cbindgen:prefix-with-name
#[derive(Default, Clone)]
pub enum RsValue {
    #[default]
    /// Undefined, not holding a value.
    Undef,
    /// Defined and holding a value.
    Def(RsValueInternal),
}

impl RsValue {
    /// `const` function for creating a null value.
    pub const fn null_const() -> Self {
        Self::Def(RsValueInternal::Null)
    }
}

impl fmt::Debug for RsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.internal() {
            Some(internal) => internal.fmt(f),
            None => f.debug_tuple("Undefined").finish(),
        }
    }
}

impl Value for RsValue {
    fn from_internal(internal: RsValueInternal) -> Self {
        Self::Def(internal)
    }

    fn undefined() -> Self {
        Self::Undef
    }

    fn internal(&self) -> Option<&RsValueInternal> {
        match self {
            Self::Undef => None,
            Self::Def(internal) => Some(internal),
        }
    }

    fn to_dyn_ref(&self) -> DynRsValueRef<'_> {
        DynRsValueRef::from(self)
    }

    fn to_shared(&self) -> SharedRsValue {
        match self.internal() {
            Some(internal) => SharedRsValue::from_internal(internal.clone()),
            None => SharedRsValue::undefined(),
        }
    }
}

pub trait Value: Sized {
    /// Create a new value from an [`RsValueInternal`]
    fn from_internal(internal: RsValueInternal) -> Self;

    /// Create a new, undefined value
    fn undefined() -> Self;

    /// Clear this value
    fn clear(&mut self) {
        *self = Self::undefined();
    }

    /// Get a reference to the [`RsValueInternal`] that is
    /// held by this value if it is defined. Returns `None` if
    /// the value is undefined.
    fn internal(&self) -> Option<&RsValueInternal>;

    /// Create a [`DynRsValueRef`] holding a reference
    /// to `self`.
    fn to_dyn_ref(&self) -> DynRsValueRef<'_>;

    /// Convert `Self` into a [`SharedRsValue`].
    fn to_shared(&self) -> SharedRsValue;

    /// Swap `self`'s value with the passed [`RsValueInternal`].
    fn swap_from_internal(&mut self, internal: RsValueInternal) {
        *self = Self::from_internal(internal);
    }

    /// Create a new, NULL value
    fn null() -> Self {
        Self::from_internal(RsValueInternal::Null)
    }

    /// Create a new numeric value given the passed number
    fn number(n: f64) -> Self {
        Self::from_internal(RsValueInternal::Number(n))
    }

    /// Create a new string value
    fn string(s: RsValueString) -> Self {
        Self::from_internal(RsValueInternal::String(s))
    }

    /// Create a new trio value
    fn trio(left: SharedRsValue, middle: SharedRsValue, right: SharedRsValue) -> Self {
        Self::from_internal(RsValueInternal::Trio(RsValueTrio::new(left, middle, right)))
    }

    /// Create a new string value backed by an rm_alloc'd string.
    /// Takes ownership of the passed string.
    ///
    /// # Safety
    /// See [`OwnedRmAllocString::take_unchecked`]
    unsafe fn take_rm_alloc_string(str: NonNull<c_char>, len: u32) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`OwnedRmAllocString::take_unchecked`]
        Self::from_internal(RsValueInternal::RmAllocString(unsafe {
            OwnedRmAllocString::take_unchecked(str, len)
        }))
    }

    /// Create a new string value backed by an rm_alloc'd string
    /// that is copied from the passed data. Does not take ownership
    /// of the passed string.
    ///
    /// # Safety
    /// See [`OwnedRmAllocString::copy_from_string`]
    unsafe fn copy_rm_alloc_string(str: *const c_char, len: u32) -> Self {
        debug_assert!(!str.is_null(), "`str` must not be NULL");
        // Safety: caller must uphold the safety requirements of
        // [`OwnedRmAllocString::copy_from_string`].
        Self::from_internal(RsValueInternal::RmAllocString(unsafe {
            OwnedRmAllocString::copy_from_string(str, len)
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
        Self::from_internal(RsValueInternal::ConstString(unsafe {
            ConstString::new(str, len)
        }))
    }

    /// Create a new value backed by a reference to a RedisModuleString.
    /// Does not increment the reference count of the backing string.
    ///
    /// # Safety
    /// See [`RedisStringRef::new_unchecked`]
    unsafe fn borrowed_redis_string(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`RedisStringRef::new_unchecked`].
        Self::from_internal(RsValueInternal::BorrowedRedisString(unsafe {
            RedisStringRef::new_unchecked(str)
        }))
    }

    /// Create a new value backed by a [`RedisModuleString`].
    /// Increments the reference count of the backing string.
    ///
    /// # Safety
    /// See [`OwnedRedisString::retain`]
    unsafe fn retain_owned_redis_string(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`OwnedRedisString::retain`].
        Self::from_internal(RsValueInternal::OwnedRedisString(unsafe {
            OwnedRedisString::retain(str)
        }))
    }

    /// Create a new value backed by a [`RedisModuleString`].
    /// Does not increment the reference count of the backing string
    /// and as such takes ownership.
    ///
    /// # Safety
    /// See [`OwnedRedisString::take`]
    unsafe fn take_owned_redis_string(str: NonNull<RedisModuleString>) -> Self {
        // Safety: caller must uphold the safety requirements of
        // [`OwnedRedisString::take`].
        Self::from_internal(RsValueInternal::OwnedRedisString(unsafe {
            OwnedRedisString::take(str)
        }))
    }

    /// Create a new array value
    fn array(arr: RsValueArray) -> Self {
        Self::from_internal(RsValueInternal::Array(arr))
    }

    /// Create a new map value
    fn map(map: RsValueMap) -> Self {
        Self::from_internal(RsValueInternal::Map(map))
    }

    /// Repeatedly dereference `self` until
    /// ending up at a non-reference value
    ///
    /// Calls [`Value::to_dyn_ref`] to convert the reference
    /// to to a `DynRsValueRef`.
    fn deep_deref(&self) -> DynRsValueRef<'_> {
        match self.internal() {
            Some(RsValueInternal::Ref(ref_v)) => ref_v.deep_deref(),
            _ => self.to_dyn_ref(),
        }
    }

    /// Attempt to parse the passed string as an `f64`, and wrap it
    /// in a value.
    fn parse_number(s: &str) -> Result<Self, std::num::ParseFloatError> {
        Ok(Self::number(s.parse()?))
    }

    /// Get the number value. Returns `None` if the value is not
    /// a number.
    fn get_number(&self) -> Option<f64> {
        let RsValueInternal::Number(number) = self.internal()? else {
            return None;
        };
        Some(*number)
    }

    /// Convert the value to a number in-place,
    /// dropping the existing value.
    fn to_number(&mut self, n: f64) {
        self.swap_from_internal(RsValueInternal::Number(n));
    }

    /// Convert the value to a reference to the passed [`SharedRsValue`] in-place,
    /// dropping the existing value,
    fn to_reference(&mut self, other: SharedRsValue) {
        self.swap_from_internal(RsValueInternal::Ref(other));
    }

    /// Get a reference to [`OwnedRmAllocString`] being
    /// wrapped by this value if so, or `None` otherwise.
    fn as_rm_alloc_string(&self) -> Option<&OwnedRmAllocString> {
        match self.internal()? {
            RsValueInternal::RmAllocString(s) => Some(s),
            _ => None,
        }
    }

    /// Get a reference to [`ConstString`] being
    /// wrapped by this value if so, or `None` otherwise.
    fn as_const_string(&self) -> Option<&ConstString> {
        match self.internal()? {
            RsValueInternal::ConstString(s) => Some(s),
            _ => None,
        }
    }

    /// Get a reference to [`OwnedRedisString`] being
    /// wrapped by this value if so, or `None` otherwise.
    fn as_owned_redis_string(&self) -> Option<&OwnedRedisString> {
        match self.internal()? {
            RsValueInternal::OwnedRedisString(s) => Some(s),
            _ => None,
        }
    }

    /// Get a reference to [`RedisStringRef`] being
    /// wrapped by this value if so, or `None` otherwise.
    fn as_borrowed_redis_string(&self) -> Option<&RedisStringRef> {
        match self.internal()? {
            RsValueInternal::BorrowedRedisString(s) => Some(s),
            _ => None,
        }
    }

    /// Get the entry at the passed index of the
    /// array wrapped by this value if so, or `None` if
    /// the value is not an array or the index is out of bounds.
    fn array_get(&self, i: u32) -> Option<&SharedRsValue> {
        match self.internal()? {
            RsValueInternal::Array(array) => array.get(i),
            _ => None,
        }
    }

    /// Get the capacity of the array being
    /// wrapped by this value if so, or `None` otherwise.
    fn array_cap(&self) -> Option<u32> {
        match self.internal()? {
            RsValueInternal::Array(array) => Some(array.cap()),
            _ => None,
        }
    }

    /// Get the entry at the passed index of the
    /// map wrapped by this value if so, or `None` if
    /// the value is not a map or the index is out of bounds.
    fn map_get(&self, i: u32) -> Option<&RsValueMapEntry> {
        match self.internal()? {
            RsValueInternal::Map(map) => map.get(i),
            _ => None,
        }
    }

    /// Get the capacity of the map being
    /// wrapped by this value if so, or `None` otherwise.
    fn map_cap(&self) -> Option<u32> {
        match self.internal()? {
            RsValueInternal::Map(map) => Some(map.cap()),
            _ => None,
        }
    }

    /// Get a reference to [`RsValueTrio`] being
    /// wrapped by this value if so, or `None` otherwise.
    fn as_trio(&self) -> Option<&RsValueTrio> {
        match self.internal()? {
            RsValueInternal::Trio(trio) => Some(trio),
            _ => None,
        }
    }

    /// Get the string value as a slice of bytes.
    /// Note: not all string variants guarantee UTF-8 encoding.
    /// Returns `None` if the value is not of one of the string types.
    fn string_as_bytes(&self) -> Option<&[u8]> {
        let bytes = match self.internal()? {
            RsValueInternal::RmAllocString(string) => string.as_bytes(),
            RsValueInternal::ConstString(string) => string.as_bytes(),
            RsValueInternal::OwnedRedisString(string) => string.as_bytes(),
            RsValueInternal::BorrowedRedisString(string) => string.as_bytes(),
            RsValueInternal::String(string) => string.as_str().as_bytes(),
            _ => return None,
        };

        Some(bytes)
    }
}

#[cfg(test)]
redis_mock::bind_redis_alloc_symbols_to_mock_impl!();
