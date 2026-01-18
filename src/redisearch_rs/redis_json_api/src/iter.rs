/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator types for JSON values.

use std::os::raw::c_void;
use std::ptr::NonNull;

use ffi::{RedisModuleCtx, RedisModuleString};

use crate::error::{JsonApiError, Result};
use crate::value::{JsonValue, OwnedJsonValue};
use crate::RedisJsonApi;

/// An iterator over JSON query results.
///
/// This iterator is returned by [`JsonValue::get`] and yields
/// all values matching a JSON path expression.
///
/// # Ownership
///
/// This type owns the underlying C iterator and will free it on drop.
pub struct JsonResultsIter<'a> {
    ptr: NonNull<c_void>,
    api: &'a RedisJsonApi,
}

impl<'a> JsonResultsIter<'a> {
    /// Creates a new results iterator from a raw pointer.
    #[inline]
    pub(crate) const fn new(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Returns the number of results in this iterator.
    #[inline]
    pub fn len(&self) -> usize {
        let api = self.api.api();
        let len = api.len.expect("len not available");
        // SAFETY: ptr is valid by construction
        unsafe { len(self.ptr.as_ptr()) }
    }

    /// Returns `true` if the iterator contains no results.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the next value in the iterator.
    ///
    /// Returns `None` when all values have been consumed.
    #[inline]
    pub fn next_value(&mut self) -> Option<JsonValue<'a>> {
        let api = self.api.api();
        let next = api.next?;
        // SAFETY: ptr is valid by construction
        let value_ptr = unsafe { next(self.ptr.as_ptr()) };
        NonNull::new(value_ptr as *mut c_void).map(|ptr| JsonValue::new(ptr, self.api))
    }

    /// Resets the iterator to the beginning.
    #[inline]
    pub fn reset(&mut self) {
        let api = self.api.api();
        if let Some(reset_iter) = api.resetIter {
            // SAFETY: ptr is valid by construction
            unsafe { reset_iter(self.ptr.as_ptr()) };
        }
    }

    /// Serializes all results to a JSON string.
    ///
    /// This does not consume the iterator.
    ///
    /// # Safety
    ///
    /// `ctx` must be a valid Redis module context.
    #[inline]
    pub unsafe fn get_json(&self, ctx: *mut RedisModuleCtx) -> Result<*mut RedisModuleString> {
        let api = self.api.api();
        let get_json_from_iter = api.getJSONFromIter.expect("getJSONFromIter not available");
        let mut str: *mut RedisModuleString = std::ptr::null_mut();
        // SAFETY: ptr and ctx are valid by construction/caller guarantee
        let status = unsafe { get_json_from_iter(self.ptr.as_ptr(), ctx, &mut str) };
        if status == ffi::REDISMODULE_OK as std::os::raw::c_int {
            Ok(str)
        } else {
            Err(JsonApiError::OperationFailed)
        }
    }

    /// Returns the raw pointer to the iterator.
    #[inline]
    pub const fn as_ptr(&self) -> *mut c_void {
        self.ptr.as_ptr()
    }
}

impl<'a> Iterator for JsonResultsIter<'a> {
    type Item = JsonValue<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next_value()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (0, Some(len))
    }
}

impl Drop for JsonResultsIter<'_> {
    fn drop(&mut self) {
        let api = self.api.api();
        if let Some(free_iter) = api.freeIter {
            // SAFETY: ptr is valid by construction
            unsafe { free_iter(self.ptr.as_ptr()) };
        }
    }
}

impl std::fmt::Debug for JsonResultsIter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonResultsIter")
            .field("len", &self.len())
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}

/// An iterator over the key-value pairs of a JSON object.
///
/// This iterator is returned by [`JsonValue::key_values`] and yields
/// tuples of (key_name, value) for each entry in the object.
///
/// # Ownership
///
/// This type owns the underlying C iterator and will free it on drop.
/// The key names yielded are owned Redis module strings that must be
/// freed by the caller.
pub struct KeyValuesIter<'a> {
    ptr: NonNull<c_void>,
    api: &'a RedisJsonApi,
    /// Pre-allocated container for receiving values
    value_container: Option<OwnedJsonValue<'a>>,
}

impl<'a> KeyValuesIter<'a> {
    /// Creates a new key-values iterator from a raw pointer.
    #[inline]
    pub(crate) fn new(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        // Pre-allocate a container for receiving values
        let value_container = Some(api.alloc_json());
        Self {
            ptr,
            api,
            value_container,
        }
    }

    /// Returns the next key-value pair in the object.
    ///
    /// Returns `None` when all pairs have been consumed.
    ///
    /// # Note
    ///
    /// The returned key is a raw Redis module string pointer that the caller
    /// is responsible for freeing.
    #[inline]
    pub fn next_pair(&mut self) -> Option<(*mut RedisModuleString, JsonValue<'a>)> {
        let api = self.api.api();
        let next_key_value = api.nextKeyValue?;
        let value_container = self.value_container.as_mut()?;

        let mut key_name: *mut RedisModuleString = std::ptr::null_mut();
        // SAFETY: ptr and value_container.ptr are valid by construction
        let status =
            unsafe { next_key_value(self.ptr.as_ptr(), &mut key_name, value_container.as_ptr()) };

        if status == ffi::REDISMODULE_OK as std::os::raw::c_int {
            // Get the value from the container
            value_container.value().map(|v| (key_name, v))
        } else {
            None
        }
    }

    /// Returns the raw pointer to the iterator.
    #[inline]
    pub const fn as_ptr(&self) -> *mut c_void {
        self.ptr.as_ptr()
    }
}

impl Drop for KeyValuesIter<'_> {
    fn drop(&mut self) {
        // Drop the value container first
        self.value_container.take();

        let api = self.api.api();
        if let Some(free_key_values_iter) = api.freeKeyValuesIter {
            // SAFETY: ptr is valid by construction
            unsafe { free_key_values_iter(self.ptr.as_ptr()) };
        }
    }
}

impl std::fmt::Debug for KeyValuesIter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyValuesIter")
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}
