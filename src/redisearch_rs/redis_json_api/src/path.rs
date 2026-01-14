/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! JSON path parsing and querying.

use std::os::raw::c_void;
use std::ptr::NonNull;

use crate::RedisJsonApi;

/// A parsed JSON path expression.
///
/// JSON paths are used to query specific values within a JSON document.
/// This type represents a pre-parsed path that can be efficiently reused
/// for multiple queries.
///
/// # Ownership
///
/// This type owns the underlying parsed path and will free it on drop.
///
/// # Examples
///
/// Common JSON path patterns:
/// - `$` - the root element
/// - `$.field` - a specific field
/// - `$.array[0]` - first array element
/// - `$.array[*]` - all array elements
/// - `$..field` - recursive descent
pub struct JsonPath<'a> {
    ptr: NonNull<c_void>,
    api: &'a RedisJsonApi,
}

impl<'a> JsonPath<'a> {
    /// Creates a new JSON path from a raw pointer.
    #[inline]
    pub(crate) const fn new(ptr: NonNull<c_void>, api: &'a RedisJsonApi) -> Self {
        Self { ptr, api }
    }

    /// Returns `true` if this path selects at most one value.
    ///
    /// A path is "single" if it doesn't contain wildcards or recursive
    /// descent operators that could match multiple values.
    #[inline]
    pub fn is_single(&self) -> bool {
        let api = self.api.api();
        let path_is_single = api.pathIsSingle.expect("pathIsSingle not available");
        // SAFETY: ptr is valid by construction
        unsafe { path_is_single(self.ptr.as_ptr()) != 0 }
    }

    /// Returns `true` if this path has a defined iteration order.
    ///
    /// Paths with defined order will always return results in the same
    /// order when applied to the same document. Paths with wildcards
    /// or recursive descent may not have a defined order.
    #[inline]
    pub fn has_defined_order(&self) -> bool {
        let api = self.api.api();
        let path_has_defined_order = api.pathHasDefinedOrder.expect("pathHasDefinedOrder not available");
        // SAFETY: ptr is valid by construction
        unsafe { path_has_defined_order(self.ptr.as_ptr()) != 0 }
    }

    /// Returns the raw pointer to the parsed path.
    #[inline]
    pub const fn as_ptr(&self) -> *const c_void {
        self.ptr.as_ptr()
    }
}

impl Drop for JsonPath<'_> {
    fn drop(&mut self) {
        let api = self.api.api();
        if let Some(path_free) = api.pathFree {
            // SAFETY: ptr is valid by construction
            unsafe { path_free(self.ptr.as_ptr()) };
        }
    }
}

impl std::fmt::Debug for JsonPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonPath")
            .field("is_single", &self.is_single())
            .field("has_defined_order", &self.has_defined_order())
            .field("ptr", &self.ptr.as_ptr())
            .finish()
    }
}
