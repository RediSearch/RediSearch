/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A one-hop reply view over an [`RSValue`].
//!
//! Reply serialization used to interrogate a value with a chain of FFI calls
//! (dereference, type, payload getter — each a separate boundary crossing per
//! field per row). [`RSValue_GetReplyView`] answers all of it in a single
//! call: it resolves the value like the reply path does and returns a flat
//! struct carrying the discriminant and the scalar payload.

use crate::RSValue;
use crate::util::expect_value;
use redis_module::RedisModuleString;
use std::ffi::c_char;
use std::ptr;
use value::Value;

/// Discriminant of [`RSValueView`], selecting which payload fields are
/// meaningful.
#[cheadergen::config(prefix_with_name)]
#[repr(C)]
#[derive(Debug, PartialEq)]
pub enum RSValueViewType {
    /// No payload. Covers both null and undefined values.
    Null = 0,
    /// [`RSValueView::num`] holds the payload.
    Number = 1,
    /// [`RSValueView::string`] holds [`RSValueStringPointer::bytes`], with
    /// [`RSValueView::str_len`] bytes.
    String = 2,
    /// [`RSValueView::resolved`] is a container of [`RSValueView::len`]
    /// elements, addressable with [`RSValue_ArrayItem`](crate::array::RSValue_ArrayItem).
    Array = 3,
    /// [`RSValueView::resolved`] is a map of [`RSValueView::len`] entries,
    /// addressable with [`RSValue_Map_GetEntry`](crate::map::RSValue_Map_GetEntry).
    Map = 4,
    /// [`RSValueView::string`] holds [`RSValueStringPointer::redis_string`].
    RedisString = 5,
}

/// Borrowed string payload selected by [`RSValueView::view_type`].
/// Sharing pointer storage keeps Redis-backed replies from enlarging the view.
#[repr(C)]
pub union RSValueStringPointer {
    /// Bytes for [`RSValueViewType::String`]; not NUL-terminated.
    pub bytes: *const c_char,
    /// Original object for [`RSValueViewType::RedisString`], allowing C to reply
    /// without calling `RedisModule_StringPtrLen`.
    pub redis_string: *const RedisModuleString,
}

/// The reply-side view of an [`RSValue`], returned by value from
/// [`RSValue_GetReplyView`].
//
// Field order goes widest-first: `repr(C)` lays fields out in declaration
// order, so grouping the 8-byte fields ahead of the 4-byte ones avoids
// interior padding (40 bytes instead of 48).
#[repr(C)]
pub struct RSValueView {
    /// The fully resolved value this view describes: references followed,
    /// trios collapsed to their middle element. Borrows from the input value.
    pub resolved: *const RSValue,
    /// Borrows from the input value; the active member is selected by
    /// [`RSValueView::view_type`].
    pub string: RSValueStringPointer,
    /// Number payload.
    pub num: f64,
    /// Byte length of [`RSValueStringPointer::bytes`] for [`RSValueViewType::String`].
    pub str_len: usize,
    /// Which payload fields are meaningful.
    pub view_type: RSValueViewType,
    /// Element count of an array or entry count of a map.
    pub len: u32,
}

impl RSValueView {
    const fn new(view_type: RSValueViewType, resolved: &Value) -> Self {
        Self {
            view_type,
            resolved: ptr::from_ref(resolved).cast(),
            string: RSValueStringPointer { bytes: ptr::null() },
            str_len: 0,
            num: 0.0,
            len: 0,
        }
    }
}

/// Returns the reply-side view of `value` in a single call.
///
/// The value is resolved the way `RedisModule_Reply_RSValue` historically
/// resolved it: references are followed, and a trio reached during resolution
/// collapses to its middle element (format-driven trio selection is the
/// caller's concern and must happen before this call).
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. The pointers in the returned view borrow from `value` and must not
///    outlive it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_GetReplyView(value: *const RSValue) -> RSValueView {
    // SAFETY: ensured by caller (1.)
    let mut value = unsafe { expect_value(value) };

    loop {
        value = value.fully_dereferenced_ref();
        match value {
            Value::Trio(trio) => value = trio.middle(),
            _ => break,
        }
    }

    use RSValueViewType as V;
    match value {
        Value::Null | Value::Undefined => RSValueView::new(V::Null, value),
        Value::Number(num) => {
            let mut view = RSValueView::new(V::Number, value);
            view.num = *num;
            view
        }
        Value::String(str) => {
            let (ptr, len) = str.as_ptr_len();
            let mut view = RSValueView::new(V::String, value);
            view.string = RSValueStringPointer { bytes: ptr };
            view.str_len = len as usize;
            view
        }
        Value::RedisString(str) => {
            let mut view = RSValueView::new(V::RedisString, value);
            view.string = RSValueStringPointer {
                redis_string: str.as_ptr(),
            };
            view
        }
        Value::Array(array) => {
            let mut view = RSValueView::new(V::Array, value);
            view.len = array.len_u32();
            view
        }
        Value::Map(map) => {
            let mut view = RSValueView::new(V::Map, value);
            view.len = map.len_u32();
            view
        }
        // Unreachable: the resolution loop above only exits on non-Ref,
        // non-Trio variants.
        Value::Ref(_) | Value::Trio(_) => unreachable!(),
    }
}
