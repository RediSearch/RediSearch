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
use std::ffi::c_char;
use std::ptr;
use value::Value;

/// Selects which value an outer [`Value::Trio`] exposes to reply serialization.
#[cheadergen::config(prefix_with_name)]
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RSValueTrioSelection {
    Left = 0,
    Middle = 1,
    Right = 2,
}

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
    /// [`RSValueView::str_ptr`] / [`RSValueView::str_len`] hold the payload.
    /// Covers both owned and Redis-backed strings.
    String = 2,
    /// [`RSValueView::resolved`] is a container of [`RSValueView::len`]
    /// elements, addressable with [`RSValue_ArrayItem`](crate::array::RSValue_ArrayItem).
    Array = 3,
    /// [`RSValueView::resolved`] is a map of [`RSValueView::len`] entries,
    /// addressable with [`RSValue_Map_GetEntry`](crate::map::RSValue_Map_GetEntry).
    Map = 4,
}

/// The reply-side view of an [`RSValue`], returned by value from
/// [`RSValue_GetReplyView`].
//
// Field order goes widest-first: `repr(C)` lays fields out in declaration
// order, so grouping the 8-byte fields ahead of the 4-byte ones avoids
// interior padding (40 bytes instead of 48).
#[repr(C)]
pub struct RSValueView {
    /// The fully resolved value this view describes. Borrows from the input value.
    pub resolved: *const RSValue,
    /// String payload. Not NUL-terminated; may contain embedded NUL bytes.
    /// Borrows from the input value.
    pub str_ptr: *const c_char,
    /// Number payload.
    pub num: f64,
    /// Length of [`RSValueView::str_ptr`] in bytes. `usize` because
    /// Redis-backed strings carry `size_t` lengths; capping at `u32` would
    /// turn an oversized value into a reply-time abort.
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
            str_ptr: ptr::null(),
            str_len: 0,
            num: 0.0,
            len: 0,
        }
    }
}

/// Returns the reply-side view of `value` in a single call.
///
/// If `value` is a [`Value::Trio`], `trio_selection` chooses its exposed value.
/// References are then followed, and any further trio reached during resolution
/// collapses to its middle element, matching recursive reply serialization.
///
/// # Safety
///
/// 1. `value` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. The pointers in the returned view borrow from `value` and must not
///    outlive it.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_GetReplyView(
    value: *const RSValue,
    trio_selection: RSValueTrioSelection,
) -> RSValueView {
    // SAFETY: ensured by caller (1.)
    let mut value = unsafe { expect_value(value) };

    if let Value::Trio(trio) = value {
        value = match trio_selection {
            RSValueTrioSelection::Left => trio.left(),
            RSValueTrioSelection::Middle => trio.middle(),
            RSValueTrioSelection::Right => trio.right(),
        };
    }

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
            view.str_ptr = ptr;
            view.str_len = len as usize;
            view
        }
        Value::RedisString(str) => {
            let (ptr, len) = str.as_ptr_len();
            let mut view = RSValueView::new(V::String, value);
            view.str_ptr = ptr;
            view.str_len = len;
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
