/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

use std::mem::{self, offset_of};
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::{cmp, ffi::CString};

use rlookup::{RLookup, RLookupKeyFlags, RLookupRow};
use rlookup_ffi::row::{
    RLookupRow_MoveFieldsFrom, RLookupRow_WriteByName, RLookupRow_WriteByNameOwned,
    RLookupRow_WriteFieldsFrom,
};
use std::ffi::c_char;
use std::ffi::c_int;
use value::RSValueFFI;
use value::RSValueTrait;

#[test]
#[should_panic(expected = "`src` and `dst` must not be the same")]
fn rlookuprow_movefieldsfrom_same_row() {
    let lookup = RLookup::new();
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(row.as_opaque_non_null()),
            Some(row.as_opaque_non_null()),
        );
    }
}

#[test]
fn rlookuprow_movefieldsfrom_different_rows() {
    let lookup = RLookup::new();
    let mut src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(src_row.as_opaque_non_null()),
            Some(dst_row.as_opaque_non_null()),
        );
    }

    // No panic was triggered.
}

#[test]
#[should_panic(expected = "`src_row` and `dst_row` must not be the same")]
fn rlookuprow_writefieldsfrom_same_row() {
    let src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    unsafe {
        RLookupRow_WriteFieldsFrom(
            row.as_opaque_ptr(),
            ptr::from_ref(&src_lookup),
            Some(row.as_opaque_non_null()),
            Some(NonNull::from(&mut dst_lookup)),
            false,
        )
    };
}

#[test]
#[should_panic(expected = "`src_lookup` and `dst_lookup` must not be the same")]
fn rlookuprow_writefieldsfrom_same_lookup() {
    let mut lookup = RLookup::new();
    let src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    unsafe {
        RLookupRow_WriteFieldsFrom(
            src_row.as_opaque_ptr(),
            ptr::from_ref(&lookup),
            Some(dst_row.as_opaque_non_null()),
            Some(NonNull::from(&mut lookup)),
            false,
        )
    };
}

#[test]
fn rlookuprow_writefieldsfrom_different_lookups_and_rows() {
    let src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();
    let src_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let mut dst_row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    unsafe {
        RLookupRow_WriteFieldsFrom(
            src_row.as_opaque_ptr(),
            ptr::from_ref(&src_lookup),
            Some(dst_row.as_opaque_non_null()),
            Some(NonNull::from(&mut dst_lookup)),
            false,
        );
    }

    // No panic was triggered.
}

#[test]
fn rlookuprow_move() {
    let mut lookup = RLookup::new();

    let mut src: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let mut dst: RLookupRow<'_, RSValueFFI> = RLookupRow::new();

    let key = lookup
        .get_key_write(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    src.write_key(key, RSValueFFI::create_num(42.0));

    #[cfg(debug_assertions)]
    {
        src.assert_valid("tests::row::rlookuprow_move");
        dst.assert_valid("tests::row::rlookuprow_move");
    }

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(src.as_opaque_non_null()),
            Some(dst.as_opaque_non_null()),
        )
    }

    assert!(src.num_dyn_values() == 0);
    let key = lookup
        .get_key_read(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    assert!(dst.get(key).is_some());
}

#[test]
fn rlookuprow_writebyname() {
    let mut lookup = RLookup::new();
    let name = CString::new("name").unwrap();
    let len = 4;
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let value = unsafe { RSValueFFI::from_raw(NonNull::new(RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByName(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(row.as_opaque_non_null()),
            NonNull::new(value.as_ptr()),
        );
    }

    assert_eq!(value.refcount(), 2);
}

#[test]
fn rlookuprow_writebynameowned() {
    let mut lookup = RLookup::new();
    let name = CString::new("name").unwrap();
    let len = 4;
    let mut row: RLookupRow<'_, RSValueFFI> = RLookupRow::new();
    let value = unsafe { RSValueFFI::from_raw(NonNull::new(RSValue_NewNumber(42.0)).unwrap()) };

    assert_eq!(value.refcount(), 1);

    unsafe {
        RLookupRow_WriteByNameOwned(
            Some(NonNull::from(&mut lookup)),
            name.as_ptr(),
            len,
            Some(row.as_opaque_non_null()),
            NonNull::new(value.as_ptr()),
        );
    }

    assert_eq!(value.refcount(), 1);

    // See the comment regarding `mem::forget()` at the end of `RLookupRow_WriteByName()` for more info.
    mem::forget(value);
}

/// Mock implementation of `RSValue_IncrRef` for testing purposes
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_IncrRef(v: *mut ffi::RSValue) -> *mut ffi::RSValue {
    unsafe { Arc::increment_strong_count(v as *mut f64) };
    v
}

/// Mock implementation of `RSValue_DecrRef` for testing purposes
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_DecrRef(v: *mut ffi::RSValue) {
    unsafe { Arc::from_raw(v as *mut f64) };
}

/// Mock implementation of `RSValue_NewNumber` for testing purposes
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_NewNumber(numval: f64) -> *mut ffi::RSValue {
    Arc::into_raw(Arc::new(numval)) as *mut ffi::RSValue
}

/// Mock implementation of `RSValue_Refcount` for testing purposes
#[unsafe(no_mangle)]
pub extern "C" fn RSValue_Refcount(v: *mut ffi::RSValue) -> u16 {
    let arc = unsafe { Arc::from_raw(v as *mut f64) };
    let count = Arc::strong_count(&arc);
    mem::forget(arc);
    count as u16
}

#[derive(Default, Copy, Clone)]
#[repr(C)]
struct UserString {
    user: *const c_char,
    length: usize,
}

/// Mock implementation of `HiddenString_GetUnsafe` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
pub unsafe extern "C" fn HiddenString_GetUnsafe(
    value: *const ffi::HiddenString,
    length: *mut usize,
) -> *const c_char {
    let text = unsafe { value.cast::<UserString>().as_ref().unwrap() };
    if text.length != 0 {
        unsafe {
            *length = text.length;
        }
    }

    text.user
}

/// Returns the following:
/// - 0, if the strings are the same
/// - a negative value if left is less than right
/// - a positive value if left is greater than right
#[unsafe(no_mangle)]
pub extern "C" fn HiddenString_CompareC(
    left: Option<NonNull<ffi::HiddenString>>,
    right: *const c_char,
    right_length: usize,
) -> c_int {
    let Some(left_ptr) = left else {
        // Treat None as empty; compare to right's length.
        return if right_length == 0 { 0 } else { -1 };
    };

    let left = unsafe { left_ptr.cast::<UserString>().as_ref() };

    let left_ptr = if left.user.is_null() {
        std::ptr::null()
    } else {
        left.user
    };
    let left_length = left.length;

    let right_ptr = if right.is_null() {
        std::ptr::null()
    } else {
        right
    };

    // Handle empty cases early to avoid UB in strncmp.
    if left_length == 0 && right_length == 0 {
        return 0;
    } else if left_length == 0 {
        return -1;
    } else if right_length == 0 {
        return 1;
    }

    // Safe to call strncmp now (pointers non-null).
    let min_len = cmp::min(left_length, right_length);
    let result = unsafe { libc::strncmp(left_ptr, right_ptr, min_len) };

    if result != 0 {
        result
    } else {
        // Length difference (saturate to i32 to avoid panic).
        let left_len_i32 = c_int::try_from(left_length).unwrap_or(c_int::MAX);
        let right_len_i32 = c_int::try_from(right_length).unwrap_or(c_int::MAX);
        left_len_i32 - right_len_i32
    }
}

/// Mock implementation of `IndexSpecCache_Decref` from spec.h for testing purposes
#[unsafe(no_mangle)]
pub extern "C" fn IndexSpecCache_Decref(s: Option<NonNull<ffi::IndexSpecCache>>) {
    let s = s.unwrap();
    let refcount = unsafe {
        s.byte_add(offset_of!(ffi::IndexSpecCache, refcount))
            .cast::<usize>()
    };

    let refcount = unsafe { AtomicUsize::from_ptr(refcount.as_ptr()) };

    if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
        drop(unsafe { Box::from_raw(s.as_ptr()) });
    }
}
