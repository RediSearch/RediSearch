/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cmp;
use std::mem::offset_of;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use libc::c_char;
use libc::c_int;
use rlookup::RLookup;
use rlookup::RLookupKeyFlags;
use rlookup::RLookupRow;
use rlookup_ffi::row::RLookupRow_MoveFieldsFrom;
use value::RSValueFFI;
use value::RSValueTrait;

#[test]
fn rlookuprow_move() {
    let mut lookup = RLookup::new();

    let mut src = RLookupRow::new(&lookup);
    let mut dst = RLookupRow::new(&lookup);

    let key = lookup
        .get_key_write(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    src.write_key(key, RSValueFFI::create_num(42.0));

    src.assert_valid("tests::row::rlookuprow_move");
    dst.assert_valid("tests::row::rlookuprow_move");

    unsafe {
        RLookupRow_MoveFieldsFrom(
            ptr::from_ref(&lookup),
            Some(NonNull::from(&mut src)),
            Some(NonNull::from(&mut dst)),
        )
    }

    assert!(src.num_dyn_values() == 0);
    let key = lookup
        .get_key_read(c"foo", RLookupKeyFlags::empty())
        .unwrap();
    assert!(dst.get(&key).is_some());
}

/// Mock implementation of `RSValue_IncrRef` for testing purposes
#[unsafe(no_mangle)]
extern "C" fn RSValue_IncrRef(v: Option<NonNull<ffi::RSValue>>) -> *mut ffi::RSValue {
    const MAX_REFCOUNT: u16 = (i16::MAX) as u16;

    let v = v.unwrap();
    let refcount_ptr = unsafe {
        v.byte_add(offset_of!(ffi::RSValue, _refcount))
            .cast::<u16>()
    };
    let refcount = unsafe { AtomicU16::from_ptr(refcount_ptr.as_ptr()) };
    let old_size = refcount.fetch_add(1, Ordering::Relaxed);
    if old_size > MAX_REFCOUNT {
        std::process::abort();
    }
    v.as_ptr()
}

/// Mock implementation of `RSValue_IncrRef` for testing purposes
#[unsafe(no_mangle)]
extern "C" fn RSValue_DecrRef(v: Option<NonNull<ffi::RSValue>>) {
    let v = v.unwrap();
    let refcount_ptr = unsafe {
        v.byte_add(offset_of!(ffi::RSValue, _refcount))
            .cast::<u16>()
    };
    let refcount = unsafe { AtomicU16::from_ptr(refcount_ptr.as_ptr()) };
    if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
        drop(unsafe { Box::from_raw(v.as_ptr()) });
    }
}

/// Mock implementation of `RSValue_IncrRef` for testing purposes
#[unsafe(no_mangle)]
extern "C" fn RSValue_NewNumber(numval: f64) -> *mut ffi::RSValue {
    Box::into_raw(Box::new(ffi::RSValue {
        __bindgen_anon_1: ffi::RSValue__bindgen_ty_1 { _numval: numval },
        _bitfield_align_1: [0u8; 0],
        _bitfield_1: {
            let mut field = ffi::__BindgenBitfieldUnit::new([0; _]);
            field.set_bit(0, true);
            field
        },
        _refcount: 1,
    }))
}

#[derive(Default, Copy, Clone)]
#[repr(C)]
struct UserString {
    user: *const c_char,
    length: usize,
}

/// Mock implementation of `HiddenString_GetUnsafe` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn HiddenString_GetUnsafe(
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
extern "C" fn HiddenString_CompareC(
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
extern "C" fn IndexSpecCache_Decref(s: Option<NonNull<ffi::IndexSpecCache>>) {
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
