/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains mocked implementations of Redis C functions used in
//! [RLookup] for testing purposes. It mocks the ref counting of the cache and
//! the `HiddenString` type used in the C code.

use std::{
    alloc::{Layout, alloc, dealloc},
    cmp,
    ffi::{c_char, c_int},
    mem::{self, offset_of},
    panic,
    ptr::{self, NonNull},
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Default, Copy, Clone)]
#[repr(C)]
pub(crate) struct UserString {
    pub(crate) user: *const c_char,
    pub(crate) length: usize,
}

/// Mock implementation of `NewHiddenString` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn NewHiddenString(
    user: *const c_char,
    length: usize,
    take_ownership: bool,
) -> *mut ffi::HiddenString {
    assert!(
        !take_ownership,
        "tests are not allowed to move ownership to C"
    );
    let value = Box::new(UserString { user, length });
    Box::into_raw(value).cast()
}

/// Mock implementation of `HiddenString_Free` from obfuscation/hidden.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn HiddenString_Free(value: *const ffi::HiddenString, took_ownership: bool) {
    assert!(
        !took_ownership,
        "tests are not allowed to move ownership to C"
    );

    drop(unsafe { Box::from_raw(value.cast_mut().cast::<UserString>()) });
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
