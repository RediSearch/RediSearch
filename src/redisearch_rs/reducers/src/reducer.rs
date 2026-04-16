/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, mem, ptr, ptr::NonNull};

/// A safe wrapper around an `ffi::Reducer`.
#[repr(transparent)]
pub struct Reducer(ffi::Reducer);

impl Reducer {
    /// Create a new blank `Reducer`.
    pub fn new() -> Self {
        Self(ffi::Reducer {
            srckey: ptr::null(),
            dstkey: ptr::null_mut(),
            alloc: ffi::BlkAlloc {
                root: ptr::null_mut(),
                last: ptr::null_mut(),
                avail: ptr::null_mut(),
            },
            reducerId: 0,
            NewInstance: None,
            Add: None,
            Finalize: None,
            FreeInstance: None,
            Free: None,
        })
    }

    /// Create a `Reducer` wrapper from a non-null pointer.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid non-null pointer to an `ffi::Reducer` that is properly initialized.
    ///    This also applies to any of its subfields.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Reducer) -> &'a Self {
        // SAFETY: ensured by caller (1.)
        unsafe { ptr.cast::<Self>().as_ref().unwrap() }
    }

    /// Set `NewInstance` function pointer.
    pub fn set_new_instance(
        &mut self,
        f: unsafe extern "C" fn(Option<NonNull<ffi::Reducer>>) -> *mut c_void,
    ) -> &mut Self {
        // SAFETY: `Option<NonNull<T>>` is ABI-compatible with `*mut T`.
        self.0.NewInstance = Some(unsafe {
            mem::transmute::<
                unsafe extern "C" fn(Option<NonNull<ffi::Reducer>>) -> *mut c_void,
                unsafe extern "C" fn(*mut ffi::Reducer) -> *mut c_void,
            >(f)
        });
        self
    }

    /// Set `FreeInstance` function pointer.
    pub fn set_free_instance(
        &mut self,
        f: unsafe extern "C" fn(Option<NonNull<ffi::Reducer>>, *mut c_void),
    ) -> &mut Self {
        // SAFETY: `Option<NonNull<T>>` is ABI-compatible with `*mut T`.
        self.0.FreeInstance = Some(unsafe {
            mem::transmute::<
                unsafe extern "C" fn(Option<NonNull<ffi::Reducer>>, *mut c_void),
                unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void),
            >(f)
        });
        self
    }

    /// Set `Add` function pointer.
    pub fn set_add(
        &mut self,
        f: unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void, *const ffi::RLookupRow) -> i32,
    ) -> &mut Self {
        self.0.Add = Some(f);
        self
    }

    /// Set `Finalize` function pointer.
    pub fn set_finalize(
        &mut self,
        f: unsafe extern "C" fn(Option<NonNull<ffi::Reducer>>, *mut c_void) -> *mut ffi::RSValue,
    ) -> &mut Self {
        // SAFETY: `Option<NonNull<T>>` is ABI-compatible with `*mut T`.
        self.0.Finalize = Some(unsafe {
            mem::transmute::<
                unsafe extern "C" fn(
                    Option<NonNull<ffi::Reducer>>,
                    *mut c_void,
                ) -> *mut ffi::RSValue,
                unsafe extern "C" fn(*mut ffi::Reducer, *mut c_void) -> *mut ffi::RSValue,
            >(f)
        });
        self
    }

    /// Set `Free` function pointer.
    pub fn set_free(&mut self, f: unsafe extern "C" fn(*mut ffi::Reducer)) -> &mut Self {
        self.0.Free = Some(f);
        self
    }
}

impl Default for Reducer {
    fn default() -> Self {
        Self::new()
    }
}
