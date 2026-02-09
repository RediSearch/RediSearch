/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;
use std::fmt;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

pub struct RmArray<T>(NonNull<[T]>);

impl<T> Drop for RmArray<T> {
    fn drop(&mut self) {
        if self.is_empty() {
            return;
        }

        // Safety: ptr is known to be non-null and well aligned. we'll also not access
        // the data anymore after this point.
        unsafe {
            NonNull::drop_in_place(self.0);
        }

        // Safety: the redis module is always initialized at this point
        let free = unsafe { ffi::RedisModule_Free.unwrap() };

        // Safety: ptr is known to be non-null and well aligned and correctly allocated by us below.
        unsafe {
            free(self.0.cast::<c_void>().as_ptr());
        }
    }
}

impl<T> RmArray<T> {
    pub fn new<const N: usize>(src: [T; N]) -> Self
    where
        T: Copy,
    {
        // Safety: the redis module is always initialized at this point
        let alloc = unsafe { ffi::RedisModule_Alloc.unwrap() };

        // Safety: the size is non-zero, and doesn't overflow isize or any other common allocator invariants
        let ptr = NonNull::new(unsafe { alloc(size_of::<T>().strict_mul(src.len())) })
            .expect("RedisModule_Alloc returned NULL")
            .cast::<T>();

        let mut ptr = NonNull::slice_from_raw_parts(ptr, src.len());

        // Safety: we just allocated the pointer above
        unsafe {
            ptr.as_mut().copy_from_slice(&src);
        }

        Self(ptr)
    }

    pub fn into_raw(self) -> *mut T {
        let me = ManuallyDrop::new(self);
        me.0.as_ptr().cast::<T>()
    }
}

impl<T: fmt::Debug> fmt::Debug for RmArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RedisModuleArray").field(&&(**self)).finish()
    }
}

impl<T> Deref for RmArray<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        // Safety: we correctly allocated the pointer above and through the type system ensure we can
        // safely access the data
        unsafe { self.0.as_ref() }
    }
}

impl<T> DerefMut for RmArray<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Safety: we correctly allocated the pointer above and through the type system ensure we can
        // safely mutably access the data
        unsafe { self.0.as_mut() }
    }
}
