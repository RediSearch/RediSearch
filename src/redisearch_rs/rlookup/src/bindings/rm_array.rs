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

        unsafe {
            NonNull::drop_in_place(self.0);
        }

        let free = unsafe { ffi::RedisModule_Free.unwrap() };

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
        let alloc = unsafe { ffi::RedisModule_Alloc.unwrap() };

        unsafe extern "C" {
            fn rm_alloc_impl(size: usize) -> *mut c_void;
        }

        debug_assert_eq!(alloc as usize, rm_alloc_impl as usize);

        let ptr = NonNull::new(unsafe { alloc(size_of::<T>() * src.len()) })
            .expect("RedisModule_Alloc returned NULL")
            .cast::<T>();

        let mut ptr = NonNull::slice_from_raw_parts(ptr, src.len());

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
        unsafe { self.0.as_ref() }
    }
}

impl<T> DerefMut for RmArray<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}
