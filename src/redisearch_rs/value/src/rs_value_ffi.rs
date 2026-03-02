/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::marker::PhantomData;
use std::ops::Deref;
use std::{ffi::c_char, mem::ManuallyDrop, ptr::NonNull, slice};

pub struct RSValueFFIRef<'a>(ManuallyDrop<RSValueFFI>, PhantomData<&'a ffi::RSValue>);

impl Deref for RSValueFFIRef<'_> {
    type Target = RSValueFFI;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// [RSValueFFI] is a wrapper around the C struct `RSValue` implement as new-type over a [std::ptr::NonNull<ffi::RSValue>].
///
/// It implements the [`Clone`] and [`Drop`] traits to manage the reference counting of the underlying C struct.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an `RSValue` created by the C side.
#[repr(transparent)]
pub struct RSValueFFI(NonNull<ffi::RSValue>);

// Clone is used to increment the reference count of the underlying C struct.
impl Clone for RSValueFFI {
    fn clone(&self) -> Self {
        // Safety: We assume a valid ptr is given by the C side, and we are incrementing the reference count.
        unsafe { ffi::RSValue_IncrRef(self.0.as_ptr()) };
        RSValueFFI(self.0)
    }
}

// Drop is used to decrement the reference count of the underlying C struct when the RSValueFFI is dropped.
impl Drop for RSValueFFI {
    fn drop(&mut self) {
        // Safety: We assume a valid ptr is given by the C side, and we are decrementing the reference count.
        unsafe { ffi::RSValue_DecrRef(self.0.as_ptr()) };
    }
}

impl RSValueFFI {
    /// Constructs an `RSValueFFI` from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. The `ptr` must be a [valid] pointer to a [`ffi::RSValue`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn from_raw(ptr: NonNull<ffi::RSValue>) -> Self {
        Self(ptr)
    }

    pub const fn as_ptr(&self) -> *mut ffi::RSValue {
        self.0.as_ptr()
    }

    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        this.0 == other.0
    }

    pub fn null_static() -> Self {
        // Safety: RSValue_NullStatic returns an immutable global ptr
        let val = unsafe { ffi::RSValue_NullStatic() };
        RSValueFFI(NonNull::new(val).expect("RSValue_NullStatic returned a null pointer"))
    }

    pub fn new_num(num: f64) -> Self {
        // Safety: RSValue_FromDouble expects a valid double value.
        let num = unsafe { ffi::RSValue_NewNumber(num) };
        RSValueFFI(NonNull::new(num).expect("RSValue_NewNumber returned a null pointer"))
    }

    pub fn new_string(str: Vec<u8>) -> Self {
        // Safety: RSValue_NewString receives a valid C string pointer (1) and length
        let value = unsafe { ffi::RSValue_NewCopiedString(str.as_ptr().cast(), str.len()) };

        Self(NonNull::new(value).expect("RSValue_NewCopiedString returned a null pointer"))
    }

    pub fn get_type(&self) -> ffi::RSValueType {
        // Safety: self.0 is a valid pointer to an RSValue struct which RSValue_Type expects.
        unsafe { ffi::RSValue_Type(self.0.as_ptr()) }
    }

    pub fn is_null(&self) -> bool {
        // Safety: RSValue_NullStatic returns an immutable global ptr
        self.0.as_ptr() == unsafe { ffi::RSValue_NullStatic() }
    }

    pub fn as_num(&self) -> Option<f64> {
        if self.get_type() == ffi::RSValueType_RSValueType_Number {
            // Safety: we checked the RSValue to be a number above.
            let value = unsafe { ffi::RSValue_Number_Get(self.0.as_ptr()) };
            Some(value)
        } else {
            None
        }
    }

    pub fn as_str_bytes(&self) -> Option<&[u8]> {
        if self.get_type() == ffi::RSValueType_RSValueType_String {
            let mut len: u32 = 0;
            // Safety: We tested that the type is a string, so we access it over the union safely.
            let cstr: *mut c_char =
                unsafe { ffi::RSValue_String_Get(self.0.as_ptr(), &mut len as *mut _) };

            // Safety: We assume the returned char pointer and associated len are valid.
            Some(unsafe { slice::from_raw_parts(cstr.cast(), len as usize) })
        } else {
            None
        }
    }

    pub fn deep_deref(&self) -> RSValueFFIRef<'_> {
        // Safety: self.0 is a valid pointer to an RSValue struct.
        let deref_ptr = unsafe { ffi::RSValue_Dereference(self.0.as_ptr()) };
        // Safety: deref_ptr is a valid pointer to an RSValue struct returned by RSValue_Dereference.
        let self_ = unsafe { RSValueFFI::from_raw(NonNull::new(deref_ptr).unwrap()) };
        RSValueFFIRef(ManuallyDrop::new(self_), PhantomData)
    }

    pub fn mem_size() -> usize {
        // Safety: Simply reading out a constant
        unsafe { ffi::RSValueSize }
    }

    pub fn refcount(&self) -> u16 {
        // Safety: self.0 is a valid pointer to an RSValue struct.
        unsafe { ffi::RSValue_Refcount(self.0.as_ptr()) }
    }
}

impl std::fmt::Debug for RSValueFFI {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RSValueFFI::")?;

        match self.get_type() {
            ffi::RSValueType_RSValueType_String => {
                // Safety: We just checked the union type.
                let str_val = self.as_str_bytes().unwrap();
                // Safety: We assume that a valid C string is generated by the C side (valid and null-terminated).
                let str_val = str::from_utf8(str_val).unwrap();
                write!(f, "String({str_val})")
            }
            ffi::RSValueType_RSValueType_Number => {
                // Safety: We just checked the union type.
                let num_val = self.as_num().unwrap();
                write!(f, "Number({num_val})")
            }
            ffi::RSValueType_RSValueType_Null => {
                write!(f, "Null")
            }
            unknown_type => {
                write!(f, "Unknown(<type> {unknown_type})")
            }
        }
    }
}
