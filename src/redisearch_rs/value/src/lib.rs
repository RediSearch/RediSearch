/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
//! Rust port of the RSValue type.

#[cfg(feature = "test_utils")]
mod test_utils;
#[cfg(feature = "test_utils")]
pub use test_utils::RSValueMock;

use std::{ffi::c_char, ptr::NonNull};

/// A trait that defines the behavior of a RediSearch RSValue.
///
/// The trait is temporary until we have a proper Rust port of the RSValue type.
///
/// This trait is used to create, manipulate, and free RSValue instances. It can be implemented
/// as a mock type for testing purposes, and by a new-type in the ffi layer to interact with the
/// C API.
pub trait RSValueTrait: Clone
where
    Self: Sized,
{
    /// Creates a new RSValue instance which is null
    fn create_null() -> Self;

    /// Creates a new RSValue instance with a string value.
    fn create_string(s: String) -> Self;

    /// Creates a new RSValue instance with a number (double) value.
    fn create_num(num: f64) -> Self;

    /// Creates a new RSValue instance that is a reference to the given value.
    fn create_ref(value: Self) -> Self;

    /// Checks if the RSValue is null
    fn is_null(&self) -> bool;

    /// gets a reference to the RSValue instance, if it is a reference type or None.
    fn get_ref(&self) -> Option<&Self>;

    /// gets the string slice of the RSValue instance, if it is a string type or None otherwise.
    fn as_str(&self) -> Option<&str>;

    /// gets the number (double) value of the RSValue instance, if it is a number type or None otherwise.
    fn as_num(&self) -> Option<f64>;

    /// gets the type of the RSValue instance, it either null, string, number, or reference.
    fn get_type(&self) -> ffi::RSValueType;

    /// returns true if the RSValue is stored as a pointer on the heap (the C implementation)
    fn is_ptr_type() -> bool;

    /// returns the approximate memory size of the RSValue instance.
    fn mem_size() -> usize {
        std::mem::size_of::<Self>()
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

impl RSValueFFI {
    /// Constructs an `RSValueFFI` from a raw pointer.
    ///
    /// # Safety
    ///
    /// 1. The `ptr` must be a [valid] pointer to a [`ffi::RSValue`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(ptr: NonNull<ffi::RSValue>) -> Self {
        Self(ptr)
    }

    pub fn as_ptr(&self) -> *mut ffi::RSValue {
        self.0.as_ptr()
    }
}

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
        unsafe { ffi::RSValue_Decref(self.0.as_ptr()) };
    }
}

impl RSValueTrait for RSValueFFI {
    fn create_null() -> Self {
        // Safety: RS_NullVal returns an immutable global ptr
        let val = unsafe { ffi::RS_NullVal() };
        RSValueFFI(NonNull::new(val).expect("RS_NullVal returned a null pointer"))
    }

    fn create_string(_: String) -> Self {
        // This method gets a Rust String which is not directly compatible with the C side
        // and we don't want to implement the conversion here.
        panic!("create_string is not implemented for RSValueFFI");
        // The C side, i.e. the function `RSSortingVector_PutStr`
        // uses `RSSortingVector::try_insert_val` to insert a pre-created value.
    }

    fn create_num(num: f64) -> Self {
        // Safety: RSValue_FromDouble expects a valid double value.
        let num = unsafe { ffi::RS_NumVal(num) };
        RSValueFFI(NonNull::new(num).expect("RS_NumVal returned a null pointer"))
    }

    fn create_ref(value: Self) -> Self {
        RSValueFFI(value.0)
    }

    fn is_null(&self) -> bool {
        // Safety: RS_NullVal returns an immutable global ptr
        self.0.as_ptr() == unsafe { ffi::RS_NullVal() }
    }

    fn get_ref(&self) -> Option<&Self> {
        // Safety: We assume a valid ptr is given by the C side
        let p = unsafe { self.0.as_ref() };
        if p.t() == ffi::RSValueType_RSValue_Reference {
            // Safety: We tested that the type is a reference, so we access it over the union safely.
            let ref_ptr = unsafe { p.__bindgen_anon_1.ref_ };

            // Safety: We assume that a valid pointer is given by the C side
            Some(unsafe { &*(ref_ptr as *const RSValueFFI) })
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        // Safety: We assume a valid ptr is given by the C side
        let p = unsafe { self.0.as_ref() };
        if p.t() == ffi::RSValueType_RSValue_String {
            // Safety: We tested that the type is a string, so we access it over the union safely.
            let c_str: *mut c_char = unsafe { p.__bindgen_anon_1.strval }.str_;

            // Safety: We assume that a valid C string is generated by the C side (valid and null-terminated).
            Some(unsafe { std::ffi::CStr::from_ptr(c_str).to_str().unwrap() })
        } else {
            None
        }
    }

    fn as_num(&self) -> Option<f64> {
        // Safety: We assume a valid ptr is given by the C side
        let p = unsafe { self.0.as_ref() };
        if p.t() == ffi::RSValueType_RSValue_Number {
            // Safety: We tested that the type is a number, so we access it over the union safely.
            Some(unsafe { p.__bindgen_anon_1.numval })
        } else {
            None
        }
    }

    fn get_type(&self) -> ffi::RSValueType {
        // Safety: We assume a valid ptr is given by the C side
        let p = unsafe { self.0.as_ref() };
        p.t()
    }

    fn is_ptr_type() -> bool {
        // Returns true if the RSValue is stored as a pointer on the heap (the C implementation).
        true
    }

    fn mem_size() -> usize {
        // The size of the RSValue struct in C is fixed, so we can use the size of the FFI struct.
        std::mem::size_of::<ffi::RSValue>()
    }
}
