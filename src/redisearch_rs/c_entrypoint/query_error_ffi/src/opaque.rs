/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query_error::QueryError;
use std::mem::MaybeUninit;

/// A type with size `N`.
#[repr(transparent)]
pub struct Size<const N: usize>(MaybeUninit<[u8; N]>);

/// An opaque query error which can be passed by value to C.
///
/// The size and alignment of this struct must match the rust structure exactly.
#[repr(C, align(8))]
pub struct OpaqueQueryError(Size<38>);

// This is used to output a clear error message when $ty and $inner
// have different sizes.
//
// Using `assert!(a == b)` is less clear since the values of `a`
// and `b` are not printed. We cannot use `assert_eq` in a const
// context. We also cannot actually run the transmute as that would
// require that $ty and $inner have default constant values, so we
// provide a never type (`break`) as the argument.
//
// If $ty and $inner have different sizes, an error like this:
//    |
// 15 | mimic::impl_opaque!(Person as OpaquePerson);
//    | ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//    |
//    = note: source type: `Person` (320 bits)
//    = note: target type: `OpaquePerson` (256 bits)
#[allow(unreachable_code)]
const _: () = {
    loop {
        unsafe { std::mem::transmute::<OpaqueQueryError, QueryError>(break) };
    }

    assert!(std::mem::align_of::<OpaqueQueryError>() == std::mem::align_of::<QueryError>());
};

pub trait QueryErrorExt {
    fn into_opaque(self) -> OpaqueQueryError;
    fn into_opaque_ptr(&self) -> *const OpaqueQueryError;
    fn into_opaque_mut_ptr(&mut self) -> *mut OpaqueQueryError;
    unsafe fn from_opaque(opaque: OpaqueQueryError) -> Self;
    unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueQueryError) -> Option<&'a Self>;
    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueQueryError) -> Option<&'a mut Self>;
}

impl QueryErrorExt for QueryError {
    fn into_opaque(self) -> OpaqueQueryError {
        unsafe { std::mem::transmute(self) }
    }

    fn into_opaque_ptr(&self) -> *const OpaqueQueryError {
        std::ptr::from_ref(&self).cast()
    }

    fn into_opaque_mut_ptr(&mut self) -> *mut OpaqueQueryError {
        std::ptr::from_mut(self).cast()
    }

    unsafe fn from_opaque(opaque: OpaqueQueryError) -> Self {
        unsafe { std::mem::transmute(opaque) }
    }

    unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueQueryError) -> Option<&'a Self> {
        unsafe { opaque.cast::<Self>().as_ref() }
    }

    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueQueryError) -> Option<&'a mut Self> {
        unsafe { opaque.cast::<Self>().as_mut() }
    }
}
