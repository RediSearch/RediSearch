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
/// The size and alignment of this struct must match the Rust `QueryError`
/// structure exactly.
#[repr(C, align(8))]
pub struct OpaqueQueryError(Size<38>);

// If `QueryError` and `OpaqueQueryError` ever differ in size, this code will
// cause a clear error message like:
//
//    = note: source type: `QueryError` (320 bits)
//    = note: target type: `OpaqueQueryError` (256 bits)
//
// Using `assert!(a == b)` is less clear since the values of `a` and `b`
// are not printed. We cannot use `assert_eq` in a const context. We also
// cannot actually run the transmute as that would require that `QueryError` and
// `OpaqueQueryError` have default constant values, so we provide a never type
// (`break`) as the argument.
//
// For alignment, printing a clear error message is more difficult as there
// isn't a magic function like `transmute` that will show the alignments.
const _: () = {
    #[allow(unreachable_code, clippy::never_loop)]
    loop {
        // Safety: this code never runs
        unsafe { std::mem::transmute::<OpaqueQueryError, QueryError>(break) };
    }

    assert!(std::mem::align_of::<OpaqueQueryError>() == std::mem::align_of::<QueryError>());
};

/// An extension trait for convenience methods attached to `QueryError` for
/// using it in an FFI context as an opaque sized type.
pub trait QueryErrorExt {
    /// Converts a `QueryError` into an [`OpaqueQueryError`].
    fn into_opaque(self) -> OpaqueQueryError;

    /// Converts a [`QueryError`] reference into an `*const OpaqueQueryError`.
    fn as_opaque_ptr(&self) -> *const OpaqueQueryError;

    /// Converts a [`QueryError`] mutable reference into an
    /// `*mut OpaqueQueryError`.
    fn as_opaque_mut_ptr(&mut self) -> *mut OpaqueQueryError;

    /// Converts an [`OpaqueQueryError`] back to an [`QueryError`].
    ///
    /// # Safety
    ///
    /// This value must have been created via [`QueryErrorExt::into_opaque`].
    unsafe fn from_opaque(opaque: OpaqueQueryError) -> Self;

    /// Converts a const pointer to a [`OpaqueQueryError`] to a reference to a
    /// [`QueryError`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`QueryErrorExt::as_opaque_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`QueryError`].
    unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueQueryError) -> Option<&'a Self>;

    /// Converts a mutable pointer to a [`OpaqueQueryError`] to a mutable
    /// reference to a [`QueryError`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`QueryErrorExt::as_opaque_mut_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`QueryError`].
    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueQueryError) -> Option<&'a mut Self>;
}

impl QueryErrorExt for QueryError {
    fn into_opaque(self) -> OpaqueQueryError {
        // Safety: `OpaqueQueryError` is defined as a `MaybeUninit` slice of
        // bytes with the same size and alignment as `QueryError`, so any valid
        // `QueryError` has a bit pattern which is a valid `OpaqueQueryError`.
        unsafe { std::mem::transmute(self) }
    }

    fn as_opaque_ptr(&self) -> *const OpaqueQueryError {
        std::ptr::from_ref(&self).cast()
    }

    fn as_opaque_mut_ptr(&mut self) -> *mut OpaqueQueryError {
        std::ptr::from_mut(self).cast()
    }

    unsafe fn from_opaque(opaque: OpaqueQueryError) -> Self {
        // Safety: see trait's safety requirement.
        unsafe { std::mem::transmute(opaque) }
    }

    unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueQueryError) -> Option<&'a Self> {
        // Safety: see trait's safety requirement.
        unsafe { opaque.cast::<Self>().as_ref() }
    }

    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueQueryError) -> Option<&'a mut Self> {
        // Safety: see trait's safety requirement.
        unsafe { opaque.cast::<Self>().as_mut() }
    }
}
