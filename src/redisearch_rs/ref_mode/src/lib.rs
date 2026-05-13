/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Abstractions for switching between reference and raw-pointer modes.
//!
//! The [`Ref`] trait has two implementations:
//!
//! - [`Active<'a>`] — uses references valid for `'a`, while the index read
//!   lock is held.
//! - [`Suspended`] — uses raw pointers, while the lock is released.
//!
//! [`Ptr<Rf, T>`] wraps a [`NonNull<T>`] whose validity semantics depend on
//! the `Rf` mode. In `Active<'a>` mode it is known to be a valid `&'a T`;
//! in `Suspended` mode it may be stale (the lock has been released).
//!
//! Wrapping `NonNull<T>` (rather than `*const T`) lets `Option<Ptr<Rf, T>>`
//! take advantage of the null-pointer niche optimization, so optional
//! `Ptr` fields stay pointer-sized and remain C-ABI compatible with
//! `nullable T *`.
//!
//! Since `Ptr` is `#[repr(transparent)]` over `NonNull<T>`, and containing
//! structs use `#[repr(C)]`, the `Active` and `Suspended` instantiations
//! have identical memory layout and can be transmuted between.

use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for reference modes.
///
/// This trait is sealed — only [`Active`] and [`Suspended`] implement it.
pub trait Ref: sealed::Sealed {}

/// Active mode — the index read lock is held.
///
/// [`Ptr<Active<'a>, T>`] wraps a [`NonNull<T>`] that is guaranteed to be
/// a valid `&'a T` reference, providing safe access via [`Ptr::get`].
pub struct Active<'a>(PhantomData<&'a ()>);

impl sealed::Sealed for Active<'_> {}
impl Ref for Active<'_> {}

/// Suspended mode — the index read lock has been released.
///
/// [`Ptr<Suspended, T>`] wraps a [`NonNull<T>`] whose target may have been
/// freed. The pointer is allowed to dangle, so it is safe to hold across
/// lock release/reacquire cycles. No safe access methods are available.
pub struct Suspended;

impl sealed::Sealed for Suspended {}
impl Ref for Suspended {}

/// A pointer to `T` whose validity semantics depend on the [`Ref`] mode `Rf`.
///
/// Internally this is a [`NonNull<T>`]. The `Rf` type parameter controls
/// which methods are available:
///
/// - [`Active<'a>`]: constructed from `&'a T`, safe access via [`Ptr::get`].
/// - [`Suspended`]: inert — no safe access, pointer may be stale.
///
/// `#[repr(transparent)]` ensures `Ptr<Rf, T>` has the same layout as
/// `NonNull<T>` regardless of `Rf`, enabling zero-cost transmutation
/// between `Active` and `Suspended` instantiations of containing
/// `#[repr(C)]` structs. The `NonNull<T>` niche makes
/// `Option<Ptr<Rf, T>>` pointer-sized as well.
#[repr(transparent)]
pub struct Ptr<Rf: Ref, T: ?Sized> {
    raw: NonNull<T>,
    _ref: PhantomData<Rf>,
}

impl<Rf: Ref, T: ?Sized> Copy for Ptr<Rf, T> {}

impl<Rf: Ref, T: ?Sized> Clone for Ptr<Rf, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Rf: Ref, T: ?Sized> fmt::Debug for Ptr<Rf, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Pointer::fmt(&self.raw.as_ptr(), f)
    }
}

impl<'a, T: ?Sized> Ptr<Active<'a>, T> {
    /// Create a new active pointer from a reference.
    #[inline(always)]
    pub const fn new(r: &'a T) -> Self {
        Self {
            raw: NonNull::from_ref(r),
            _ref: PhantomData,
        }
    }

    /// Access the referenced value.
    ///
    /// This is safe because `Ptr<Active<'a>, T>` is only constructed from
    /// valid `&'a T` references, and the `Active<'a>` invariant (read
    /// lock held) guarantees the referent is alive.
    #[inline(always)]
    pub const fn get(self) -> &'a T {
        // SAFETY: In `Active<'a>` mode, the pointer was created from a
        // valid `&'a T` reference. The `Active<'a>` invariant (read lock
        // held) guarantees the referent is alive and unaliased for reads
        // for the duration of `'a`.
        unsafe { self.raw.as_ref() }
    }
}

impl<Rf: Ref, T: ?Sized> Ptr<Rf, T> {
    /// Get the underlying raw pointer.
    #[inline(always)]
    pub const fn as_raw(self) -> *const T {
        self.raw.as_ptr()
    }

    /// Get the underlying [`NonNull<T>`].
    #[inline(always)]
    pub const fn as_non_null(self) -> NonNull<T> {
        self.raw
    }

    /// Create a `Ptr` from a raw [`NonNull<T>`].
    ///
    /// # Safety
    ///
    /// When `Rf = Active<'a>`, the caller must ensure the pointer is a
    /// valid `&'a T` reference. When `Rf = Suspended`, the only
    /// requirement is the non-null invariant of [`NonNull`] itself.
    #[inline(always)]
    pub const unsafe fn from_non_null(raw: NonNull<T>) -> Self {
        Self {
            raw,
            _ref: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_round_trip() {
        let value = 42u32;
        let ptr: Ptr<Active<'_>, u32> = Ptr::new(&value);
        assert_eq!(*ptr.get(), 42);
        assert_eq!(ptr.as_raw(), &value as *const u32);
    }

    #[test]
    fn option_ptr_is_pointer_sized() {
        // The `NonNull<T>` niche makes `Option<Ptr<_, _>>` pointer-sized
        // and ABI-compatible with `nullable T *`.
        use std::mem::size_of;
        assert_eq!(
            size_of::<Option<Ptr<Active<'_>, u32>>>(),
            size_of::<*const u32>()
        );
        assert_eq!(
            size_of::<Option<Ptr<Suspended, u32>>>(),
            size_of::<*const u32>()
        );
    }
}
