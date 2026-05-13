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
//! - [`Active<'a>`] — the pointer is known to be a valid `&'a T`.
//! - [`Suspended`] — the pointer was weakened from `&` to a raw pointer
//!   and may no longer be valid, because other parts of the program may
//!   have made changes in the meantime (e.g. dropped or moved the
//!   pointee).
//!
//! [`SharedPtr<Rf, T>`] wraps a [`NonNull<T>`] whose validity semantics
//! depend on the `Rf` mode. In `Active<'a>` mode it is known to be a
//! valid `&'a T`; in `Suspended` mode it may be stale. The name reflects
//! that `SharedPtr` only ever exposes shared (immutable) access — it is
//! never a mutable pointer.
//!
//! Construction is split by mode: [`SharedPtr::<Active>::from_ref`] takes
//! a `&'a T` and [`SharedPtr::<Suspended>::from_non_null`] takes a
//! [`NonNull<T>`]; both are safe. To go from a suspended pointer back to
//! an active one, use the unsafe [`SharedPtr::<Suspended>::into_active`],
//! whose safety contract is that the pointer is valid for reads and
//! non-aliased for the chosen lifetime.
//!
//! Wrapping `NonNull<T>` (rather than `*const T`) lets
//! `Option<SharedPtr<Rf, T>>` take advantage of the null-pointer niche
//! optimization, so optional `SharedPtr` fields stay pointer-sized and
//! remain C-ABI compatible with `nullable T *`.
//!
//! Since `SharedPtr` is `#[repr(transparent)]` over `NonNull<T>`, and
//! containing structs use `#[repr(C)]`, the `Active` and `Suspended`
//! instantiations have identical memory layout and can be transmuted
//! between.

use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;

mod sealed {
    pub trait Sealed {}
}

/// Marker trait for reference modes.
///
/// This trait is sealed — only [`Active`] and [`Suspended`] implement it.
/// The `fmt::Debug` supertrait lets containing types derive `Debug` without
/// requiring an explicit `where R: Debug` bound at every use site.
pub trait Ref: sealed::Sealed + fmt::Debug {
    /// Format a [`SharedPtr`] of this mode for `Debug`.
    ///
    /// In [`Active`] mode the pointer is dereferenced and the pointee's
    /// `Debug` output is shown. In [`Suspended`] mode the pointer may be
    /// stale, so only the raw address is printed.
    fn fmt_ptr<T: ?Sized + fmt::Debug>(
        ptr: &SharedPtr<Self, T>,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result
    where
        Self: Sized;
}

/// Active mode — pointers are guaranteed to point to a live, valid `T`.
///
/// [`SharedPtr<Active<'a>, T>`] wraps a [`NonNull<T>`] that is guaranteed
/// to be a valid `&'a T` reference, providing safe access via
/// [`SharedPtr::get`].
///
/// Implements `PartialEq`/`Eq` so types parameterised by `R: Ref` can
/// `#[derive(PartialEq)]` — the auto-added `R: PartialEq` bound restricts
/// the derived impl to `Active` (since [`Suspended`] does not implement
/// `PartialEq`).
#[derive(Debug, PartialEq, Eq)]
pub struct Active<'a>(PhantomData<&'a ()>);

impl sealed::Sealed for Active<'_> {}
impl Ref for Active<'_> {
    fn fmt_ptr<T: ?Sized + fmt::Debug>(
        ptr: &SharedPtr<Self, T>,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        ptr.get().fmt(f)
    }
}

/// Suspended mode — pointers may no longer be valid.
///
/// [`SharedPtr<Suspended, T>`] wraps a [`NonNull<T>`] whose target may no
/// longer be valid: the `&` it was originally derived from has been
/// weakened to a raw pointer, and other parts of the program may have
/// made changes since (e.g. dropping or moving the pointee). The pointer
/// is therefore allowed to dangle, and no safe access methods are
/// available.
#[derive(Debug)]
pub struct Suspended;

impl sealed::Sealed for Suspended {}
impl Ref for Suspended {
    fn fmt_ptr<T: ?Sized + fmt::Debug>(
        ptr: &SharedPtr<Self, T>,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        fmt::Pointer::fmt(&ptr.raw.as_ptr(), f)
    }
}

/// A pointer to `T` whose validity semantics depend on the [`Ref`] mode `Rf`.
///
/// Internally this is a [`NonNull<T>`]. The `Rf` type parameter controls
/// which constructors and methods are available:
///
/// - [`Active<'a>`]: built from a reference via
///   [`SharedPtr::<Active>::from_ref`], with safe access via
///   [`SharedPtr::get`].
/// - [`Suspended`]: built from a raw [`NonNull<T>`] via
///   [`SharedPtr::<Suspended>::from_non_null`]; inert (no safe access).
///   Upgrade to [`Active`] with the unsafe
///   [`SharedPtr::<Suspended>::into_active`] once validity is
///   re-established.
///
/// `SharedPtr` only ever exposes shared (immutable) access to its
/// referent; there is no mutable counterpart.
///
/// `#[repr(transparent)]` ensures `SharedPtr<Rf, T>` has the same layout
/// as `NonNull<T>` regardless of `Rf`, enabling zero-cost transmutation
/// between `Active` and `Suspended` instantiations of containing
/// `#[repr(C)]` structs. The `NonNull<T>` niche makes
/// `Option<SharedPtr<Rf, T>>` pointer-sized as well.
#[repr(transparent)]
pub struct SharedPtr<Rf: Ref, T: ?Sized> {
    raw: NonNull<T>,
    _ref: PhantomData<Rf>,
}

impl<Rf: Ref, T: ?Sized> Copy for SharedPtr<Rf, T> {}

impl<Rf: Ref, T: ?Sized> Clone for SharedPtr<Rf, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Rf: Ref, T: ?Sized + fmt::Debug> fmt::Debug for SharedPtr<Rf, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Rf::fmt_ptr(self, f)
    }
}

impl<R: Ref + PartialEq, T: ?Sized + PartialEq> PartialEq for SharedPtr<R, T> {
    fn eq(&self, other: &Self) -> bool {
        // SAFETY: `R: Ref + PartialEq` is satisfied only by `Active`
        // (sealed-trait + the fact that `Suspended` does not implement
        // `PartialEq`). The `Active` invariant guarantees the pointer is
        // a valid reference for the lifetime carried in `R`.
        let lhs = unsafe { self.raw.as_ref() };
        // SAFETY: same as above.
        let rhs = unsafe { other.raw.as_ref() };
        lhs == rhs
    }
}

impl<R: Ref + Eq, T: ?Sized + Eq> Eq for SharedPtr<R, T> {}

impl<'a, T: ?Sized> SharedPtr<Active<'a>, T> {
    /// Build an active pointer from a reference.
    #[inline(always)]
    pub const fn from_ref(r: &'a T) -> Self {
        Self {
            raw: NonNull::from_ref(r),
            _ref: PhantomData,
        }
    }

    /// Access the referenced value.
    #[inline(always)]
    pub const fn get(self) -> &'a T {
        // SAFETY: In `Active<'a>` mode, the pointer is a valid `&'a T`
        // reference. The `Active<'a>` invariant guarantees the referent
        // is alive and unaliased for reads for the duration of `'a`.
        unsafe { self.raw.as_ref() }
    }
}

impl<T: ?Sized> SharedPtr<Suspended, T> {
    /// Build a suspended pointer from a [`NonNull<T>`].
    ///
    /// This is safe: suspended pointers carry no validity guarantee, so
    /// the non-null invariant of [`NonNull`] itself is the only thing
    /// required. Use [`Self::into_active`] to upgrade back to an active
    /// pointer once validity can be re-established.
    #[inline(always)]
    pub const fn from_non_null(raw: NonNull<T>) -> Self {
        Self {
            raw,
            _ref: PhantomData,
        }
    }

    /// Re-attach a lifetime to a suspended pointer and treat it as active.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that, for the chosen lifetime `'a`, the
    /// pointer is:
    ///
    /// 1. valid for reads of `T`, and
    /// 2. not aliased by any mutable reference.
    #[inline(always)]
    pub const unsafe fn into_active<'a>(self) -> SharedPtr<Active<'a>, T> {
        SharedPtr {
            raw: self.raw,
            _ref: PhantomData,
        }
    }
}

impl<Rf: Ref, T: ?Sized> SharedPtr<Rf, T> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_round_trip() {
        let value = 42u32;
        let ptr: SharedPtr<Active<'_>, u32> = SharedPtr::from_ref(&value);
        assert_eq!(*ptr.get(), 42);
        assert_eq!(ptr.as_raw(), &value as *const u32);
    }

    #[test]
    fn option_ptr_is_pointer_sized() {
        // The `NonNull<T>` niche makes `Option<SharedPtr<_, _>>`
        // pointer-sized and ABI-compatible with `nullable T *`.
        use std::mem::size_of;
        assert_eq!(
            size_of::<Option<SharedPtr<Active<'_>, u32>>>(),
            size_of::<*const u32>()
        );
        assert_eq!(
            size_of::<Option<SharedPtr<Suspended, u32>>>(),
            size_of::<*const u32>()
        );
    }

    #[test]
    fn active_and_suspended_are_layout_compatible() {
        // `SharedPtr` is `#[repr(transparent)]` over `NonNull<T>`, so the
        // `Active` and `Suspended` instantiations have identical layout.
        // `transmute` would fail to compile if they didn't, and the
        // resulting pointer must round-trip to the same address.
        let value = 42u32;
        let active: SharedPtr<Active<'_>, u32> = SharedPtr::from_ref(&value);
        // SAFETY: both instantiations are `#[repr(transparent)]` over
        // `NonNull<u32>`; weakening `Active` to `Suspended` only drops
        // the validity guarantee, which is sound to do.
        let suspended: SharedPtr<Suspended, u32> = unsafe { std::mem::transmute(active) };
        assert_eq!(suspended.as_raw(), &value as *const u32);
    }
}
