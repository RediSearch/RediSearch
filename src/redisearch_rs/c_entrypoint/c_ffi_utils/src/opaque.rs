/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

/// A type with size `N`.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Size<const N: usize>(std::mem::MaybeUninit<[u8; N]>);

/// Marker trait that signals a type can be safely transmuted from
/// `T`, and that it's safe to cast pointers of the type to `T`
/// and dereference them.
///
/// # Safety
/// - It must be safe to [transmute](std::mem::transmute) from `T`
///   to the implementer of this trait.
/// - It must be safe to cast pointers of the implementer
///   to `T` and dereference them.
pub unsafe trait Transmute<T> {}

/// A trait for using a sized type in an FFI context as an opaque sized type,
/// allowing it to be allocated on the stack on the other side of the
/// FFI-boundary, without the implementer needing to be FFI-safe.
pub trait IntoOpaque: Sized {
    type Opaque: Sized;

    /// Converts `Self` into an [`Self::Opaque`].
    fn into_opaque(self) -> Self::Opaque;

    /// Converts [`Self`] reference into an `*const Self::Opaque`.
    fn as_opaque_ptr(&self) -> *const Self::Opaque;

    /// Converts [`Self`] mutable reference into an
    /// `*mut Self::Opaque`.
    fn as_opaque_mut_ptr(&mut self) -> *mut Self::Opaque;

    /// Converts [`Self`] mutable reference into a
    /// [`NonNull<Self::Opaque>`](NonNull).
    fn as_opaque_non_null(&mut self) -> NonNull<Self::Opaque>;

    /// Converts an [`Self::Opaque`] back to [`Self`].
    ///
    /// # Safety
    ///
    /// This value must have been created via [`IntoOpaque::into_opaque`].
    unsafe fn from_opaque(opaque: Self::Opaque) -> Self;

    /// Converts a const pointer to a [`Self::Opaque`] to a reference to a
    /// [`Self`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`IntoOpaque::as_opaque_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`Self`].
    unsafe fn from_opaque_ptr<'a>(opaque: *const Self::Opaque) -> Option<&'a Self>;

    /// Converts a mutable pointer to a [`Self::Opaque`] to a mutable
    /// reference to a [`Self`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`IntoOpaque::as_opaque_mut_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`Self`].
    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut Self::Opaque) -> Option<&'a mut Self>;

    /// Converts a [`NonNull<Self::Opaque>`](NonNull) to a mutable
    /// reference to a [`Self`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`IntoOpaque::as_opaque_non_null`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`Self`].
    unsafe fn from_opaque_non_null<'a>(opaque: NonNull<Self::Opaque>) -> &'a mut Self;
}

/// Implements [`IntoOpaque`] for the passed type,
/// setting the passed opaque type as [`IntoOpaque::Opaque`].
///
/// This implementation requires that the opaque and original
/// type have the same size and alignment, and that it's
/// safe to transmute from the original type to the opaque type
/// and back.
///
/// These constraints are checked with compile-time assertions,
/// and by validating that the [`Transmute`] trait has been implemented.
///
/// The reason that `IntoOpaque::Opaque` is not bound to types
/// implementing `Transmute<Self>` at the trait level instead, is that
/// this is not a requirement of the trait, but one of the way
/// this macro implements that trait, i.e. by using
/// [`std::mem::transmute`] and pointer casts.
///
/// # Example
/// ```
/// mod opaque {
///     use c_ffi_utils::opaque::{Size, Transmute};
///     use std::sync::Arc;
///
///     // A type that is 8-aligned and is 24 bytes in size.
///     struct Thing {
///         data: [Arc<u8>; 3]
///     }
///
///     /// Opaque projection of [`Thing`], allowing the
///     /// non-FFI-safe [`Thing`] to be passed to C
///     /// and even allow C land to place it on the stack.
///     #[repr(C, align(8))]
///     pub struct OpaqueThing(Size<24>);
///
///     // Safety: `OpaqueThing` is defined as a `MaybeUninit` slice of
///     // bytes with the same size and alignment as `Thing`, so any valid
///     // `Thing` has a bit pattern which is a valid `OpaqueThing`.
///     unsafe impl Transmute<Thing> for OpaqueThing {}
///
///     c_ffi_utils::opaque!(Thing, OpaqueThing);
/// }
/// ```
#[macro_export]
macro_rules! opaque {
    ($ty:ident $(<$ty_lt:lifetime>)?, $opaque_ty:ident) => {
        mod __opaque {
            use super::{$opaque_ty, $ty};

            impl$(<$ty_lt>)? $crate::opaque::IntoOpaque for $ty$(<$ty_lt>)? {
                type Opaque = $opaque_ty;

                fn into_opaque(self) -> Self::Opaque {
                    // Safety:
                    // Self::Opaque is validated to implement
                    // `Transmute<Self>` in _ASSERT_IMPL_TRANSMUTE
                    unsafe { std::mem::transmute(self) }
                }

                fn as_opaque_ptr(&self) -> *const Self::Opaque {
                    std::ptr::from_ref(self).cast()
                }

                fn as_opaque_mut_ptr(&mut self) -> *mut Self::Opaque {
                    std::ptr::from_mut(self).cast()
                }

                fn as_opaque_non_null(&mut self) -> std::ptr::NonNull<Self::Opaque> {
                    std::ptr::NonNull::from_mut(self).cast()
                }

                unsafe fn from_opaque(opaque: Self::Opaque) -> Self {
                    // Safety: see trait's safety requirement.
                    unsafe { std::mem::transmute(opaque) }
                }

                unsafe fn from_opaque_ptr<'_opaque_lt_a>(opaque: *const Self::Opaque) -> Option<&'_opaque_lt_a Self> {
                    // Safety: see trait's safety requirement.
                    unsafe { opaque.cast::<Self>().as_ref() }
                }

                unsafe fn from_opaque_mut_ptr<'_opaque_lt_a>(
                    opaque: *mut Self::Opaque,
                ) -> Option<&'_opaque_lt_a mut Self> {
                    // Safety: see trait's safety requirement.
                    unsafe { opaque.cast::<Self>().as_mut() }
                }

                unsafe fn from_opaque_non_null<'_opaque_lt_a>(
                    opaque: std::ptr::NonNull<Self::Opaque>,
                ) -> &'_opaque_lt_a mut Self {
                    // Safety: see trait's safety requirement.
                    unsafe { opaque.cast::<Self>().as_mut() }
                }
            }

            // Sanity check to ensure size and alignment of opaque
            // type match that of the original.
            //
            // If `$ty` and `$opaque_ty` ever differ in size, this code will
            // cause a clear error message like:
            //
            //    = note: source type: `QueryError` (320 bits)
            //    = note: target type: `OpaqueQueryError` (256 bits)
            //
            // Using `assert!(a == b)` is less clear since the values of `a` and `b`
            // are not printed. We cannot use `assert_eq` in a const context. We also
            // cannot actually run the transmute as that would require that `$ty`
            // has a default constant value, so we provide a never type
            // (`break`) as the argument.
            //
            // For alignment, printing a clear error message is more difficult as there
            // isn't a magic function like `transmute` that will show the alignments.
            const _ASSERT_SIZE_AND_ALIGN: () = {
                #[allow(unreachable_code, clippy::never_loop)]
                loop {
                    // Safety: this code never runs
                    unsafe { std::mem::transmute::<$opaque_ty, $ty>(break) };
                }

                assert!(std::mem::align_of::<$opaque_ty>() == std::mem::align_of::<$ty>());
            };

            // Compile-time check that `$opaque_ty` implements
            // `Transmute<$ty>`.
            const _ASSERT_IMPL_TRANSMUTE: () = {
                const fn assert_impl_transmute_size<$($ty_lt,)? T: $crate::opaque::Transmute<$ty $(<$ty_lt>)?>>() {}
                assert_impl_transmute_size::<$opaque_ty>();
            };
        }
    };
}
