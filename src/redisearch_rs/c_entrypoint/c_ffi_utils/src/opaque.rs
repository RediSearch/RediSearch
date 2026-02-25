/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// A type with size `N`.
#[repr(transparent)]
pub struct Size<const N: usize>(std::mem::MaybeUninit<[u8; N]>);

/// Implements conversions from the passed in type to the given opaque type.
///
/// This implementation requires that the opaque and original
/// type have the same size and alignment, and that it's
/// safe to transmute from the original type to the opaque type
/// and back.
///
/// These constraints are checked with compile-time assertions.
///
/// # Example
/// ```
/// mod opaque {
///     use c_ffi_utils::opaque::Size;
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
///     c_ffi_utils::opaque!(Thing, OpaqueThing);
/// }
/// ```
#[macro_export]
macro_rules! opaque {
    ($ty:ty, $opaque_ty:ident) => {
        impl $ty {
            pub fn into_opaque(self) -> $opaque_ty {
                // Safety:
                // Self::Opaque is validated to implement
                // `Transmute<Self>` in _ASSERT_IMPL_TRANSMUTE
                unsafe { std::mem::transmute(self) }
            }

            pub fn as_opaque_ptr(&self) -> *const $opaque_ty {
                std::ptr::from_ref(self).cast()
            }

            pub fn as_opaque_mut_ptr(&mut self) -> *mut $opaque_ty {
                std::ptr::from_mut(self).cast()
            }

            pub fn as_opaque_non_null(&mut self) -> ::std::ptr::NonNull<$opaque_ty> {
                ::std::ptr::NonNull::from(self).cast()
            }

            pub unsafe fn from_opaque(opaque: $opaque_ty) -> Self {
                // Safety: see trait's safety requirement.
                unsafe { std::mem::transmute(opaque) }
            }

            pub unsafe fn from_opaque_ptr<'__lt>(opaque: *const $opaque_ty) -> Option<&'__lt Self> {
                // Safety: see trait's safety requirement.
                unsafe { opaque.cast::<Self>().as_ref() }
            }

            pub unsafe fn from_opaque_mut_ptr<'__lt>(
                opaque: *mut $opaque_ty,
            ) -> Option<&'__lt mut Self> {
                // Safety: see trait's safety requirement.
                unsafe { opaque.cast::<Self>().as_mut() }
            }

            pub unsafe fn from_opaque_non_null<'__lt>(
                opaque: ::std::ptr::NonNull<$opaque_ty>,
            ) -> &'__lt mut Self {
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
    };
}
