/// An extension trait for convenience methods attached to `Opaque` for
/// using it in an FFI context as an opaque sized type.
pub trait OpaqueExt {
    type OpaqueType;

    /// Converts a `Opaque` into an [`Self::OpaqueType`].
    fn into_opaque(self) -> Self::OpaqueType;

    /// Converts a [`Opaque`] reference into an `*const Self::OpaqueType`.
    fn as_opaque_ptr(&self) -> *const Self::OpaqueType;

    /// Converts a [`Opaque`] mutable reference into an
    /// `*mut Self::OpaqueType`.
    fn as_opaque_mut_ptr(&mut self) -> *mut Self::OpaqueType;

    /// Converts an [`Self::OpaqueType`] back to an [`Opaque`].
    ///
    /// # Safety
    ///
    /// This value must have been created via [`OpaqueExt::into_opaque`].
    unsafe fn from_opaque(opaque: Self::OpaqueType) -> Self;

    /// Converts a const pointer to a [`Self::OpaqueType`] to a reference to a
    /// [`Opaque`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`OpaqueExt::as_opaque_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`Opaque`].
    unsafe fn from_opaque_ptr<'a>(opaque: *const Self::OpaqueType) -> Option<&'a Self>;

    /// Converts a mutable pointer to a [`Self::OpaqueType`] to a mutable
    /// reference to a [`Opaque`].
    ///
    /// # Safety
    ///
    /// The pointer itself must have been created via
    /// [`OpaqueExt::as_opaque_mut_ptr`], as the alignment of the value
    /// pointed to by `opaque` must also be an alignment-compatible address for
    /// a [`Opaque`].
    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut Self::OpaqueType) -> Option<&'a mut Self>;
}

#[macro_export]
macro_rules! impl_opaque_ext {
    ($concrete_type:ty, $opaque_name:ident, $alignment:expr, $size_const:expr, $size_const_debug:expr) => {
        #[cfg(not(debug_assertions))]
        #[repr(C, align($alignment))]
        pub struct $opaque_name(std::mem::MaybeUninit<[u8; $size_const]>);

        #[cfg(debug_assertions)]
        #[repr(C, align($alignment))]
        pub struct $opaque_name(std::mem::MaybeUninit<[u8; $size_const_debug]>);

        // Ensure size and alignment compatibility
        const _: () = {
            #[allow(unreachable_code, clippy::never_loop)]
            loop {
                // Safety: this code never runs
                unsafe { std::mem::transmute::<$opaque_name, $concrete_type>(break) };
            }

            assert!(std::mem::align_of::<$opaque_name>() == std::mem::align_of::<$concrete_type>());
        };

        impl OpaqueExt for $concrete_type {
            type OpaqueType = $opaque_name;

            fn into_opaque(self) -> Self::OpaqueType {
                // Safety: Same size and alignment guaranteed by compile-time checks above
                unsafe { std::mem::transmute(self) }
            }

            fn as_opaque_ptr(&self) -> *const Self::OpaqueType {
                std::ptr::from_ref(self).cast()
            }

            fn as_opaque_mut_ptr(&mut self) -> *mut Self::OpaqueType {
                std::ptr::from_mut(self).cast()
            }

            unsafe fn from_opaque(opaque: Self::OpaqueType) -> Self {
                // Safety: see trait's safety requirement
                unsafe { std::mem::transmute(opaque) }
            }

            unsafe fn from_opaque_ptr<'a>(opaque: *const Self::OpaqueType) -> Option<&'a Self> {
                // Safety: see trait's safety requirement
                unsafe { opaque.cast::<Self>().as_ref() }
            }

            unsafe fn from_opaque_mut_ptr<'a>(
                opaque: *mut Self::OpaqueType,
            ) -> Option<&'a mut Self> {
                // Safety: see trait's safety requirement
                unsafe { opaque.cast::<Self>().as_mut() }
            }
        }
    };
}
