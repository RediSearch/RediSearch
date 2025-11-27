/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{RsValue, RsValueInternal, Value, dynamic::DynRsValueRef, shared::SharedRsValue};

/// Macro that allows you to apply an [`FnOnce`] closure
/// to a dereferenced [`DynRsValuePtr`]. This macro takes
/// care of determining whether the value was exclusive or
/// shared, dereferencing the pointer, and applying the passed
/// closure.
///
/// This is a macro rather than a generic function as
/// the latter would either require passing a macro
/// that takes an `&mut dyn Value` as parameter,
/// meaning that [`Value`](value::Value) would need to be dyn safe, or require
/// the passing two identical closures.
///
/// # Safety
/// - (1) `$ptr` must originate from a call to [`DynRsValuePtr::from_dyn_value`]
/// - (2) `$ptr` must not outlive the `&DynRsValue`
///   passed [`DynRsValuePtr::from_dyn_value`] to create `$ptr`
///
/// # Example
/// ```
/// use value_ffi::{
///     apply_with_dyn_ptr,
///     dynamic::{
///         opaque::OpaqueDynRsValuePtr,
///         DynRsValuePtr,
///     }
/// };
/// use value::Value;
/// use c_ffi_utils::opaque::IntoOpaque;
/// # fn example(v: OpaqueDynRsValuePtr) {
///
/// let v = unsafe { DynRsValuePtr::from_opaque(v) };
/// let n: Option<f64> = unsafe { apply_with_dyn_ptr!(v, |v| v.get_number()) };
///
/// # }
/// ```
#[macro_export]
macro_rules! apply_with_dyn_ptr {
    ($ptr:expr, $f:expr) => {{
        unsafe fn _with_ptr<FE, FS, T>(ptr: DynRsValuePtr, f_exclusive: FE, f_shared: FS) -> T
        where
            FE: FnOnce(&value::RsValue) -> T,
            FS: FnOnce(&value::shared::SharedRsValue) -> T,
        {
            match ptr {
                DynRsValuePtr::Exclusive(v) => {
                    // Safety: caller must ensure (1), thereby
                    // guaranteeing that `v` originates from a cast
                    // from a `&RsValue`.
                    let v = unsafe { v.as_ref() };
                    // Safety: see previous statement
                    let v = unsafe { c_ffi_utils::expect_unchecked!(v, "`v` must not be null") };
                    f_exclusive(v)
                }
                DynRsValuePtr::Shared(v) => {
                    // Safety: caller must ensure (1), thereby
                    // guaranteeing that `v` originates from a call
                    // to `SharedRsValue::as_raw` which destructures
                    // the `SharedRsValue` and returns the `*const RsValueInternal`
                    // it wraps. Furthermore, the resulting `SharedRsValue`
                    // is forgotten rather than dropped below.
                    let v = unsafe { value::shared::SharedRsValue::from_raw(v) };
                    let res = f_shared(&v);
                    // Forget v to avoid double free
                    std::mem::forget(v);
                    res
                }
            }
        }

        _with_ptr($ptr, $f, $f)
    }};
}

/// A value that can either be shared (wrapping a [`SharedRsValue`])
/// or exclusive (wrapping an [`RsValue`]).
#[derive(Debug, Clone)]
pub enum DynRsValue {
    /// Exclusive, non-refcounted.
    Exclusive(RsValue),
    /// Shared, refcounted.
    Shared(SharedRsValue),
}

impl DynRsValue {
    /// Create a null value. Can be called in a `const` context.
    pub const fn null_const() -> Self {
        Self::Exclusive(RsValue::null_const())
    }

    /// Convert this [`DynRsValue`] into a `DynRsValueRef`.
    pub fn as_ref(&self) -> DynRsValueRef<'_> {
        match self {
            DynRsValue::Exclusive(v) => DynRsValueRef::Exclusive(v),
            DynRsValue::Shared(v) => DynRsValueRef::Shared(v.clone()),
        }
    }

    /// Convert this value into a [`SharedRsValue`]
    pub fn into_shared(self) -> SharedRsValue {
        match self {
            DynRsValue::Exclusive(RsValue::Undef) => SharedRsValue::undefined(),
            DynRsValue::Exclusive(RsValue::Def(internal)) => SharedRsValue::from_internal(internal),
            DynRsValue::Shared(v) => v,
        }
    }
}

impl From<RsValue> for DynRsValue {
    fn from(value: RsValue) -> Self {
        Self::Exclusive(value)
    }
}

impl From<SharedRsValue> for DynRsValue {
    fn from(value: SharedRsValue) -> Self {
        Self::Shared(value)
    }
}

impl<'v> From<&'v DynRsValue> for DynRsValueRef<'v> {
    fn from(value: &'v DynRsValue) -> Self {
        value.as_ref()
    }
}

impl Value for DynRsValue {
    fn from_internal(internal: RsValueInternal) -> Self {
        Self::Exclusive(RsValue::from_internal(internal))
    }

    fn undefined() -> Self {
        Self::Exclusive(RsValue::undefined())
    }

    fn internal(&self) -> Option<&RsValueInternal> {
        match self {
            DynRsValue::Exclusive(v) => v.internal(),
            DynRsValue::Shared(v) => v.internal(),
        }
    }

    fn swap_from_internal(&mut self, internal: RsValueInternal) {
        match self {
            DynRsValue::Exclusive(v) => v.swap_from_internal(internal),
            DynRsValue::Shared(v) => v.swap_from_internal(internal),
        }
    }

    fn to_dyn_ref(&self) -> DynRsValueRef<'_> {
        self.as_ref()
    }

    fn to_shared(&self) -> SharedRsValue {
        match self {
            DynRsValue::Exclusive(v) => v.to_shared(),
            DynRsValue::Shared(v) => v.to_shared(),
        }
    }
}

/// Pointer type that either points to an
/// exclusive [`RsValue`] or to the inner value
/// of a [`SharedRsValue`].
// DAX: Why is DynRsValuePtr an enum and not a `*const DynRsValue` or something similar?
#[derive(Debug, Clone, Copy)]
pub enum DynRsValuePtr {
    Exclusive(*const RsValue),
    Shared(*const RsValueInternal),
}

impl DynRsValuePtr {
    pub const fn exclusive(&self) -> Option<*const RsValue> {
        match *self {
            Self::Exclusive(ptr) => Some(ptr),
            _ => None,
        }
    }
    pub const fn shared(&self) -> Option<*const RsValueInternal> {
        match *self {
            Self::Shared(ptr) => Some(ptr),
            _ => None,
        }
    }

    /// Convert a [`DynRsValueRef`] into a [`DynRsValuePtr`].
    /// To convert the [`DynRsValueRef::Shared`] variant to a pointer,
    /// which holds an owned [`SharedRsValue`],
    /// this function calls [`SharedRsValue::into_raw`],
    /// ensuring the [`SharedRsValue`]
    /// does not get dropped.
    pub fn from_dyn_value_ref(v_ref: DynRsValueRef) -> Self {
        match v_ref {
            DynRsValueRef::Exclusive(v) => Self::Exclusive(v as *const RsValue),
            DynRsValueRef::Shared(v) => Self::Shared(v.into_raw()),
        }
    }

    /// Convert a [`&DynRsValue`](DynRsValue) into a [`DynRsValuePtr`].
    /// To convert the [`DynRsValueRef::Shared`] variant to a pointer,
    /// which holds a [`&SharedRsValue`](SharedRsValue) reference,
    /// this function calls [`SharedRsValue::as_raw`]
    pub const fn from_dyn_value(v: &DynRsValue) -> DynRsValuePtr {
        match v {
            DynRsValue::Exclusive(v) => DynRsValuePtr::Exclusive(v as *const RsValue),
            DynRsValue::Shared(v) => DynRsValuePtr::Shared(v.as_raw()),
        }
    }
}

pub mod opaque {
    pub use dyn_ptr::OpaqueDynRsValuePtr;
    pub use dyn_value::OpaqueDynRsValue;

    mod dyn_ptr {
        use c_ffi_utils::opaque::{Size, Transmute};

        use crate::dynamic::DynRsValuePtr;

        #[repr(C, align(8))]
        #[derive(Clone, Copy)]
        pub struct OpaqueDynRsValuePtr(Size<16>);

        // Safety: `OpaqueDynRsValuePtr` is defined as a `MaybeUninit` slice of
        // bytes with the same size and alignment as `DynRsValuePtr`, so any valid
        // `RsValue` has a bit pattern which is a valid `OpaqueDynRsValuePtr`
        unsafe impl Transmute<DynRsValuePtr> for OpaqueDynRsValuePtr {}

        c_ffi_utils::opaque!(DynRsValuePtr, OpaqueDynRsValuePtr);
    }

    mod dyn_value {
        use crate::dynamic::DynRsValue;

        use c_ffi_utils::opaque::{Size, Transmute};

        #[repr(C, align(8))]
        pub struct OpaqueDynRsValue(Size<16>);

        // Safety: `OpaqueDynRsValue` is defined as a `MaybeUninit` slice of
        // bytes with the same size and alignment as `DynRsValue`, so any valid
        // `RsValue` has a bit pattern which is a valid `OpaqueDynRsValue`.
        unsafe impl Transmute<DynRsValue> for OpaqueDynRsValue {}

        c_ffi_utils::opaque!(DynRsValue, OpaqueDynRsValue);
    }
}
