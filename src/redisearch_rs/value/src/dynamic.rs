/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RsValue, Value, shared::SharedRsValue};

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

impl Value for DynRsValue {
    fn from_internal(internal: crate::RsValueInternal) -> Self {
        Self::Exclusive(RsValue::from_internal(internal))
    }

    fn undefined() -> Self {
        Self::Exclusive(RsValue::undefined())
    }

    fn internal(&self) -> Option<&crate::RsValueInternal> {
        match self {
            DynRsValue::Exclusive(v) => v.internal(),
            DynRsValue::Shared(v) => v.internal(),
        }
    }

    fn swap_from_internal(&mut self, internal: crate::RsValueInternal) {
        match self {
            DynRsValue::Exclusive(v) => v.swap_from_internal(internal),
            DynRsValue::Shared(v) => v.swap_from_internal(internal),
        }
    }

    fn to_dyn_ref(&self) -> DynRsValueRef<'_> {
        self.as_ref()
    }
}

/// A reference to a [`DynRsValue`].
#[derive(Debug, Clone)]
pub enum DynRsValueRef<'v> {
    /// Reference to an exclusive value
    Exclusive(&'v RsValue),
    /// Refcounted, owned shared value
    Shared(SharedRsValue),
}

impl<'v> DynRsValueRef<'v> {
    /// Convert this value into a [`SharedRsValue`]
    pub fn to_shared(&self) -> SharedRsValue {
        match self {
            DynRsValueRef::Exclusive(v) => v
                .internal()
                .cloned()
                .map(SharedRsValue::from_internal)
                .unwrap_or_else(SharedRsValue::undefined),
            DynRsValueRef::Shared(v) => v.clone(),
        }
    }
}

impl<'v> From<&'v RsValue> for DynRsValueRef<'v> {
    fn from(value: &'v RsValue) -> Self {
        Self::Exclusive(value)
    }
}

impl From<SharedRsValue> for DynRsValueRef<'_> {
    fn from(value: SharedRsValue) -> Self {
        Self::Shared(value)
    }
}

impl<'v> From<&'v DynRsValue> for DynRsValueRef<'v> {
    fn from(value: &'v DynRsValue) -> Self {
        value.as_ref()
    }
}

impl<'v> Value for DynRsValueRef<'v> {
    fn from_internal(internal: crate::RsValueInternal) -> Self {
        Self::Shared(SharedRsValue::from_internal(internal))
    }

    fn undefined() -> Self {
        Self::Shared(SharedRsValue::undefined())
    }

    fn internal(&self) -> Option<&crate::RsValueInternal> {
        match self {
            DynRsValueRef::Exclusive(v) => v.internal(),
            DynRsValueRef::Shared(v) => v.internal(),
        }
    }

    fn to_dyn_ref(&self) -> DynRsValueRef<'_> {
        self.clone()
    }
}

pub mod opaque {
    pub use dyn_value::OpaqueDynRsValue;
    pub use dyn_value_ref::OpaqueDynRsValueRef;

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
    mod dyn_value_ref {
        use c_ffi_utils::opaque::{Size, Transmute};

        use crate::dynamic::DynRsValueRef;

        #[repr(C, align(8))]
        pub struct OpaqueDynRsValueRef(Size<16>);

        // Safety: `OpaqueDynRsValueRef` is defined as a `MaybeUninit` slice of
        // bytes with the same size and alignment as `DynRsValueRef`, so any valid
        // `RsValue` has a bit pattern which is a valid `OpaqueDynRsValueRef`
        unsafe impl Transmute<DynRsValueRef<'_>> for OpaqueDynRsValueRef {}

        c_ffi_utils::opaque!(DynRsValueRef<'c>, OpaqueDynRsValueRef);
    }
}
