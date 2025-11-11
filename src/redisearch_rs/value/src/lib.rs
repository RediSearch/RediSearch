/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#[cfg(feature = "test_utils")]
mod test_utils;
#[cfg(feature = "test_utils")]
pub use test_utils::RSValueMock;

use std::fmt::{self, Debug};

use crate::{
    collection::{RsValueArray, RsValueMap},
    shared::SharedRsValue,
    trio::RsValueTrio,
};

#[cfg(feature = "c_ffi_impl")]
/// Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
/// Rust port of the RSValue type.
mod rs_value_ffi;
#[cfg(feature = "c_ffi_impl")]
pub use rs_value_ffi::*;

pub mod collection;
pub mod shared;
pub mod trio;

#[derive(Debug, Clone)]
#[repr(C)]
pub struct SDS(usize); // TODO bind

/// Internal storage of [`RsValue`] and [`SharedRsValue`]
// TODO: optimize memory size
#[derive(Debug, Clone)]
pub enum RsValueInternal {
    /// Null value
    Null,
    /// Numeric value
    Number(f64),
    /// Array value
    Array(RsValueArray),
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(RsValueMap),
    // TODO add string variants
}

/// A stack-allocated RediSearch dynamic value.
// TODO: optimize memory layout
/// cbindgen:prefix-with-name
#[derive(Default, Clone)]
pub enum RsValue {
    #[default]
    /// Undefined, not holding a value.
    Undef,
    /// Defined and holding a value.
    Def(RsValueInternal),
}

impl RsValue {
    pub const fn null_const() -> Self {
        Self::Def(RsValueInternal::Null)
    }
}

impl fmt::Debug for RsValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.internal() {
            Some(internal) => internal.fmt(f),
            None => f.debug_tuple("Undefined").finish(),
        }
    }
}

impl Value for RsValue {
    fn from_internal(internal: RsValueInternal) -> Self {
        Self::Def(internal)
    }

    fn undefined() -> Self {
        Self::Undef
    }

    fn internal(&self) -> Option<&RsValueInternal> {
        match self {
            Self::Undef => None,
            Self::Def(internal) => Some(internal),
        }
    }
}

pub trait Value: Sized {
    /// Create a new value from an [`RsValueInternal`]
    fn from_internal(internal: RsValueInternal) -> Self;

    // Create a new, undefined value
    fn undefined() -> Self;

    // Clear this value
    fn clear(&mut self) {
        *self = Self::undefined();
    }

    /// Get a reference to the [`RsValueInternal`] that is
    /// held by this value if it is defined. Returns `None` if
    /// the value is undefined.
    fn internal(&self) -> Option<&RsValueInternal>;

    /// Create a new, NULL value
    fn null() -> Self {
        Self::from_internal(RsValueInternal::Null)
    }

    /// Create a new numeric value given the passed number
    fn number(n: f64) -> Self {
        Self::from_internal(RsValueInternal::Number(n))
    }

    /// Create a new trio value
    fn trio(left: SharedRsValue, middle: SharedRsValue, right: SharedRsValue) -> Self {
        Self::from_internal(RsValueInternal::Trio(RsValueTrio::new(left, middle, right)))
    }

    /// Create a new array value
    fn array(arr: RsValueArray) -> Self {
        Self::from_internal(RsValueInternal::Array(arr))
    }

    /// Create a new map value
    fn map(map: RsValueMap) -> Self {
        Self::from_internal(RsValueInternal::Map(map))
    }

    /// Attempt to parse the passed string as an `f64`, and wrap it
    /// in a [`SharedRsValue`].
    fn parse_number(s: &str) -> Result<Self, std::num::ParseFloatError> {
        Ok(Self::number(s.parse()?))
    }

    /// Get the number value. Returns `None` if the value is not
    /// a number.
    fn get_number(&self) -> Option<f64> {
        let RsValueInternal::Number(number) = self.internal()? else {
            return None;
        };
        Some(*number)
    }
}

pub mod opaque {
    use c_ffi_utils::opaque::{Size, Transmute};

    use super::RsValue;

    /// Opaque projection of [`RsValue`], allowing the
    /// non-FFI-safe [`RsValue`] to be passed to C
    /// and even allow C land to place it on the stack.
    #[repr(C, align(8))]
    pub struct OpaqueRsValue(Size<16>);

    // Safety: `OpaqueRsValue` is defined as a `MaybeUninit` slice of
    // bytes with the same size and alignment as `RsValue`, so any valid
    // `RsValue` has a bit pattern which is a valid `OpaqueRsValue`.
    unsafe impl Transmute<RsValue> for OpaqueRsValue {}

    c_ffi_utils::opaque!(RsValue, OpaqueRsValue);
}
