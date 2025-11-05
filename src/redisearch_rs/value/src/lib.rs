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

use std::fmt::Debug;

use crate::{map::RsValueMap, shared::SharedRsValue, trio::RsValueTrio};

#[cfg(feature = "c_ffi_impl")]
/// Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
/// Rust port of the RSValue type.
mod rs_value_ffi;
#[cfg(feature = "c_ffi_impl")]
pub use rs_value_ffi::*;

pub mod map;
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
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(RsValueMap),
    // TODO add array variant, possibly based on LowMemoryThinVec
    // TODO add string variants
}

/// A stack-allocated RediSearch dynamic value.
// TODO: optimize memory layout
/// cbindgen:prefix-with-name
#[derive(Debug, Default, Clone)]
pub enum RsValue {
    #[default]
    /// Undefined, not holding a value.
    Undef,
    /// Defined and holding a value.
    Def(RsValueInternal),
}

impl RsValue {
    /// Create a new, undefined [`RsValue`]
    pub const fn undefined() -> Self {
        Self::Undef
    }

    /// Create a new, NULL [`RsValue`]
    pub const fn null() -> Self {
        Self::Def(RsValueInternal::Null)
    }

    /// Create a new numeric [`RsValue`] given the passed number
    pub const fn number(n: f64) -> Self {
        Self::Def(RsValueInternal::Number(n))
    }

    /// Clear this [`RsValue`]
    pub fn clear(&mut self) {
        *self = Self::Undef
    }
}

pub mod opaque {
    use c_ffi_utils::opaque::{Size, Transmute};

    use super::RsValue;

    /// Opaque variant of [`RsValue`], allowing the
    /// non-FFI-safe [`RsValue`] to be passed to C
    /// and even allow C land to place it on the stack.
    #[repr(C, align(8))]
    pub struct OpaqueRsValue(Size<24>);

    // Safety: `OpaqueRsValue` is defined as a `MaybeUninit` slice of
    // bytes with the same size and alignment as `RsValue`, so any valid
    // `RsValue` has a bit pattern which is a valid `OpaqueRsValue`.
    unsafe impl Transmute<RsValue> for OpaqueRsValue {}

    c_ffi_utils::opaque!(RsValue, OpaqueRsValue);
}
