/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt::Debug;

pub use crate::{
    collection::{RsValueArray, RsValueMap},
    redis_string::RedisString,
    rs_string::RsString,
    shared::SharedRsValue,
    trio::RsValueTrio,
};

/// Ports part of the RediSearch RSValue type to Rust. This is a temporary solution until we have a proper
/// Rust port of the RSValue type.
#[cfg(feature = "c_ffi_impl")]
mod rs_value_ffi;
#[cfg(feature = "c_ffi_impl")]
pub use rs_value_ffi::*;

#[cfg(feature = "test_utils")]
mod test_utils;
#[cfg(feature = "test_utils")]
pub use test_utils::RSValueMock;

pub mod collection;
pub mod redis_string;
pub mod rs_string;
pub mod shared;
pub mod trio;

/// An actual [`RsValue`] object
#[derive(Debug, Clone)]
pub enum RsValue {
    /// Undefined, not holding a value.
    Undefined,
    /// Null value
    Null,
    /// Numeric value
    Number(f64),
    /// String
    String(RsString),
    /// String value backed by a Redis string
    RedisString(RedisString),
    /// Array value
    Array(RsValueArray),
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(RsValueMap),
}

impl RsValue {
    pub fn fully_dereferenced(&self) -> &Self {
        if let RsValue::Ref(ref_value) = self {
            ref_value.value().fully_dereferenced()
        } else {
            self
        }
    }

    pub fn variant_name(&self) -> &'static str {
        match self {
            RsValue::Undefined => "Undefined",
            RsValue::Null => "Null",
            RsValue::Number(_) => "Number",
            RsValue::String(_) => "String",
            RsValue::RedisString(_) => "RedisString",
            RsValue::Array(_) => "Array",
            RsValue::Ref(_) => "Ref",
            RsValue::Trio(_) => "Trio",
            RsValue::Map(_) => "Map",
        }
    }
}

#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();
#[cfg(test)]
#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();
