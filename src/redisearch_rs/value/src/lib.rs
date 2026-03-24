/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub use crate::{
    collection::{Array, Map},
    redis_string::RedisString,
    rs_string::RsString,
    shared::SharedRsValue,
    trio::RsValueTrio,
};
use std::fmt::Debug;

pub mod collection;
pub mod comparison;
pub mod debug;
pub mod hash;
mod pool;
pub mod redis_string;
pub mod rs_string;
pub mod sds_writer;
pub mod shared;
pub mod trio;
pub mod util;

/// An actual [`RsValue`] object
#[derive(Debug)]
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
    Array(Array),
    /// Reference value
    Ref(SharedRsValue),
    /// Trio value
    Trio(RsValueTrio),
    /// Map value
    Map(Map),
}

impl RsValue {
    pub fn new_string(str: Vec<u8>) -> Self {
        Self::String(RsString::from_vec(str))
    }

    pub fn fully_dereferenced_ref(&self) -> &Self {
        match self {
            RsValue::Ref(ref_value) => ref_value.fully_dereferenced_ref(),
            _ => self,
        }
    }

    pub fn fully_dereferenced_ref_and_trio(&self) -> &Self {
        match self {
            RsValue::Ref(ref_value) => ref_value.fully_dereferenced_ref_and_trio(),
            RsValue::Trio(trio) => trio.left().fully_dereferenced_ref_and_trio(),
            _ => self,
        }
    }

    pub const fn variant_name(&self) -> &'static str {
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

    pub const fn is_null(&self) -> bool {
        matches!(self, RsValue::Null)
    }

    /// Returns the string bytes of the value, if it is a string type.
    pub fn as_str_bytes(&self) -> Option<&[u8]> {
        match self {
            RsValue::String(str) => Some(str.as_bytes()),
            RsValue::RedisString(str) => Some(str.as_bytes()),
            _ => None,
        }
    }

    pub const fn as_num(&self) -> Option<f64> {
        if let RsValue::Number(num) = self {
            Some(*num)
        } else {
            None
        }
    }

    pub const fn debug_formatter(&self, obfuscate: bool) -> debug::DebugFormatter<'_> {
        debug::DebugFormatter {
            value: self,
            obfuscate,
        }
    }
}
