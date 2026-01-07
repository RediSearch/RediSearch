/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{RsValue, Value, shared::SharedRsValue};

/// Enumeration of the types an
/// `RsValue` or a `SharedRsValue` can be of.
/// cbindgen:prefix-with-name
#[repr(C)]
#[derive(Debug)]
pub enum RsValueType {
    Undefined,
    Null,
    Number,
    RmAllocString,
    ConstString,
    OwnedRedisString,
    BorrowedRedisString,
    String,
    Array,
    Ref,
    Trio,
    Map,
}

pub trait AsRsValueType {
    fn as_value_type(&self) -> RsValueType;
}

impl AsRsValueType for RsValue {
    fn as_value_type(&self) -> RsValueType {
        use RsValueType::*;
        match self {
            RsValue::Undefined => Undefined,
            RsValue::Null => Null,
            RsValue::Number(_) => Number,
            RsValue::RmAllocString(_) => RmAllocString,
            RsValue::ConstString(_) => ConstString,
            RsValue::OwnedRedisString(_) => OwnedRedisString,
            RsValue::BorrowedRedisString(_) => BorrowedRedisString,
            RsValue::String(_) => String,
            RsValue::Array(_) => Array,
            RsValue::Ref(_) => Ref,
            RsValue::Trio(_) => Trio,
            RsValue::Map(_) => Map,
        }
    }
}

impl AsRsValueType for SharedRsValue {
    fn as_value_type(&self) -> RsValueType {
        self.value().as_value_type()
    }
}
