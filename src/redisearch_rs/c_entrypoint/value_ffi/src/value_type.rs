/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{RsValue, RsValueInternal, Value, shared::SharedRsValue};

/// Enumeration of the types an
/// `RsValue` or a `SharedRsValue` can be of.
/// cbindgen:prefix-with-name
#[repr(C)]
#[derive(Debug, Default)]
pub enum RsValueType {
    #[default]
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

pub(crate) trait AsRsValueType {
    fn as_value_type(&self) -> RsValueType;
}

impl AsRsValueType for RsValueInternal {
    fn as_value_type(&self) -> RsValueType {
        use RsValueType::*;
        match self {
            RsValueInternal::Null => Null,
            RsValueInternal::Number(_) => Number,
            RsValueInternal::RmAllocString(_) => RmAllocString,
            RsValueInternal::ConstString(_) => ConstString,
            RsValueInternal::OwnedRedisString(_) => OwnedRedisString,
            RsValueInternal::BorrowedRedisString(_) => BorrowedRedisString,
            RsValueInternal::String(_) => String,
            RsValueInternal::Array(_) => Array,
            RsValueInternal::Ref(_) => Ref,
            RsValueInternal::Trio(_) => Trio,
            RsValueInternal::Map(_) => Map,
        }
    }
}

impl AsRsValueType for SharedRsValue {
    fn as_value_type(&self) -> RsValueType {
        self.internal()
            .map(|i| i.as_value_type())
            .unwrap_or_default()
    }
}

impl AsRsValueType for RsValue {
    fn as_value_type(&self) -> RsValueType {
        self.internal()
            .map(|i| i.as_value_type())
            .unwrap_or_default()
    }
}
