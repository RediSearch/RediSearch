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
#[derive(Debug, Default, Clone, Copy)]
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

impl RsValueType {
    /// Check whether the type is `RsValue::Undefined`
    pub const fn is_undefined(self) -> bool {
        matches!(self, Self::Undefined)
    }

    /// Check whether the type is `RsValue::Null`
    pub const fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Check whether the type is `RsValue::Number`
    pub const fn is_number(self) -> bool {
        matches!(self, Self::Number)
    }

    /// Check whether the type is `RsValue::RmAllocString`
    pub const fn is_rm_alloc_string(self) -> bool {
        matches!(self, Self::RmAllocString)
    }

    /// Check whether the type is `RsValue::ConstString`
    pub const fn is_const_string(self) -> bool {
        matches!(self, Self::ConstString)
    }

    /// Check whether the type is `RsValue::OwnedRedisString`
    pub const fn is_owned_redis_string(self) -> bool {
        matches!(self, Self::OwnedRedisString)
    }

    /// Check whether the type is `RsValue::BorrowedRedisString`
    pub const fn is_borrowed_redis_string(self) -> bool {
        matches!(self, Self::BorrowedRedisString)
    }

    /// Check whether the type is `RsValue::String`
    pub const fn is_string(self) -> bool {
        matches!(self, Self::String)
    }

    /// Check whether the type is `Self::RmAllocString`, `Self::ConstString`,
    /// `Self::OwnedRedisString`, `Self::BorrowedRedisString`, or `Self::String`.
    pub const fn is_any_string(self) -> bool {
        matches!(
            self,
            Self::RmAllocString
                | Self::ConstString
                | Self::OwnedRedisString
                | Self::BorrowedRedisString
                | Self::String
        )
    }

    /// Check whether the type is `RsValue::Array`
    pub const fn is_array(self) -> bool {
        matches!(self, Self::Array)
    }

    /// Check whether the type is `RsValue::Ref`
    pub const fn is_ref(self) -> bool {
        matches!(self, Self::Ref)
    }

    /// Check whether the type is `RsValue::Trio`
    pub const fn is_trio(self) -> bool {
        matches!(self, Self::Trio)
    }

    /// Check whether the type is `RsValue::Map`
    pub const fn is_map(self) -> bool {
        matches!(self, Self::Map)
    }
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
