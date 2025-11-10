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
    Array,
    Ref,
    Trio,
    Map,
    // TODO add string variants
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
            RsValueInternal::Array(_) => Array,
            RsValueInternal::Ref(_) => Ref,
            RsValueInternal::Trio(_) => Trio,
            RsValueInternal::Map(_) => Map,
        }
    }
}

impl AsRsValueType for RsValue {
    fn as_value_type(&self) -> RsValueType {
        match self {
            RsValue::Undef => RsValueType::Undefined,
            RsValue::Def(i) => i.as_value_type(),
        }
    }
}

impl AsRsValueType for SharedRsValue {
    fn as_value_type(&self) -> RsValueType {
        self.internal()
            .map(AsRsValueType::as_value_type)
            .unwrap_or_default()
    }
}
