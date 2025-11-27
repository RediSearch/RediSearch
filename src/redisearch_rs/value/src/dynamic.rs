/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{RsValue, Value, shared::SharedRsValue};

/// A reference to either an [`RsValue`] or a [`SharedRsValue`].
// DAX: Shouldn't this live in the values ffi bridge together with `DynRsValue` and `DynRsValuePtr`?
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

    fn to_shared(&self) -> SharedRsValue {
        match self {
            DynRsValueRef::Exclusive(v) => v.to_shared(),
            DynRsValueRef::Shared(v) => v.to_shared(),
        }
    }
}

pub mod opaque {}
