/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::sync::Arc;

use crate::{RsValue, Value};

/// A shared RedisSearch dynamic value, backed by an `Arc<RsValue>`.
#[derive(Clone)]
pub struct SharedRsValue {
    inner: Arc<RsValue>,
}

impl SharedRsValue {
    /// Create a new shared RsValue wrapping an [`RsValue`]
    pub fn new(value: RsValue) -> Self {
        Self {
            inner: Arc::new(value),
        }
    }

    /// Convert a [`SharedRsValue`] into a raw `*const RsValue` pointer.
    pub fn into_raw(self) -> *const RsValue {
        Arc::into_raw(self.inner)
    }

    /// Convert a `*const RsValue` back into a [`SharedRsValue`].
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer obtained from `SharedRsValue::into_raw`.
    pub unsafe fn from_raw(ptr: *const RsValue) -> Self {
        Self {
            // SAFETY: `ptr` is a valid pointer obtained from `SharedRsValue::into_raw`.
            inner: unsafe { Arc::from_raw(ptr) },
        }
    }

    /// Get a reference to the inner [`RsValue`].
    pub fn value(&self) -> &RsValue {
        &self.inner
    }
}

impl Value for SharedRsValue {
    fn from_value(value: RsValue) -> Self {
        Self::new(value)
    }
}

impl std::fmt::Debug for SharedRsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value().fmt(f)
    }
}
