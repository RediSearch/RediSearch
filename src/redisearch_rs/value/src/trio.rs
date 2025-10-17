/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;

use crate::shared::SharedRsValue;

/// A container for the [`RsValueInternal::Trio`](crate::RsValueInternal::Trio)
/// variant.
#[repr(C)]
#[derive(Clone)]
pub struct RsValueTrio(Box<RsValueTrioData>);

impl RsValueTrio {
    pub fn new(left: SharedRsValue, middle: SharedRsValue, right: SharedRsValue) -> Self {
        Self(Box::new(RsValueTrioData(left, middle, right)))
    }
}

impl fmt::Debug for RsValueTrio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RsValueTrio")
            .field(&self.0.0)
            .field(&self.0.1)
            .field(&self.0.2)
            .finish()
    }
}

/// Tuple struct holding 3 [`SharedRsValue`] items.
#[derive(Clone)]
struct RsValueTrioData(SharedRsValue, SharedRsValue, SharedRsValue);
