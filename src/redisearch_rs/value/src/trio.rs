/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::fmt;

use crate::shared::SharedValue;

/// A container for the [`Value::Trio`](crate::Value::Trio) variant.
#[derive(Clone)]
pub struct Trio(Box<(SharedValue, SharedValue, SharedValue)>);

impl Trio {
    pub fn new(left: SharedValue, middle: SharedValue, right: SharedValue) -> Self {
        Self(Box::new((left, middle, right)))
    }

    pub fn left(&self) -> &SharedValue {
        &self.0.0
    }

    pub fn middle(&self) -> &SharedValue {
        &self.0.1
    }

    pub fn right(&self) -> &SharedValue {
        &self.0.2
    }
}

impl fmt::Debug for Trio {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RsValueTrio")
            .field(&self.0.0)
            .field(&self.0.1)
            .field(&self.0.2)
            .finish()
    }
}
