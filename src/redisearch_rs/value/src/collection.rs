/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::{Deref, DerefMut};

use crate::SharedRsValue;

pub type Array = Collection<SharedRsValue>;
pub type Map = Collection<(SharedRsValue, SharedRsValue)>;

/// A wrapper around a box slice which limits its `len` to `u32::MAX`
/// for compatibility with existing C code.
#[derive(Debug, Clone)]
pub struct Collection<T>(Box<[T]>);

impl<T> Collection<T> {
    /// Wraps the box slice inside this collection struct
    ///
    /// # Panic
    ///
    /// Panics if the `len` of the box slice is > u32::MAX
    pub fn new(inner: Box<[T]>) -> Self {
        assert!(inner.len() <= u32::MAX as usize);

        Self(inner)
    }

    /// Gets the `len` of the collection which is ensured
    /// to be <= u32::MAX during construction.
    pub fn len_u32(&self) -> u32 {
        self.len() as u32
    }
}

impl<T> Deref for Collection<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Collection<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
