/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{SharedValue, util::debug_assert_warn};
use std::ops::{Deref, DerefMut};

pub type Array = Collection<SharedValue>;
pub type Map = Collection<(SharedValue, SharedValue)>;

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

impl Map {
    /// Looks up a value by its string key bytes, returning a reference to the
    /// first matching [`SharedValue`] or `None` if no match is found.
    ///
    /// Returning `&SharedValue` lets callers clone without unwrapping.
    ///
    /// Non-string keys (e.g. numeric values) are skipped.
    pub fn get(&self, key: &[u8]) -> Option<&SharedValue> {
        self.iter()
            .find_map(|(k, v)| (k.as_str_bytes()? == key).then_some(v))
    }
}

impl Array {
    /// Looks up a value by string key in a flat key-value array layout
    /// (`[k1, v1, k2, v2, ...]`), returning a reference to the first matching
    /// [`SharedValue`] or `None` if no match is found.
    ///
    /// This is needed because RESP2 does not have a native map type.
    /// Map-like data (e.g. `extra_attributes`) is sent as a flat array of
    /// alternating keys and values. In RESP3 this same data arrives as a
    /// proper [`Map`], where [`Map::get`] can be used instead.
    ///
    /// # Odd-length arrays
    ///
    /// An odd number of elements indicates malformed data. A warning is emitted
    /// via `tracing` and a `debug_assert!` fires in debug builds. The trailing
    /// element is ignored and the search proceeds over the well-formed prefix.
    ///
    /// Non-string keys are skipped.
    pub fn map_get(&self, key: &[u8]) -> Option<&SharedValue> {
        let (pairs, remainder) = self.as_chunks::<2>();
        debug_assert_warn!(
            remainder.is_empty(),
            "map_get called on an odd-length array;"
        );
        pairs
            .iter()
            .find_map(|[k, v]| (k.as_str_bytes()? == key).then_some(v))
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
