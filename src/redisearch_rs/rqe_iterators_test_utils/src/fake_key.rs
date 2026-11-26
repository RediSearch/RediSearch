/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A stand-in for the [`RLookupKey`] pipeline construction binds to a yielded value.

use rlookup::RLookupKey;

/// A stand-in for the key pipeline construction resolves for a yielded value's
/// alias. Index loading only stores a reference to it, never reads it.
pub struct FakeKey(Box<ffi::RLookupKey>);

impl FakeKey {
    /// A key with every field zeroed.
    pub fn new() -> Self {
        // SAFETY: `RLookupKey` is a plain C struct, for which all-zero is a
        // valid value (null pointers, zero indices and flags).
        Self(Box::new(unsafe { std::mem::zeroed() }))
    }

    /// The key, as the iterators' key slots hold it.
    pub fn as_ptr<'index>(&mut self) -> *mut RLookupKey<'index> {
        (&raw mut *self.0).cast()
    }

    /// The key, as the yielded values a hit carries name it.
    pub fn as_yielded_key(&self) -> *const ffi::RLookupKey {
        &raw const *self.0
    }
}

impl Default for FakeKey {
    fn default() -> Self {
        Self::new()
    }
}
