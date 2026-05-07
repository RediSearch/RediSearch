/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Opaque FFI wrappers around [`TrieMap`](crate::TrieMap) and
//! [`RuneTrieMap`](crate::RuneTrieMap) for use with C code.

use std::{ffi::c_void, ptr::NonNull};

/// Opaque type wrapping a [`TrieMap<*mut c_void>`](crate::TrieMap) for FFI use.
///
/// This type is intended to be passed across the FFI boundary as an opaque
/// pointer. It can be instantiated with `TrieMap(crate::TrieMap::new())` and
/// the inner [`crate::TrieMap`] can be accessed via the public field.
pub struct TrieMap(pub crate::TrieMap<*mut c_void>);

/// Opaque type wrapping a [`RuneTrieMap<*mut c_void>`](crate::RuneTrieMap)
/// for FFI use.
///
/// Like [`TrieMap`], this is meant to be passed across the FFI boundary as
/// an opaque pointer. The payload type is uniformly `*mut c_void`; the
/// concrete meaning (e.g. `Box<f32>` score for the dictionary path,
/// `suffixData *` for the suffix path) is owned by the caller.
pub struct RuneTrieMap(pub crate::RuneTrieMap<*mut c_void>);

impl TrieMap {
    /// Find the value associated with a key in the trie.
    ///
    /// Returns `None` if the key does not exist or if the stored value is
    /// null. Returns `Some(value)` with a non-null pointer otherwise.
    pub fn find(&self, key: &[u8]) -> Option<NonNull<c_void>> {
        self.0.find(key).copied().and_then(NonNull::new)
    }

    /// Insert a key-value pair into the trie.
    ///
    /// Returns the previous value associated with the key if it was present.
    pub fn insert(&mut self, key: &[u8], value: *mut c_void) -> Option<*mut c_void> {
        self.0.insert(key, value)
    }

    /// Remove a key from the trie.
    ///
    /// Returns the value associated with the key if it was present.
    pub fn remove(&mut self, key: &[u8]) -> Option<*mut c_void> {
        self.0.remove(key)
    }
}
