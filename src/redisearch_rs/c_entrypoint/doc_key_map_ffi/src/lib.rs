/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Key-to-document-ID lookup without owning another copy of document keys.

use ahash::RandomState;
use hashbrown::HashTable;
use std::{ffi::c_void, slice};

/// A candidate whose key remains owned by the document table.
struct Entry {
    hash: u64,
    id: u64,
}

/// Spec-lock-protected lookup table. Full-hash collisions remain separate entries.
pub struct DocKeyMap {
    entries: HashTable<Entry>,
    hasher: RandomState,
}

impl Default for DocKeyMap {
    fn default() -> Self {
        Self {
            entries: HashTable::new(),
            hasher: RandomState::new(),
        }
    }
}

impl DocKeyMap {
    /// Hash a binary key using this table's private seed.
    pub fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Insert an ID once. The caller must remove it before changing its key.
    pub fn insert(&mut self, hash: u64, id: u64) {
        assert_ne!(id, 0);
        self.entries
            .insert_unique(hash, Entry { hash, id }, |e| e.hash);
    }

    /// Remove exactly one candidate, including when full hashes collide.
    pub fn remove(&mut self, hash: u64, id: u64) -> bool {
        if let Ok(entry) = self
            .entries
            .find_entry(hash, |e| e.hash == hash && e.id == id)
        {
            entry.remove();
            true
        } else {
            false
        }
    }

    /// Find an ID by comparing candidate keys through their owning document table.
    pub fn find(&self, hash: u64, mut matches: impl FnMut(u64) -> bool) -> u64 {
        self.entries
            .find(hash, |e| e.hash == hash && matches(e.id))
            .map_or(0, |e| e.id)
    }

    /// Resolve a borrowed key, checking full-key equality for matching hash candidates.
    pub fn get_id(&self, key: &[u8], matches: impl FnMut(u64) -> bool) -> u64 {
        self.find(self.hash(key), matches)
    }

    /// Allocated bucket/control storage plus the table object, excluding document keys.
    pub fn memory_usage(&self) -> usize {
        size_of::<Self>() + self.entries.allocation_size()
    }
}

/// Allocate a table. Production uses the Redis global allocator.
#[unsafe(no_mangle)]
pub extern "C" fn DocKeyMap_New() -> *mut DocKeyMap {
    Box::into_raw(Box::new(DocKeyMap::default()))
}

/// Destroy a table; a null pointer is accepted.
///
/// # Safety
/// A non-null `map` must be a [valid], exclusively owned allocation from
/// [`DocKeyMap_New`] that has not already been destroyed.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Free(map: *mut DocKeyMap) {
    if !map.is_null() {
        // SAFETY: The caller transfers ownership of this live allocation.
        drop(unsafe { Box::from_raw(map) });
    }
}

/// Hash borrowed key bytes; no key storage is retained.
///
/// # Safety
/// `map` must be [valid] for shared access. `key` must be [valid] for `len`
/// readable bytes, or may be null when `len` is zero.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Hash(map: *const DocKeyMap, key: *const u8, len: usize) -> u64 {
    debug_assert!(!map.is_null());
    debug_assert!(len == 0 || !key.is_null());
    // SAFETY: The caller guarantees a shared table borrow.
    let map = unsafe { &*map };
    let key = if len == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees these bytes are readable for this call.
        unsafe { slice::from_raw_parts(key, len) }
    };
    map.hash(key)
}

/// Insert a prehashed document ID.
///
/// # Safety
/// `map` must be [valid] for exclusive access. The ID must be nonzero and absent.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Insert(map: *mut DocKeyMap, hash: u64, id: u64) {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees exclusive table access.
    unsafe { &mut *map }.insert(hash, id);
}

/// Remove a prehashed document ID, returning whether it was present.
///
/// # Safety
/// `map` must be [valid] for exclusive access.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Remove(map: *mut DocKeyMap, hash: u64, id: u64) -> bool {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees exclusive table access.
    unsafe { &mut *map }.remove(hash, id)
}

/// Compare a candidate's document key with the query key borrowed through `ctx`.
pub type DocKeyMatches = unsafe extern "C" fn(ctx: *const c_void, id: u64) -> bool;

/// Resolve a borrowed key with full-key comparison through the document table.
///
/// # Safety
/// `map` must be [valid] for shared access throughout the call. `key` must be
/// [valid] for `len` readable bytes, or may be null when `len` is zero. `matches` must
/// be [ABI-compatible] with [`DocKeyMatches`] and callable with `ctx` for
/// each candidate ID. The callback must not mutate the table or its candidates.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
/// [ABI-compatible]: https://doc.rust-lang.org/std/primitive.fn.html#abi-compatibility
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_GetId(
    map: *const DocKeyMap,
    key: *const u8,
    len: usize,
    ctx: *const c_void,
    matches: DocKeyMatches,
) -> u64 {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees a shared table borrow.
    let map = unsafe { &*map };
    debug_assert!(len == 0 || !key.is_null());
    let key = if len == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees these bytes are readable for this call.
        unsafe { slice::from_raw_parts(key, len) }
    };
    map.get_id(key, |id| {
        // SAFETY: The caller guarantees the callback and context remain usable.
        unsafe { matches(ctx, id) }
    })
}

/// Report allocated table storage; null denotes disabled acceleration and returns zero.
///
/// # Safety
/// A non-null `map` must be [valid] for shared access.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_MemoryUsage(map: *const DocKeyMap) -> usize {
    if map.is_null() {
        0
    } else {
        // SAFETY: The caller guarantees a shared table borrow.
        unsafe { &*map }.memory_usage()
    }
}
