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
use rqe_core::DocId;
use std::{ffi::c_void, slice};

/// A candidate whose key remains owned by the document table.
struct Entry {
    hash: u64,
    id: DocId,
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
    fn hash(&self, key: &[u8]) -> u64 {
        self.hasher.hash_one(key)
    }

    /// Reserve room for at least `capacity` entries, preserving existing candidates.
    ///
    /// Backfill knows its live-document count and can avoid repeated full-table rehashes.
    pub fn reserve(&mut self, capacity: usize) {
        self.entries
            .reserve(capacity.saturating_sub(self.entries.len()), |e| e.hash);
    }

    /// Insert an ID once. The caller must remove it before changing its key.
    pub fn insert(&mut self, key: &[u8], id: DocId) {
        self.insert_hashed(self.hash(key), id)
    }

    fn insert_hashed(&mut self, hash: u64, id: DocId) {
        debug_assert_ne!(id, 0);
        self.entries
            .insert_unique(hash, Entry { hash, id }, |e| e.hash);
    }

    /// Populate an ID during backfill, retaining a candidate already maintained by a writer.
    pub fn insert_if_absent(&mut self, hash: u64, id: DocId) {
        debug_assert_ne!(id, 0);
        self.entries
            .entry(hash, |entry| entry.id == id, |entry| entry.hash)
            .or_insert(Entry { hash, id });
    }

    /// Remove exactly one candidate, including when full hashes collide.
    pub fn remove(&mut self, key: &[u8], id: DocId) -> bool {
        self.remove_hashed(self.hash(key), id)
    }

    fn remove_hashed(&mut self, hash: u64, id: DocId) -> bool {
        if let Ok(entry) = self.entries.find_entry(hash, |e| e.id == id) {
            entry.remove();
            true
        } else {
            false
        }
    }

    /// Find an ID by comparing candidate keys through their owning document table.
    fn find(&self, hash: u64, mut matches: impl FnMut(DocId) -> bool) -> DocId {
        self.entries
            .find(hash, |e| e.hash == hash && matches(e.id))
            .map_or(0, |e| e.id)
    }

    /// Resolve a borrowed key, checking full-key equality for matching hash candidates.
    pub fn get_id(&self, key: &[u8], matches: impl FnMut(DocId) -> bool) -> DocId {
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

/// Reserve room for at least `capacity` entries without changing membership.
///
/// # Safety
/// `map` must be [valid] for exclusive access and allocated by [`DocKeyMap_New`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Reserve(map: *mut DocKeyMap, capacity: usize) {
    debug_assert!(!map.is_null());
    // SAFETY: The caller provides exclusive access to a live map.
    unsafe { &mut *map }.reserve(capacity);
}

/// Populate a backfilled ID without duplicating a candidate maintained by a writer.
///
/// # Safety
/// `map` must be [valid] for exclusive access and allocated by [`DocKeyMap_New`].
/// The caller must remove an ID before changing its key and corresponding hash.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_InsertIfAbsent(map: *mut DocKeyMap, hash: u64, id: DocId) {
    debug_assert!(!map.is_null());
    // SAFETY: The caller provides exclusive access to a live map.
    unsafe { &mut *map }.insert_if_absent(hash, id);
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

/// Borrow key bytes, accepting a null pointer for an empty key.
///
/// # Safety
/// A nonempty key must be [valid] for reads for the returned lifetime.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn key_bytes<'a>(key: *const u8, len: usize) -> &'a [u8] {
    debug_assert!(len == 0 || !key.is_null());
    if len == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees the bytes are readable for the borrow.
        unsafe { slice::from_raw_parts(key, len) }
    }
}

/// Insert a document ID using borrowed key bytes.
///
/// # Safety
/// `key` must be [valid] for `len` readable bytes, or null when `len` is zero.
/// `map` must be [valid] for exclusive access. The ID must be nonzero and absent.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Insert(
    map: *mut DocKeyMap,
    key: *const u8,
    len: usize,
    id: DocId,
) {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees exclusive table access and readable key bytes.
    unsafe { &mut *map }.insert(unsafe { key_bytes(key, len) }, id);
}

/// Remove a document ID using borrowed key bytes, returning whether it was present.
///
/// # Safety
/// `key` must be [valid] for `len` readable bytes, or null when `len` is zero.
/// `map` must be [valid] for exclusive access.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn DocKeyMap_Remove(
    map: *mut DocKeyMap,
    key: *const u8,
    len: usize,
    id: DocId,
) -> bool {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees exclusive table access and readable key bytes.
    unsafe { &mut *map }.remove(unsafe { key_bytes(key, len) }, id)
}

/// Compare a candidate's document key with the query key borrowed through `ctx`.
pub type DocKeyMatches = unsafe extern "C" fn(ctx: *const c_void, id: DocId) -> bool;

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
) -> DocId {
    debug_assert!(!map.is_null());
    // SAFETY: The caller guarantees a shared table borrow.
    let map = unsafe { &*map };
    // SAFETY: The caller guarantees readable key bytes for this call.
    let key = unsafe { key_bytes(key, len) };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn colliding_candidates_survive_growth_and_independent_removal() {
        let mut map = DocKeyMap::default();
        // Full-hash collisions make this quadratic; 64 entries still exercise multiple resizes.
        let count = if cfg!(miri) { 64 } else { 1000 };
        let removed = count / 2;
        let empty = map.memory_usage();
        assert_eq!(map.find(42, |_| true), 0);
        for id in 1..=count {
            map.insert_hashed(42, id);
        }
        assert!(map.memory_usage() > empty);
        for id in 1..=count {
            assert_eq!(map.find(42, |candidate| candidate == id), id);
        }
        assert_eq!(map.find(43, |_| true), 0);
        assert!(map.remove_hashed(42, removed));
        assert!(!map.remove_hashed(42, removed));
        assert_eq!(map.find(42, |id| id == removed), 0);
        assert_eq!(map.find(42, |id| id == removed + 1), removed + 1);
        for id in 1..=count {
            if id != removed {
                assert!(map.remove_hashed(42, id));
            }
        }
        assert_eq!(map.find(42, |_| true), 0);
        map.insert_hashed(42, count + 1);
        assert_eq!(map.find(42, |_| true), count + 1);
    }
}
