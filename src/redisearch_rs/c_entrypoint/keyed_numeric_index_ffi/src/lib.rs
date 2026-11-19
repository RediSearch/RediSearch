/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bindings for [`KeyedNumericIndex`].
//!
//! # Index ownership
//!
//! [`KeyedNumericIndex_New`] allocates the index as a [`Box`] and hands C the
//! raw pointer; C releases it with [`KeyedNumericIndex_Free`].

use numeric_range_tree::{AddResult, NumericRangeTree};
use rqe_core::DocId;

pub use keyed_numeric_index::{KeyedNumericBatch, KeyedNumericIndex};

/// Create an empty index. Release it with [`KeyedNumericIndex_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn KeyedNumericIndex_New(compress_floats: bool) -> *mut KeyedNumericIndex {
    Box::into_raw(Box::new(KeyedNumericIndex::new(compress_floats)))
}

/// Free an index and all its trees.
///
/// # Safety
///
/// 1. `index` must be NULL (a no-op), or a pointer returned by
///    [`KeyedNumericIndex_New`] that was not freed yet.
/// 2. Nothing may still borrow from the index.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_Free(index: *mut KeyedNumericIndex) {
    if index.is_null() {
        return;
    }
    // SAFETY: 1 (non-null, so it came from `Box::into_raw`), 2.
    drop(unsafe { Box::from_raw(index) });
}

/// Add (`doc_id`, `value`) under the key of `key_len` bytes at `key`.
///
/// Returns the change to the key's tree, as [`NumericRangeTree::add`] does. The result
/// excludes the new key and the tree's base size, which only
/// [`KeyedNumericIndex_MemoryUsage`] reflects.
///
/// A document must be added at most once per key; a repeat is dropped, as are an
/// out-of-order `doc_id` and a `NaN` value. See [`KeyedNumericIndex::add`].
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. The caller must have unique access to `index`: no other reference to it, and no
///    pointer from [`KeyedNumericIndex_Get`] being dereferenced, while this runs.
/// 3. `key` must be valid for reads of `key_len` bytes, or may be NULL if `key_len` is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_Add(
    index: *mut KeyedNumericIndex,
    doc_id: DocId,
    key: *const u8,
    key_len: usize,
    value: f64,
    has_field_expiration: bool,
    max_depth_range: usize,
) -> AddResult {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    // SAFETY: 1, 2.
    let index = unsafe { &mut *index };
    // SAFETY: 3.
    let key = unsafe { key_bytes(key, key_len) };
    index.add(doc_id, key, value, has_field_expiration, max_depth_range)
}

/// Create an empty batch. Release it with [`KeyedNumericBatch_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn KeyedNumericBatch_New() -> *mut KeyedNumericBatch {
    Box::into_raw(Box::new(KeyedNumericBatch::new()))
}

/// Free a batch.
///
/// # Safety
///
/// `batch` must be NULL (a no-op), or a pointer returned by [`KeyedNumericBatch_New`]
/// that was not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericBatch_Free(batch: *mut KeyedNumericBatch) {
    if batch.is_null() {
        return;
    }
    // SAFETY: the caller guarantees `batch` came from `Box::into_raw` and is live.
    drop(unsafe { Box::from_raw(batch) });
}

/// Append the member (`key`, `value`) to a batch. See [`KeyedNumericBatch::push`].
///
/// # Safety
///
/// 1. `batch` must point to a valid [`KeyedNumericBatch`] and cannot be NULL.
/// 2. `key` must be valid for reads of `key_len` bytes, or may be NULL if `key_len` is 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericBatch_Push(
    batch: *mut KeyedNumericBatch,
    key: *const u8,
    key_len: usize,
    value: f64,
) {
    debug_assert!(!batch.is_null(), "batch cannot be NULL");
    // SAFETY: 1.
    let batch = unsafe { &mut *batch };
    // SAFETY: 2.
    let key = unsafe { key_bytes(key, key_len) };
    batch.push(key, value);
}

/// The number of members in a batch.
///
/// # Safety
///
/// `batch` must point to a valid [`KeyedNumericBatch`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericBatch_Len(batch: *const KeyedNumericBatch) -> usize {
    debug_assert!(!batch.is_null(), "batch cannot be NULL");
    // SAFETY: the caller guarantees `batch` is valid.
    unsafe { &*batch }.len()
}

/// Add every member of `batch` for `doc_id`. See [`KeyedNumericIndex::add_batch`].
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. The caller must have unique access to `index`, as for [`KeyedNumericIndex_Add`].
/// 3. `batch` must point to a valid [`KeyedNumericBatch`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_AddBatch(
    index: *mut KeyedNumericIndex,
    doc_id: DocId,
    batch: *const KeyedNumericBatch,
    has_field_expiration: bool,
    max_depth_range: usize,
) -> AddResult {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    debug_assert!(!batch.is_null(), "batch cannot be NULL");
    // SAFETY: 1, 2.
    let index = unsafe { &mut *index };
    // SAFETY: 3.
    let batch = unsafe { &*batch };
    index.add_batch(doc_id, batch, has_field_expiration, max_depth_range)
}

/// The tree of the key of `key_len` bytes at `key`, or NULL if the key has none.
///
/// The address does not identify the tree: after its key is removed and re-added, another
/// tree may live at the same address. A caller that releases the spec lock must look the
/// key up again instead of trusting the pointer.
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. No `&mut` access to `index` may exist while this runs: the caller holds at least
///    the spec read lock.
/// 3. `key` must be valid for reads of `key_len` bytes, or may be NULL if `key_len` is 0.
/// 4. The returned pointer may only be dereferenced while that lock is still held. Any
///    mutator run under the write lock, such as [`KeyedNumericIndex_Add`] or key removal,
///    may change the tree behind it or free it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_Get(
    index: *const KeyedNumericIndex,
    key: *const u8,
    key_len: usize,
) -> *const NumericRangeTree {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    // SAFETY: 1, 2.
    let index = unsafe { &*index };
    // SAFETY: 3.
    let key = unsafe { key_bytes(key, key_len) };
    index
        .get(key)
        .map_or(std::ptr::null(), |tree| tree as *const _)
}

/// The number of keys that have a tree.
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. No `&mut` access to `index` may exist while this runs: the caller holds at least
///    the spec read lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_NumKeys(index: *const KeyedNumericIndex) -> usize {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    // SAFETY: 1, 2.
    unsafe { &*index }.num_keys()
}

/// Bytes used by the index, its keys, their trees and the map's table.
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. No `&mut` access to `index` may exist while this runs: the caller holds at least
///    the spec read lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_MemoryUsage(index: *const KeyedNumericIndex) -> usize {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    // SAFETY: 1, 2.
    unsafe { &*index }.memory_usage()
}

/// Identifies this index instance among all that were ever created.
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. No `&mut` access to `index` may exist while this runs: the caller holds at least
///    the spec read lock.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericIndex_UniqueId(index: *const KeyedNumericIndex) -> u32 {
    debug_assert!(!index.is_null(), "index cannot be NULL");
    // SAFETY: 1, 2.
    unsafe { &*index }.unique_id().into()
}

/// View a C `(pointer, length)` pair as a slice, accepting NULL for an empty key.
///
/// # Safety
///
/// `key` must be valid for reads of `key_len` bytes, or may be NULL if `key_len` is 0.
const unsafe fn key_bytes<'a>(key: *const u8, key_len: usize) -> &'a [u8] {
    if key_len == 0 {
        return &[];
    }
    // SAFETY: `key_len` is non-zero, so `key` is valid for `key_len` reads by the contract above.
    unsafe { std::slice::from_raw_parts(key, key_len) }
}
