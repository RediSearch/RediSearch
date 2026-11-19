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
//!
//! # Batch ownership
//!
//! [`KeyedNumericBatch_FromJson`] allocates a batch the same way; C releases it with
//! [`KeyedNumericBatch_Free`].

use numeric_range_tree::AddResult;
use redis_json_api::{JsonValueRef, RedisJsonApi};
use rqe_core::DocId;
use std::ptr::NonNull;

pub use keyed_numeric_index::{BatchFromJsonError, KeyedNumericBatch, KeyedNumericIndex};

/// Why [`KeyedNumericBatch_FromJson`] returned NULL.
#[cheadergen::config(export, prefix_with_name)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyedNumericBatchError {
    /// The RedisJSON API is not available.
    ApiUnavailable = 0,
    /// The value is not a JSON object.
    NotAnObject = 1,
    /// A member is not a number.
    MemberNotNumber = 2,
}

impl From<BatchFromJsonError> for KeyedNumericBatchError {
    fn from(err: BatchFromJsonError) -> Self {
        match err {
            BatchFromJsonError::NotAnObject => Self::NotAnObject,
            BatchFromJsonError::MemberNotNumber => Self::MemberNotNumber,
        }
    }
}

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

/// Collect the members of the JSON object `obj` into a new batch.
///
/// Returns NULL and sets `err` if the value cannot be collected; see
/// [`KeyedNumericBatch::from_json`]. Release a batch with [`KeyedNumericBatch_Free`].
///
/// # Safety
///
/// 1. `obj` must be a valid, non-null `RedisJSON` value, and the RedisJSON module must be
///    initialized.
/// 2. `ctx` must be a valid, non-null Redis module context.
/// 3. `err` must be valid for writes and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericBatch_FromJson(
    obj: ffi::RedisJSON,
    ctx: *mut redis_module::RedisModuleCtx,
    err: *mut KeyedNumericBatchError,
) -> *mut KeyedNumericBatch {
    debug_assert!(!err.is_null(), "err cannot be NULL");
    let fail = |e| {
        // SAFETY: 3.
        unsafe { err.write(e) };
        std::ptr::null_mut()
    };
    // SAFETY: 1.
    let Some(api) = (unsafe { RedisJsonApi::get() }) else {
        return fail(KeyedNumericBatchError::ApiUnavailable);
    };
    let obj = NonNull::new(obj.cast_mut()).expect("obj cannot be NULL");
    let ctx = NonNull::new(ctx).expect("ctx cannot be NULL");
    // SAFETY: 1 (`obj` came from the RedisJSON API).
    let obj = unsafe { JsonValueRef::from_raw(obj, &api) };
    // SAFETY: 2.
    match unsafe { KeyedNumericBatch::from_json(obj, ctx) } {
        Ok(batch) => Box::into_raw(Box::new(batch)),
        Err(e) => fail(e.into()),
    }
}

/// Free a batch.
///
/// # Safety
///
/// `batch` must be NULL (a no-op), or a pointer returned by [`KeyedNumericBatch_FromJson`]
/// that was not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn KeyedNumericBatch_Free(batch: *mut KeyedNumericBatch) {
    if batch.is_null() {
        return;
    }
    // SAFETY: the caller guarantees `batch` came from `Box::into_raw` and is live.
    drop(unsafe { Box::from_raw(batch) });
}

/// Add every member of `batch` for `doc_id`. See [`KeyedNumericIndex::add_batch`].
///
/// # Safety
///
/// 1. `index` must point to a valid [`KeyedNumericIndex`] and cannot be NULL.
/// 2. The caller must have unique access to `index`: no other reference to it while this runs.
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
