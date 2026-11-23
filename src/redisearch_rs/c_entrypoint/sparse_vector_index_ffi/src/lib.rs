/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C bindings for the sparse vector index.
//!
//! The metric crosses the boundary as the `u32` discriminant of
//! [`SparseMetric`]. Indexing is split in two so that the parsing and
//! validation can run before the spec write lock is taken:
//! [`PreparedSparseVector_FromBlob`] checks and normalizes a document's vector,
//! and [`SparseVectorIndex_Add`] writes it under the lock.

use std::{ffi::c_char, ptr::NonNull};

use ffi::t_docId;
use redis_reply::RedisModuleCtx;
use sparse_vector_index::{
    SparseMetric, SparseVectorAddOutcome, SparseVectorIndex, SparseVectorIndexStats,
};

/// A document's sparse vector, checked against its field and normalized
/// under `COSINE`, ready for [`SparseVectorIndex_Add`].
pub struct PreparedSparseVector(Vec<(u32, f32)>);

fn metric_from_u32(metric: u32) -> SparseMetric {
    SparseMetric::try_from(metric).expect("C passes a metric the schema validated")
}

/// Create an empty index for a field of `dim` dimensions ranked by `metric`.
///
/// Free it with [`SparseVectorIndex_Free`].
///
/// # Panics
///
/// Panics when `metric` is not a [`SparseMetric`] discriminant.
#[unsafe(no_mangle)]
pub extern "C" fn SparseVectorIndex_New(dim: u64, metric: u32) -> *mut SparseVectorIndex {
    Box::into_raw(Box::new(SparseVectorIndex::new(
        dim,
        metric_from_u32(metric),
    )))
}

/// Free an index built by [`SparseVectorIndex_New`]. A null `index` is a no-op.
///
/// # Safety
///
/// 1. `index` is null, or was returned by [`SparseVectorIndex_New`] and not
///    freed yet.
/// 2. No query iterator over `index` is alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SparseVectorIndex_Free(index: *mut SparseVectorIndex) {
    if !index.is_null() {
        // SAFETY: guaranteed by 1; 2 makes dropping the posting lists sound.
        drop(unsafe { Box::from_raw(index) });
    }
}

/// The index's size, for `FT.INFO` and `INFO MODULES`.
///
/// # Safety
///
/// `index` is a valid pointer returned by [`SparseVectorIndex_New`], not
/// written concurrently (the caller holds the spec lock).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SparseVectorIndex_Stats(
    index: *const SparseVectorIndex,
) -> SparseVectorIndexStats {
    // SAFETY: guaranteed by the caller.
    unsafe { &*index }.stats()
}

/// Reply to `FT.DEBUG INFO_SPARSEIDX` with the field's size and its `top`
/// longest dimensions. A null `index` (no document written yet) replies as
/// an empty index.
///
/// # Safety
///
/// 1. `ctx` is a valid Redis module context.
/// 2. `index` is null, or a valid pointer returned by [`SparseVectorIndex_New`]
///    and not written concurrently (the caller holds the spec lock).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SparseVectorIndex_DebugInfo(
    ctx: *mut RedisModuleCtx,
    index: *const SparseVectorIndex,
    top: usize,
) {
    let ctx = NonNull::new(ctx).expect("ctx cannot be NULL");
    // SAFETY: guaranteed by 2.
    let index = unsafe { index.as_ref() };
    // SAFETY: guaranteed by 1.
    unsafe { sparse_vector_index::debug::debug_info(ctx, index, top) }
}

/// Reply to `FT.DEBUG DUMP_SPARSEIDX` with `dim_id`'s posting list. A null
/// `index` (no document written yet) replies with an empty array.
///
/// # Safety
///
/// As [`SparseVectorIndex_DebugInfo`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SparseVectorIndex_DebugDumpDimension(
    ctx: *mut RedisModuleCtx,
    index: *const SparseVectorIndex,
    dim_id: u32,
) {
    let ctx = NonNull::new(ctx).expect("ctx cannot be NULL");
    // SAFETY: guaranteed by 2 of [`SparseVectorIndex_DebugInfo`].
    let index = unsafe { index.as_ref() };
    // SAFETY: guaranteed by 1 of [`SparseVectorIndex_DebugInfo`].
    unsafe { sparse_vector_index::debug::debug_dump_dimension(ctx, index, dim_id) }
}

/// Decode a `(u32 dim_id, f32 weight)` little-endian blob and check it
/// against a field of `dim` dimensions ranked by `metric`.
///
/// Returns null when the vector is rejected, after writing a NUL-terminated
/// reason into `err`, truncated to `err_len` bytes. Free a non-null result
/// with [`PreparedSparseVector_Free`].
///
/// # Safety
///
/// 1. `blob` is valid for reads of `len` bytes, or `len` is zero.
/// 2. `err` is valid for writes of `err_len` bytes, or `err_len` is zero.
///
/// # Panics
///
/// Panics when `metric` is not a [`SparseMetric`] discriminant.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn PreparedSparseVector_FromBlob(
    blob: *const u8,
    len: usize,
    dim: u64,
    metric: u32,
    err: *mut c_char,
    err_len: usize,
) -> *mut PreparedSparseVector {
    let bytes = if len == 0 {
        &[][..]
    } else {
        // SAFETY: guaranteed by 1.
        unsafe { std::slice::from_raw_parts(blob, len) }
    };
    match sparse_vector_index::parse_blob(bytes)
        .and_then(|entries| sparse_vector_index::prepare(entries, dim, metric_from_u32(metric)))
    {
        Ok(entries) => Box::into_raw(Box::new(PreparedSparseVector(entries))),
        Err(e) => {
            if err_len > 0 {
                // SAFETY: guaranteed by 2.
                let buf = unsafe { std::slice::from_raw_parts_mut(err.cast::<u8>(), err_len) };
                let msg = e.to_string();
                let n = msg.len().min(err_len - 1);
                buf[..n].copy_from_slice(&msg.as_bytes()[..n]);
                buf[n] = 0;
            }
            std::ptr::null_mut()
        }
    }
}

/// Free a vector from [`PreparedSparseVector_FromBlob`]. A null `vector` is a
/// no-op.
///
/// # Safety
///
/// `vector` is null, or was returned by [`PreparedSparseVector_FromBlob`] and
/// not freed yet.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn PreparedSparseVector_Free(vector: *mut PreparedSparseVector) {
    if !vector.is_null() {
        // SAFETY: guaranteed by the caller.
        drop(unsafe { Box::from_raw(vector) });
    }
}

/// Add a document's prepared vector to `index`.
///
/// Returns what the write changed, for the spec's statistics.
///
/// # Safety
///
/// 1. `index` is a valid pointer returned by [`SparseVectorIndex_New`], held
///    exclusively (under the spec write lock).
/// 2. `vector` is a valid pointer returned by [`PreparedSparseVector_FromBlob`]
///    for the same field's `DIM` and metric.
/// 3. `doc_id` is greater than every doc ID added to `index` before.
///
/// # Panics
///
/// Panics when the posting list cannot be written, which only an allocation
/// failure causes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SparseVectorIndex_Add(
    index: *mut SparseVectorIndex,
    doc_id: t_docId,
    vector: *const PreparedSparseVector,
) -> SparseVectorAddOutcome {
    // SAFETY: guaranteed by 1.
    let index = unsafe { &mut *index };
    // SAFETY: guaranteed by 2.
    let vector = unsafe { &*vector };
    index
        .add(doc_id, &vector.0)
        .expect("writing a posting list to memory cannot fail")
}
