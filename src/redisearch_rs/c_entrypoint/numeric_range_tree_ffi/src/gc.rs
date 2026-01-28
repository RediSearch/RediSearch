/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for garbage collection on numeric inverted indexes.

use ffi::{DocTable_Exists, RedisSearchCtx};
use inverted_index::{GcApplyInfo, GcScanDelta, IndexBlock, RSIndexResult};
use serde::Serialize;

use crate::{
    IndexRepairParams, InvertedIndexGCCallback, InvertedIndexGCWriter, InvertedIndexNumeric,
};

/// Scan a numeric inverted index for garbage collection.
///
/// This scans the index for deleted documents and computes the GC deltas.
/// If there are deltas, the callback `cb` is called, then the deltas are
/// serialized to the writer `wr`.
///
/// Returns `true` if GC work was found and written, `false` otherwise.
///
/// # Safety
///
/// - `wr` must point to a valid [`InvertedIndexGCWriter`] and cannot be NULL.
/// - `sctx` must point to a valid [`RedisSearchCtx`] and cannot be NULL.
/// - `idx` must point to a valid [`InvertedIndexNumeric`] and cannot be NULL.
/// - `cb` must point to a valid [`InvertedIndexGCCallback`] and cannot be NULL.
/// - `params` may be NULL for no repair callback, or must point to a valid [`IndexRepairParams`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_GcDelta_Scan(
    wr: *mut InvertedIndexGCWriter,
    sctx: *mut RedisSearchCtx,
    idx: *const InvertedIndexNumeric,
    cb: *mut InvertedIndexGCCallback,
    params: *mut IndexRepairParams,
) -> bool {
    debug_assert!(!wr.is_null(), "wr cannot be NULL");
    debug_assert!(!sctx.is_null(), "sctx cannot be NULL");
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    debug_assert!(!cb.is_null(), "cb cannot be NULL");

    // SAFETY: sctx is a valid pointer
    let sctx_ref = unsafe { &*sctx };
    debug_assert!(!sctx_ref.spec.is_null(), "sctx.spec cannot be NULL");

    // SAFETY: spec is valid
    let spec = unsafe { &*sctx_ref.spec };
    let doc_table = spec.docs;

    // SAFETY: doc_table is valid from spec
    let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };

    // SAFETY: idx is a valid pointer to InvertedIndexNumeric
    let idx = unsafe { &*idx };

    // Get repair callback if present
    let repair_info = if params.is_null() {
        None
    } else {
        // SAFETY: params is valid if non-NULL
        let params_ref = unsafe { &*params };
        params_ref
            .repair_callback
            .map(|cb| (cb, params_ref.repair_arg))
    };

    let deltas = match (idx, repair_info) {
        (InvertedIndexNumeric::Uncompressed(entries), None) => {
            entries.scan_gc(doc_exists, None::<fn(&RSIndexResult<'_>, &IndexBlock)>)
        }
        (InvertedIndexNumeric::Uncompressed(entries), Some((repair_cb, repair_arg))) => entries
            .scan_gc(
                doc_exists,
                Some(move |res: &RSIndexResult<'_>, ib: &IndexBlock| {
                    repair_cb(res, ib, repair_arg)
                }),
            ),
        (InvertedIndexNumeric::Compressed(entries), None) => {
            entries.scan_gc(doc_exists, None::<fn(&RSIndexResult<'_>, &IndexBlock)>)
        }
        (InvertedIndexNumeric::Compressed(entries), Some((repair_cb, repair_arg))) => entries
            .scan_gc(
                doc_exists,
                Some(move |res: &RSIndexResult<'_>, ib: &IndexBlock| {
                    repair_cb(res, ib, repair_arg)
                }),
            ),
    };

    let Ok(deltas) = deltas else {
        return false;
    };

    let Some(deltas) = deltas else {
        return false;
    };

    // SAFETY: cb is a valid pointer
    let cb_ref = unsafe { &*cb };
    let cb_call = cb_ref.call;
    cb_call(cb_ref.ctx);

    // SAFETY: wr is a valid pointer
    let wr_ref = unsafe { &mut *wr };

    deltas
        .serialize(&mut rmp_serde::Serializer::new(wr_ref))
        .is_ok()
}

/// Apply GC deltas to a numeric inverted index.
///
/// This applies the garbage collection changes computed by a scan to the index,
/// freeing deleted entries and compacting the index.
///
/// The `deltas` pointer is consumed by this function - do NOT call
/// `InvertedIndex_GcDelta_Free` on it afterward.
///
/// # Safety
///
/// - `idx` must point to a valid mutable [`InvertedIndexNumeric`] and cannot be NULL.
/// - `deltas` must point to a valid [`GcScanDelta`] created by `InvertedIndex_GcDelta_Read`.
/// - `apply_info` must point to a valid [`GcApplyInfo`] struct to receive results.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_ApplyGcDelta(
    idx: *mut InvertedIndexNumeric,
    deltas: *mut GcScanDelta,
    apply_info: *mut GcApplyInfo,
) {
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    debug_assert!(!deltas.is_null(), "deltas cannot be NULL");
    debug_assert!(!apply_info.is_null(), "apply_info cannot be NULL");

    // SAFETY: idx is a valid pointer to InvertedIndexNumeric
    let idx = unsafe { &mut *idx };

    // SAFETY: deltas was created by InvertedIndex_GcDelta_Read and is valid
    let deltas = unsafe { Box::from_raw(deltas) };

    let info = match idx {
        InvertedIndexNumeric::Uncompressed(entries) => entries.apply_gc(*deltas),
        InvertedIndexNumeric::Compressed(entries) => entries.apply_gc(*deltas),
    };

    // SAFETY: apply_info is a valid pointer
    unsafe { *apply_info = info };
}
