/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_IDS` query nodes.

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::id_list::IdListSorted;

use crate::{Evaluated, QueryEvalContext};

/// `QN_IDS` — filter by explicit document key names.
///
/// Deferred keys are resolved through the lookup map under the execution
/// spec lock once ready; before that, evaluation uses metadata with the GIL held.
/// Disk mode supplies IDs before evaluation.
/// Both paths sort, deduplicate, and build an [`IdListSorted`] iterator.
///
/// * `keys` — the key names to match against, borrowing the request's held
///   argv.
/// * `doc_ids` — document IDs positionally matching `keys`, where `0` denotes
///   an unresolved (not indexed) key and is filtered out.
pub(crate) fn eval<'index>(
    ctx: &QueryEvalContext,
    keys: &[*mut redis_module::raw::RedisModuleString],
    doc_ids: Option<&[DocId]>,
) -> Evaluated<'index> {
    debug_assert!(
        doc_ids.is_none_or(|d| d.len() == keys.len()),
        "doc_ids and keys must have the same length"
    );

    let mut ids: Vec<DocId> = if let Some(doc_ids) = doc_ids {
        doc_ids.iter().copied().filter(|&did| did != 0).collect()
    } else if ctx.doc_table().keyMapState == ffi::DocKeyMapState_DocKeyMap_Ready {
        resolve_with_keymap(ctx, keys)
    } else {
        resolve_with_metadata(ctx, keys)
    };

    if !ids.is_empty() {
        ids.sort_unstable();
        ids.dedup();
    }

    Evaluated::RustLeaf(Box::new(IdListSorted::with_result(
        ids,
        RSIndexResult::build_virt().weight(1.0).build(),
    )))
}

/// Resolve through metadata with both the GIL and the spec read lock held,
/// as required by [`QueryEvalContext::new`].
fn resolve_with_metadata(
    ctx: &QueryEvalContext,
    keys: &[*mut redis_module::raw::RedisModuleString],
) -> Vec<DocId> {
    keys.iter()
        .filter_map(|&key| {
            let mut id = 0;
            // SAFETY: fallback evaluation holds the GIL before the spec read lock;
            // the context and borrowed argv remain valid for this call.
            unsafe {
                ffi::DocIdMeta_Get(ctx.sctx().redisCtx, key.cast(), ctx.spec().specId, &mut id)
            };
            (id != 0).then_some(id)
        })
        .collect()
}

/// Resolve through the ready map under the spec read lock, which pins the map,
/// candidate metadata, and keys as required by [`QueryEvalContext::new`].
fn resolve_with_keymap(
    ctx: &QueryEvalContext,
    keys: &[*mut redis_module::raw::RedisModuleString],
) -> Vec<DocId> {
    let table = ctx.doc_table();
    debug_assert!(!table.keyMap.is_null());
    // SAFETY: the spec read lock pins the map and excludes mutation throughout evaluation.
    let map = unsafe { &*table.keyMap.cast::<doc_key_map_ffi::DocKeyMap>() };
    // SAFETY: installed once at module load and never mutated during evaluation.
    let string_ptr_len =
        unsafe { redis_module::RedisModule_StringPtrLen }.expect("RedisModule_StringPtrLen unset");
    keys.iter()
        .filter_map(|&key| {
            debug_assert!(!key.is_null());
            let mut len = 0;
            // SAFETY: unresolved ID nodes borrow live Redis strings from the
            // request's held argv (QueryNodeMut::new invariant 6).
            let bytes = unsafe { string_ptr_len(key, &mut len) };
            // SAFETY: StringPtrLen returned readable bytes from the held argv string.
            let key_bytes = unsafe { std::slice::from_raw_parts(bytes.cast::<u8>(), len) };
            let id = map.get_id(key_bytes, |id| {
                // SAFETY: the spec read lock pins candidate DMDs and their keys;
                // the query bytes remain live throughout this comparison.
                unsafe { ffi::DocTable_KeyEquals(table, id, bytes, len) }
            });
            (id != 0).then_some(id)
        })
        .collect()
}
