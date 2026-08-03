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
/// Resolves each key to a document ID, sorts, deduplicates, and creates a
/// sorted [`IdListSorted`] iterator.
///
/// * `keys` — SDS key strings from the query node.  When `doc_ids` is
///   `None`, each key is looked up in the [`DocTable`](ffi::DocTable) to
///   obtain its document ID.
/// * `doc_ids` — when present (search-on-disk mode), contains pre-resolved
///   document IDs positionally matching `keys`, bypassing the `DocTable`
///   lookup.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    keys: &[ffi::sds],
    doc_ids: Option<&[DocId]>,
) -> Evaluated<'index> {
    // Pre-resolved `doc_ids` are only produced on the search-on-disk path, so
    // they must be accompanied by a non-null `spec.diskSpec`. Guard the
    // invariant.
    debug_assert!(
        doc_ids.is_none() || !ctx.spec().diskSpec.is_null(),
        "pre-resolved doc_ids requires search-on-disk to be enabled"
    );

    // When pre-resolved, `doc_ids` is consumed directly and must line up
    // positionally with `keys` (they describe the same id-filter node).
    debug_assert!(
        doc_ids.is_none_or(|d| d.len() == keys.len()),
        "doc_ids and keys must have the same length"
    );

    let mut ids: Vec<DocId> = match doc_ids {
        // Search-on-disk: ids are already resolved, just drop the misses.
        Some(resolved) => resolved.iter().copied().filter(|&did| did != 0).collect(),
        // In-memory: resolve each key to a doc id through the `DocTable`.
        None => keys
            .iter()
            .filter_map(|&key| {
                // SAFETY: `key` is a valid SDS string (guaranteed by
                // `QueryNodeRef`); `sdslen_rust` reads its header.
                let key_len = unsafe { ffi::sdslen_rust(key) };
                // SAFETY: `doc_table()` returns a valid `DocTable` reference
                // (`QueryEvalContext` invariant).
                let did = unsafe { ffi::DocTable_GetId(ctx.doc_table(), key, key_len) };
                (did != 0).then_some(did)
            })
            .collect(),
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
