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

use crate::Evaluated;

/// `QN_IDS` — filter by explicit document key names.
///
/// Resolving a key to a docId opens the Redis key (which needs the GIL), so it
/// happens on the main thread during query construction — see
/// `applyGlobalFilters` in `aggregate_request.c`. This only consumes the
/// resulting `doc_ids`, sorts, deduplicates, and builds a sorted
/// [`IdListSorted`] iterator.
///
/// * `keys` — the key names to match against, borrowing the request's held
///   argv; kept only for the length invariant against `doc_ids`.
/// * `doc_ids` — document IDs positionally matching `keys`, where `0` denotes
///   an unresolved (not indexed) key and is filtered out.
pub(crate) fn eval<'index>(
    keys: &[*mut redis_module::raw::RedisModuleString],
    doc_ids: Option<&[DocId]>,
) -> Evaluated<'index> {
    debug_assert!(
        doc_ids.is_some(),
        "QN_IDS nodes must carry pre-resolved doc_ids"
    );
    debug_assert!(
        doc_ids.is_none_or(|d| d.len() == keys.len()),
        "doc_ids and keys must have the same length"
    );

    // The fallback only keeps release builds from panicking if a caller
    // violates the invariant above.
    let mut ids: Vec<DocId> = doc_ids
        .unwrap_or(&[])
        .iter()
        .copied()
        .filter(|&did| did != 0)
        .collect();

    if !ids.is_empty() {
        ids.sort_unstable();
        ids.dedup();
    }

    Evaluated::RustLeaf(Box::new(IdListSorted::with_result(
        ids,
        RSIndexResult::build_virt().weight(1.0).build(),
    )))
}
