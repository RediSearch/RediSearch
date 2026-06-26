/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Main query evaluation dispatcher.
//!
//! Converts a parsed query AST node into an executable iterator tree by
//! dispatching on the [`QueryNodeType`](query_node_type::QueryNodeType) discriminant.

use std::ptr::NonNull;

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::{
    Empty, c2rust::CRQEIterator, id_list::IdListSorted, interop::RQEIteratorWrapper,
    inverted_index::new_missing_iterator,
};

use crate::{QueryEvalContext, QueryNode, QueryNodeRef};

/// The return type of [`eval_node`]: a boxed Rust iterator that implements
/// both [`RQEIterator`](rqe_iterators::RQEIterator) and
/// [`ProfilePrint`](rqe_iterators::profile_print::ProfilePrint).
pub type EvalResult<'index> = rqe_iterators::BoxedRQEIterator<'index>;

/// The outcome of evaluating a query node.
///
/// A node is either built by Rust ([`Evaluated::Rust`]) or, while the dispatcher
/// is only partially ported, by the C `Query_EvalNode` ([`Evaluated::C`]).
/// Keeping the two apart means a C iterator can be handed straight back to C —
/// without an intermediate Rust wrapper that would hide it from the C-side
/// optimizer/profiler, and without a throwaway heap allocation.
// TODO: Remove this enum once all the nodes types have been ported to Rust
// and C `Query_EvalNode` has been removed.
#[must_use = "an unconsumed `Evaluated::C` leaks its owning C iterator; consume it via `into_c_iterator` or `into_boxed`"]
pub enum Evaluated<'index> {
    /// An iterator implemented in Rust.
    Rust(EvalResult<'index>),
    /// An owning C iterator handle returned by [`ffi::Query_EvalNode`].
    C(NonNull<ffi::QueryIterator>),
}

impl<'index> Evaluated<'index> {
    /// Consume into an owning C [`QueryIterator`](ffi::QueryIterator) pointer.
    ///
    /// A Rust iterator is wrapped via [`RQEIteratorWrapper::boxed_new`]; a C
    /// iterator is returned as-is, so C-side introspection (optimizer, profiler)
    /// keeps seeing the original C iterator.
    pub fn into_c_iterator(self) -> *mut ffi::QueryIterator {
        match self {
            Evaluated::Rust(it) => RQEIteratorWrapper::boxed_new(it),
            Evaluated::C(it) => it.as_ptr(),
        }
    }

    /// Consume into a boxed Rust iterator, wrapping a C iterator in a
    /// [`CRQEIterator`] shim so it satisfies the Rust iterator trait.
    ///
    /// Used by Rust consumers that compose evaluated children as trait objects.
    pub fn into_boxed(self) -> EvalResult<'index> {
        match self {
            Evaluated::Rust(it) => it,
            // SAFETY: `Evaluated::C` always holds a valid, owning `QueryIterator`
            // with all required callbacks populated (it came from
            // `ffi::Query_EvalNode`) — exactly the preconditions of
            // `CRQEIterator::new`.
            Evaluated::C(it) => rqe_iterators::BoxedRQEIterator::new(Box::new(unsafe {
                CRQEIterator::new(it)
            })),
        }
    }
}

/// Build the executable iterator tree for a parsed query AST.
///
/// The `root` node is evaluated via [`eval_node`]. When evaluation yields no
/// iterator (`None`), an [`Empty`] iterator is returned.
pub fn qast_iterate<'index>(
    ctx: &'index mut QueryEvalContext,
    root: &QueryNodeRef,
) -> Evaluated<'index> {
    eval_node(ctx, root)
        .unwrap_or_else(|| Evaluated::Rust(rqe_iterators::BoxedRQEIterator::new(Box::new(Empty))))
}

/// Evaluate a single query node, producing the corresponding iterator.
///
/// Returns `None` when the node produces no results.
pub fn eval_node<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Option<Evaluated<'index>> {
    match node.as_enum() {
        QueryNode::Null => Some(Evaluated::Rust(eval_null())),
        QueryNode::Wildcard => Some(Evaluated::Rust(eval_wildcard(ctx, node))),
        QueryNode::Ids { keys, doc_ids } => Some(Evaluated::Rust(eval_ids(ctx, keys, doc_ids))),
        QueryNode::Missing { field } => eval_missing(ctx, field).map(Evaluated::Rust),
        // Node types not yet ported to Rust are delegated back to the C
        // dispatcher.
        _ => eval_node_c(ctx, node),
    }
}

/// Evaluate a not-yet-ported node by delegating to the C [`ffi::Query_EvalNode`]
/// dispatcher, returning its C iterator as [`Evaluated::C`].
///
/// Returns `None` when `Query_EvalNode` produces no iterator (NULL), preserving
/// the C semantics where some nodes (e.g. an empty expansion) yield no results.
fn eval_node_c<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Option<Evaluated<'index>> {
    let q = ctx.as_non_null().as_ptr();
    let n = node.as_non_null().as_ptr();
    // SAFETY: `q` comes from a live `QueryEvalContext` (a valid `QueryEvalCtx`
    // with exclusive access, since `ctx` is `&mut`) and `n` from a live
    // `QueryNodeRef` (a valid `RSQueryNode`), satisfying `Query_EvalNode`'s
    // contract.
    let it = unsafe { ffi::Query_EvalNode(q, n) };
    NonNull::new(it).map(Evaluated::C)
}

/// `QN_NULL` — stopword queries produce an empty iterator.
fn eval_null<'index>() -> EvalResult<'index> {
    rqe_iterators::BoxedRQEIterator::new(Box::new(Empty))
}

/// `QN_WILDCARD` — the `*` query that matches every document.
fn eval_wildcard<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> EvalResult<'index> {
    let weight = node.opts().weight;
    // SAFETY: `new_wildcard_iterator` preconditions map to
    // `QueryEvalContext::new` invariants as follows:
    // 1. `query` is a valid `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is a valid, non-null `RedisSearchCtx` — invariant (2).
    // 3. `query.sctx.spec` is a valid, non-null `IndexSpec` — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1),
    //    a properly initialised `QueryEvalCtx` is built from a valid spec.
    // 5. `new_wildcard_iterator_optimized` preconditions hold when
    //    `rule.index_all` is true — the spec's `existingDocs` inverted
    //    index is initialised during `IndexSpec_Init`.
    // 6. `query.docTable` is a valid, non-null `DocTable` — invariant (2).
    // 7. `spec.diskSpec`, when non-null, is a valid
    //    `RedisSearchDiskIndexSpec` — part of (1).
    // 8. `SEARCH_ENTERPRISE_ITERATORS` is initialised when `diskSpec` is
    //    non-null — the enterprise module sets it during `OnLoad`.
    let it = unsafe { rqe_iterators::wildcard::new_wildcard_iterator(ctx.as_non_null(), weight) };
    rqe_iterators::BoxedRQEIterator::new(Box::new(it))
}

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
fn eval_ids<'index>(
    ctx: &'index mut QueryEvalContext,
    keys: &[ffi::sds],
    doc_ids: Option<&[DocId]>,
) -> EvalResult<'index> {
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

    rqe_iterators::BoxedRQEIterator::new(Box::new(IdListSorted::with_result(
        ids,
        RSIndexResult::build_virt().weight(1.0).build(),
    )))
}

/// `QN_MISSING` — matches documents where a field has no indexed value.
fn eval_missing<'index>(
    ctx: &'index mut QueryEvalContext,
    fs: &ffi::FieldSpec,
) -> Option<EvalResult<'index>> {
    let spec = ctx.spec();

    // SAFETY: `spec` is valid (`QueryEvalContext::new` invariant 2), and any
    // queryable spec has its `missingFieldDict` initialised by
    // `IndexSpec_MakeKeyless`, so the pointer is a valid dict; `fs.fieldName`
    // is a valid `HiddenString` key, matching the C `Query_EvalMissingNode`.
    let ii_ptr = unsafe { ffi::RS_dictFetchValue(spec.missingFieldDict, fs.fieldName as *mut _) };

    if ii_ptr.is_null() {
        // There are no missing values for this field.
        return None;
    }

    let ii_ptr: *const inverted_index::opaque::InvertedIndex = ii_ptr.cast();
    // SAFETY: `ii_ptr` is a valid `InvertedIndex` obtained from the
    // missing-field dict (non-null checked above).
    let ii_ref = unsafe { &*ii_ptr };

    // `ctx.sctx()` is a live reference, so the resulting pointer is never null.
    let sctx_nn = NonNull::from(ctx.sctx());

    // SAFETY: `new_missing_iterator`'s four preconditions hold here:
    // 1. `sctx` is a valid `RedisSearchCtx` with a non-null, valid `spec` —
    //    `QueryEvalContext` invariant (2).
    // 2. `fs.index` is a valid index into `spec.fields`: the query AST node
    //    references a field of this very spec, so its `FieldSpec::index` is in
    //    bounds (mirrors the C `Query_EvalMissingNode` using `fs->index`).
    // 3. `spec.missingFieldDict` is a non-null, valid dict — initialised by
    //    `IndexSpec_MakeKeyless` for every queryable spec; it is also the dict
    //    we just fetched `ii_ptr` from above.
    // 4. `ii_ref` uses `DocIdsOnly`/`RawDocIdsOnly` encoding: the indexer only
    //    ever stores doc-ids-only inverted indexes in `missingFieldDict`.
    Some(unsafe { new_missing_iterator(ii_ref, sctx_nn, fs.index) })
}
