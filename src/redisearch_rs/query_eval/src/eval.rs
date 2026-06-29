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
    Empty, RQEIteratorPrintable,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::RQEIteratorWrapper,
    inverted_index::new_missing_iterator,
    not_reducer::{NewNotIterator, new_not_iterator},
    optional_reducer::{NewOptionalIterator, new_optional_iterator},
};

use crate::{QueryEvalContext, QueryNode, QueryNodeRef};

/// The return type of [`eval_node`]: a boxed Rust iterator that implements
/// both [`RQEIterator`](rqe_iterators::RQEIterator) and
/// [`ProfilePrint`](rqe_iterators::profile_print::ProfilePrint).
pub type EvalResult<'index> = Box<dyn RQEIteratorPrintable<'index> + 'index>;

/// The outcome of evaluating a query node.
///
/// The variant records *how* the resulting iterator is currently represented,
/// so it can be handed across the FFI boundary — or composed into a parent Rust
/// iterator — without a redundant wrapper or allocation. Three shapes occur
/// while the dispatcher is only partially ported:
///
/// - [`Evaluated::RustLeaf`] — a Rust iterator held as a trait object, not yet
///   lowered to the C ABI.
/// - [`Evaluated::C`] — an iterator *built by* the C [`ffi::Query_EvalNode`]
///   dispatcher for a node type not yet ported. Handed straight back to C so the
///   C-side optimizer/profiler keep seeing the original iterator.
/// - [`Evaluated::RustCompound`] — an owning C-ABI handle that Rust built and
///   already lowered, returned as-is rather than as a trait object (see the
///   variant docs for the two cases that need this shape).
// TODO: Remove this enum once all the node types have been ported to Rust
// and C `Query_EvalNode` has been removed.
#[must_use = "an unconsumed `Evaluated` may leak its owning iterator handle; consume it via `into_c_iterator` or `into_boxed`"]
pub enum Evaluated<'index> {
    /// An iterator implemented in Rust, held as a boxed trait object.
    ///
    /// Lowered to the C ABI lazily (via [`RQEIteratorWrapper::boxed_new`]) only
    /// if and when it crosses back to C.
    RustLeaf(EvalResult<'index>),

    /// An owning C iterator handle built by the C [`ffi::Query_EvalNode`]
    /// dispatcher for a node type not yet ported to Rust.
    C(NonNull<ffi::QueryIterator>),

    /// An owning C-ABI [`QueryIterator`](ffi::QueryIterator) handle that Rust
    /// built and already lowered, returned as-is rather than as an
    /// [`Evaluated::RustLeaf`] `Box<dyn …>`. Two cases need this shape:
    ///
    /// - A Rust *compound* iterator (e.g.
    ///   [`Optional`](rqe_iterators::optional::Optional)) lowered via
    ///   [`RQEIteratorWrapper::boxed_new_compound`]. It must reach the still
    ///   C-driven optimizer and profiler as an
    ///   `RQEIteratorWrapper<Compound<CRQEIterator>>`: only that shape carries the
    ///   [`ProfileChildren`](rqe_iterators::interop::ProfileChildren) callback the
    ///   profiler needs to recurse into the child, and keeps the child a concrete
    ///   [`CRQEIterator`] so the optimizer's in-place tree rewrites keep working.
    ///   Lowering it via [`RQEIteratorWrapper::boxed_new`] instead would drop the
    ///   child's profile counters.
    /// - A child iterator handed straight back unchanged — e.g. the optional
    ///   reducer's wildcard passthrough, where the optional node collapses to its
    ///   already-lowered wildcard child. Re-wrapping it as an [`Evaluated::RustLeaf`]
    ///   `Box<dyn …>` would add a redundant [`RQEIteratorWrapper`] layer and hide
    ///   the original iterator from the C-side optimizer and profiler.
    ///
    /// A freshly built Rust leaf (e.g. the optional reducer's wildcard *fallback*)
    /// needs none of this and is returned as a plain [`Evaluated::RustLeaf`] instead.
    ///
    /// Lifecycle-wise this is identical to [`Evaluated::C`]: an owning handle
    /// handed back untouched by [`into_c_iterator`](Self::into_c_iterator), or
    /// re-wrapped as a [`CRQEIterator`] child by [`into_boxed`](Self::into_boxed).
    /// The separate variant exists only to record that Rust, not C, built it.
    //
    // A typed `Box<dyn …>` (deferring the lowering to `into_c_iterator`) was
    // considered and rejected: the compound's child must already be a concrete
    // `CRQEIterator` for the C-side profiler/optimizer, so there is no pure-Rust
    // subtree to preserve; every consumer (the C entrypoint, or an outer Rust
    // compound) re-lowers to a handle anyway; and it would not cover the
    // passthrough case, which is a child handle, not a compound.
    //
    // Once profiling and the optimizer no longer reach into the tree as C
    // `*mut QueryIterator` nodes, these can hold pure-Rust `Box<dyn …>`
    // children and this variant can fold back into `RustLeaf`.
    RustCompound(NonNull<ffi::QueryIterator>),
}

impl<'index> Evaluated<'index> {
    /// Consume into an owning C [`QueryIterator`](ffi::QueryIterator) pointer.
    ///
    /// An [`Evaluated::RustLeaf`] iterator is lowered via
    /// [`RQEIteratorWrapper::boxed_new`]; an already-lowered handle
    /// ([`Evaluated::C`] or [`Evaluated::RustCompound`]) is returned as-is, so
    /// C-side introspection (optimizer, profiler) keeps seeing the same iterator.
    pub fn into_c_iterator(self) -> *mut ffi::QueryIterator {
        match self {
            Evaluated::RustLeaf(it) => RQEIteratorWrapper::boxed_new(it),
            Evaluated::C(it) | Evaluated::RustCompound(it) => it.as_ptr(),
        }
    }

    /// Consume into a boxed Rust iterator, wrapping an already-lowered C-ABI
    /// handle in a [`CRQEIterator`] shim so it satisfies the Rust iterator trait.
    ///
    /// Used by Rust consumers that compose evaluated children as trait objects.
    pub fn into_boxed(self) -> EvalResult<'index> {
        match self {
            Evaluated::RustLeaf(it) => it,
            Evaluated::C(it) | Evaluated::RustCompound(it) => {
                // SAFETY: both handle variants hold a valid, owning `QueryIterator`
                // with all required callbacks populated — `Evaluated::C` came from
                // `ffi::Query_EvalNode`, `Evaluated::RustCompound` from
                // `RQEIteratorWrapper::boxed_new_compound` — exactly the
                // preconditions of `CRQEIterator::new`.
                Box::new(unsafe { CRQEIterator::new(it) })
            }
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
    eval_node(ctx, root).unwrap_or_else(|| Evaluated::RustLeaf(Box::new(Empty)))
}

/// Evaluate a single query node, producing the corresponding iterator.
///
/// Returns `None` when the node produces no results.
pub fn eval_node<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Option<Evaluated<'index>> {
    match node.as_enum() {
        QueryNode::Null => Some(eval_null()),
        QueryNode::Wildcard => Some(eval_wildcard(ctx, node)),
        QueryNode::Ids { keys, doc_ids } => Some(eval_ids(ctx, keys, doc_ids)),
        QueryNode::Missing { field } => eval_missing(ctx, field).map(Evaluated::RustLeaf),
        QueryNode::Optional => Some(eval_optional(ctx, node)),
        QueryNode::Not => Some(eval_not(ctx, node)),
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

/// Evaluate a child node into an owning [`CRQEIterator`] for use as a child of
/// a Rust compound iterator.
///
/// A `None` child (no results) becomes a freshly boxed [`Empty`] so the
/// reducer can apply its empty-child rules, since a missing child is
/// equivalent to one that matches nothing.
fn eval_child_iterator(ctx: &mut QueryEvalContext, child: &QueryNodeRef) -> CRQEIterator {
    let ptr = match eval_node(&mut *ctx, child) {
        Some(ev) => ev.into_c_iterator(),
        None => RQEIteratorWrapper::boxed_new(Empty),
    };
    // `into_c_iterator` and `boxed_new` always return a valid, owning, non-null
    // C `QueryIterator`.
    let nn = NonNull::new(ptr).expect("evaluated child iterator must not be null");
    // SAFETY: `nn` is a valid, owning C `QueryIterator` with all callbacks
    // populated — exactly the precondition of `CRQEIterator::new`.
    unsafe { CRQEIterator::new(nn) }
}

/// `QN_NULL` — stopword queries produce an empty iterator.
fn eval_null<'index>() -> Evaluated<'index> {
    Evaluated::RustLeaf(Box::new(Empty))
}

/// `QN_WILDCARD` — the `*` query that matches every document.
fn eval_wildcard<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Evaluated<'index> {
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
    Evaluated::RustLeaf(Box::new(it))
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

/// `QN_OPTIONAL` — an optional match that boosts the score when its single
/// child matches but does not exclude documents that don't.
fn eval_optional<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Evaluated<'index> {
    debug_assert_eq!(
        node.num_children(),
        1,
        "an optional node must have exactly one child"
    );

    // Evaluate the child. A `None` child becomes an `Empty` iterator, which
    // `new_optional_iterator` reduces to a wildcard fallback — an empty optional
    // matches every document as a virtual hit.
    let child_node = node.child(0);
    let child = eval_child_iterator(ctx, &child_node);

    // SAFETY: the preconditions of `new_optional_iterator` map to
    // `QueryEvalContext::new` invariants:
    // 1. `query` is a valid, non-null `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is valid and non-null — invariant (2).
    // 3. `query.sctx.spec` is valid and non-null — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1).
    // 5-7. The wildcard-iterator preconditions hold for the same reasons
    //    as in `eval_wildcard` (a properly initialised spec with its
    //    `existingDocs` index, valid `docTable`, and
    //    `diskSpec`/`SEARCH_ENTERPRISE_ITERATORS` when on disk).
    let outcome = unsafe {
        new_optional_iterator(
            child,
            node.opts().weight,
            ctx.as_non_null(),
            ctx.max_doc_id(),
        )
    };
    match outcome {
        // The child was structurally empty: the reducer built a fresh Rust
        // wildcard leaf so every document is returned as a virtual hit.
        NewOptionalIterator::WildcardFallback(wc) => Evaluated::RustLeaf(Box::new(wc)),
        // The optional collapsed to its already-lowered wildcard child: hand the
        // child's owning handle straight back, exactly as the former C path did,
        // so the C-side optimizer/profiler keep seeing the original iterator. A
        // plain `Evaluated::RustLeaf` would wrap it in a redundant `RQEIteratorWrapper`.
        NewOptionalIterator::WildcardPassthrough(child) => {
            Evaluated::RustCompound(child.into_raw())
        }
        // Genuine compound iterators: lower via `boxed_new_compound` so the
        // profiler keeps the `ProfileChildren` callback into the child.
        NewOptionalIterator::Optional(opt) => Evaluated::RustCompound(
            NonNull::new(RQEIteratorWrapper::boxed_new_compound(opt))
                .expect("optional iterator must not be null"),
        ),
        NewOptionalIterator::OptionalOptimized(opt) => Evaluated::RustCompound(
            NonNull::new(RQEIteratorWrapper::boxed_new_compound(opt))
                .expect("optional iterator must not be null"),
        ),
    }
}

/// `QN_NOT` — logical negation: matches every document *not* matched by its
/// single child.
fn eval_not<'index>(ctx: &'index mut QueryEvalContext, node: &QueryNodeRef) -> Evaluated<'index> {
    debug_assert_eq!(
        node.num_children(),
        1,
        "a not node must have exactly one child"
    );

    // Evaluate the child with the "not-subtree" flag set. A NOT only cares
    // *whether* a document matches its child, never the child's score, so any
    // descendant `UNION` may stop at its first matching branch instead of
    // visiting every branch to accumulate a score. Setting the flag lets those
    // unions take that cheaper quick exit path.
    //
    // The previous value is saved and restored rather than just cleared: NOT
    // nodes can nest (e.g. `-(-foo)`), and the outer NOT must keep the flag set
    // while the inner one is being evaluated and after it returns.
    let prev_in_not_sub_tree = ctx.set_in_not_sub_tree(true);
    let child_node = node.child(0);
    let child = eval_child_iterator(ctx, &child_node);
    ctx.set_in_not_sub_tree(prev_in_not_sub_tree);

    // SAFETY: the preconditions of `new_not_iterator` map to
    // `QueryEvalContext::new` invariants:
    // 1. `query` is a valid, non-null `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is valid and non-null — invariant (2).
    // 3. `query.sctx.spec` is valid and non-null — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1).
    // 5. The wildcard-iterator preconditions hold for the same reasons
    //    as in `eval_wildcard` (a properly initialised spec with its
    //    `existingDocs` index, valid `docTable`, and
    //    `diskSpec`/`SEARCH_ENTERPRISE_ITERATORS` when on disk).
    let outcome = unsafe {
        new_not_iterator(
            child,
            ctx.max_doc_id(),
            node.opts().weight,
            ctx.build_timeout_context(),
            ctx.as_non_null(),
        )
    };
    match outcome {
        NewNotIterator::ReducedWildcard(wc) => Evaluated::RustLeaf(Box::new(wc)),
        NewNotIterator::ReducedEmpty(empty) => Evaluated::RustLeaf(Box::new(empty)),
        NewNotIterator::Not(it) => Evaluated::RustCompound(
            NonNull::new(RQEIteratorWrapper::boxed_new_compound(it))
                .expect("not iterator must not be null"),
        ),
        NewNotIterator::NotOptimized(it) => Evaluated::RustCompound(
            NonNull::new(RQEIteratorWrapper::boxed_new_compound(it))
                .expect("not iterator must not be null"),
        ),
    }
}
