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
//! dispatching on the [`QueryNodeType`](query_types::QueryNodeType) discriminant.

use std::{ffi::CStr, ptr::NonNull};

use c_trie::CTrieRef;
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::NumericFilter;
use query_error::QueryErrorCode;
use query_term::RSQueryTerm;
use query_types::{QueryNodeOptions, scorers::slop_forces_offsets};
use rqe_core::{DocId, FieldMask};
use rqe_iterators::{
    Empty, IteratorsConfig, RQEIteratorPrintable, build_geo_range_iterator,
    build_numeric_filter_iterator, build_term_iterator,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    interop::RQEIteratorWrapper,
    intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator},
    inverted_index::new_missing_iterator,
    not_reducer::{NewNotIterator, new_not_iterator},
    optional_reducer::{NewOptionalIterator, new_optional_iterator},
    union_opaque::build_union,
};
use search_disk::SearchDiskHandle;

use crate::{
    QueryEvalContext, QueryNode, QueryNodeRef,
    scorers::{BuiltInScorer, RequestedScorer},
};

/// Global configuration values consumed by the query evaluator.
///
/// These are snapshotted from the process-wide configuration once, at the FFI
/// entry point, and threaded through evaluation as an explicit parameter rather
/// than read from global state deep inside the dispatcher.
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// Whether numeric inverted indexes use the compressed on-disk encoding.
    pub numeric_compress: bool,
    /// Whether an intersection prioritizes its union children when ordering its
    /// sub-iterators for evaluation.
    pub prioritize_intersect_union_children: bool,
    /// The configured default scorer, applied to a query that sets no scorer of
    /// its own.
    ///
    /// [`None`] when no built-in default is configured (either unset or a custom
    /// scorer name), in which case a query with no scorer of its own is treated
    /// as a custom scorer.
    pub default_scorer: Option<BuiltInScorer>,
    /// Minimum number of children for a union iterator to use a heap-based
    /// implementation instead of a flat linear scan.
    pub min_union_iter_heap: usize,
}

impl Default for Config {
    fn default() -> Self {
        // Reuse the shared iterator-config default so the union-heap threshold
        // keeps a single source of truth.
        let IteratorsConfig {
            min_union_iter_heap,
            ..
        } = IteratorsConfig::default();
        let min_union_iter_heap = min_union_iter_heap as usize;

        Self {
            numeric_compress: false,
            prioritize_intersect_union_children: false,
            default_scorer: None,
            min_union_iter_heap,
        }
    }
}

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
    config: Config,
) -> Evaluated<'index> {
    eval_node(ctx, root, config).unwrap_or_else(|| Evaluated::RustLeaf(Box::new(Empty)))
}

/// Evaluate a single query node, producing the corresponding iterator.
///
/// Returns `None` when the node produces no results.
pub fn eval_node<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    config: Config,
) -> Option<Evaluated<'index>> {
    match node.as_enum() {
        QueryNode::Null => Some(eval_null()),
        QueryNode::Wildcard => Some(eval_wildcard(ctx, node)),
        QueryNode::Ids { keys, doc_ids } => Some(eval_ids(ctx, keys, doc_ids)),
        QueryNode::Missing { field } => eval_missing(ctx, field).map(Evaluated::RustLeaf),
        QueryNode::Optional => Some(eval_optional(ctx, node, config)),
        QueryNode::Not => Some(eval_not(ctx, node, config)),
        QueryNode::Phrase { exact } => eval_phrase(ctx, node, exact, config),
        QueryNode::Union => Some(eval_union(ctx, node, config)),
        QueryNode::Numeric { nf } => eval_numeric(ctx, nf, config),
        QueryNode::Geo { gf } => eval_geo(ctx, gf, config),
        QueryNode::Token { tok } => eval_token(ctx, node, tok, config),
        QueryNode::Geometry { geomq } => eval_geometry(ctx, geomq),
        // Node types not yet ported to Rust are delegated back to the C
        // dispatcher.
        _ => eval_node_c(ctx, node, config),
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
    config: Config,
) -> Option<Evaluated<'index>> {
    let q = ctx.as_non_null().as_ptr();
    let n = node.as_non_null().as_ptr();
    let config = (&raw const config).cast::<ffi::EvalConfig>();
    // SAFETY: `q` comes from a live `QueryEvalContext` (a valid `QueryEvalCtx`
    // with exclusive access, since `ctx` is `&mut`) and `n` from a live
    // `QueryNodeRef` (a valid `RSQueryNode`), satisfying `Query_EvalNode`'s
    // contract. `config` points to a live `Config` valid for the duration of the
    // call.
    let it = unsafe { ffi::Query_EvalNode(q, n, config) };
    NonNull::new(it).map(Evaluated::C)
}

/// Evaluate a child node into an owning [`CRQEIterator`] for use as a child of
/// a Rust compound iterator.
///
/// A `None` child (no results) becomes a freshly boxed [`Empty`] so the
/// reducer can apply its empty-child rules, since a missing child is
/// equivalent to one that matches nothing.
fn eval_child_iterator(
    ctx: &mut QueryEvalContext,
    child: &QueryNodeRef,
    config: Config,
) -> CRQEIterator {
    let ptr = match eval_node(&mut *ctx, child, config) {
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
    config: Config,
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
    let child = eval_child_iterator(ctx, &child_node, config);

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
fn eval_not<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    config: Config,
) -> Evaluated<'index> {
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
    let child = eval_child_iterator(ctx, &child_node, config);
    ctx.set_in_not_sub_tree(prev_in_not_sub_tree);

    // SAFETY: invariant (2) of `QueryEvalContext::new` guarantees `bcTimeoutAreq`
    // outlives every timeout context derived from `ctx`, and the returned context
    // is handed straight to `new_not_iterator` below (never retained past this
    // query), so it cannot be used after the `AREQ` is freed.
    let timeout_ctx = unsafe { ctx.build_timeout_context() };

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
            timeout_ctx,
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

/// `QN_PHRASE` — an ordered/unordered conjunction of child terms.
///
/// A single-child phrase is equivalent to the child itself, so the child is
/// returned directly (after narrowing its field mask). Otherwise the children
/// are evaluated and combined with an [`Intersection`], honoring the phrase's
/// slop/in-order constraints (exact phrases force slop `0`, in order).
///
/// Each child's field mask is first intersected with the phrase node's own
/// mask, so a child only matches the fields shared with the phrase.
///
/// * `exact` — whether this is an exact (quoted) phrase. When `true`, the
///   terms must be adjacent and in order: slop is forced to `0` and in-order
///   matching is required, ignoring the per-node and query-wide slop/in-order
///   settings. When `false`, the slop and in-order constraints are resolved
///   from the node's own options, falling back to the query-wide defaults.
fn eval_phrase<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    exact: bool,
    config: Config,
) -> Option<Evaluated<'index>> {
    // Query evaluation is single-threaded and walks each AST node exactly once,
    // top-down, so we hold exclusive access to this phrase node and its
    // descendants for the duration of the call: no other wrapper into this
    // subtree, and no reference derived from one, is live here.
    //
    // SAFETY: that exclusive-access precondition is exactly what `as_mut`
    // requires. It is asserted once, here; every field-mask write below then
    // goes through the exclusive `QueryNodeMut` and is statically alias-free.
    let mut node = unsafe { node.as_mut() };

    let num_children = node.num_children();
    let node_mask = node.opts().field_mask;
    // A single-child intersection is just the child; return it directly.
    if num_children == 1 {
        let mut child = node.child_mut(0);
        child.and_field_mask(node_mask);
        return eval_node(ctx, child.as_ref(), config);
    }

    let weight = node.opts().weight;

    let (max_slop, in_order) = if exact {
        // An exact (quoted) phrase requires adjacent, in-order terms.
        (Some(0), true)
    } else {
        // The node may override the query-wide slop; -1 means "use the default".
        let slop = match node.opts().max_slop {
            -1 => ctx.slop(),
            s => s,
        };
        let in_order = ctx.search_in_order() || node.opts().in_order != 0;
        let max_slop = if slop < 0 { None } else { Some(slop as u32) };
        (max_slop, in_order)
    };

    // Recursively evaluate every child, narrowing its field mask first.
    let mut children = Vec::with_capacity(num_children);
    for i in 0..num_children {
        let mut child = node.child_mut(i);
        child.and_field_mask(node_mask);
        children.push(eval_child_iterator(ctx, child.as_ref(), config));
    }

    let result_ptr = match new_intersection_iterator(children) {
        NewIntersectionIterator::Empty => return Some(Evaluated::RustLeaf(Box::new(Empty))),
        NewIntersectionIterator::Single(child) => child.into_raw().as_ptr(),
        NewIntersectionIterator::Proceed(cs) => {
            let intersection = Intersection::new_with_slop_order(
                cs,
                weight,
                config.prioritize_intersect_union_children,
                max_slop,
                in_order,
            );
            RQEIteratorWrapper::boxed_new_compound(intersection)
        }
    };

    Some(Evaluated::RustCompound(
        NonNull::new(result_ptr).expect("phrase iterator must not be null"),
    ))
}

/// `QN_UNION` — a logical OR over its children (matches any document matched by
/// at least one child).
///
/// The children are evaluated and combined with the Rust union iterator. Each
/// child's field mask is first intersected with the union node's own mask, and
/// `quick_exit` is enabled when the union only needs the matching id set rather
/// than per-child scores — i.e. inside a `NOT` subtree or when the node's weight
/// is zero.
fn eval_union<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    config: Config,
) -> Evaluated<'index> {
    // Parsers and expanders always create unions with 2+ children.
    debug_assert!(
        node.num_children() > 1,
        "a union node must have more than one child"
    );

    let num_children = node.num_children();
    let node_mask = node.opts().field_mask;
    let weight = node.opts().weight;
    let node_type = node.node_type();

    // We want results from every matching child (`quick_exit == false`) unless
    // either (1) we are inside a `NOT` subtree, where only the id set matters,
    // or (2) the node's weight is zero, so its subtree is irrelevant to scoring.
    let quick_exit = ctx.in_not_sub_tree() || weight == 0.0;
    let min_union_iter_heap = config.min_union_iter_heap;

    // Recursively evaluate every child, narrowing its field mask first.
    //
    // SAFETY: query evaluation is single-threaded and walks each AST node
    // exactly once, top-down, so we hold exclusive access to this node and its
    // subtree for the duration of the call — exactly what `as_mut` requires.
    let mut node = unsafe { node.as_mut() };
    let children: Vec<CRQEIterator> = (0..num_children)
        .map(|i| {
            let mut child = node.child_mut(i);
            child.and_field_mask(node_mask);
            eval_child_iterator(ctx, child.as_ref(), config)
        })
        .collect();

    let iter = build_union(children, quick_exit, min_union_iter_heap, node_type, weight);

    Evaluated::RustCompound(iter)
}

/// `QN_NUMERIC` — a numeric range filter on a numeric field.
///
/// When the spec is backed by an on-disk index, delegates to the enterprise
/// numeric iterator via [`SearchDiskHandle::new_numeric_iterator`]. Otherwise
/// opens the field's numeric range tree and builds a union over the matching
/// sub-ranges. Returns `None` when the field has no numeric index yet,
/// no sub-range matches, or the disk iterator not be created
/// (in which case the failure is reported via [`status`](QueryEvalContext::status)).
fn eval_numeric<'index>(
    ctx: &'index mut QueryEvalContext,
    nf: &NumericFilter,
    config: Config,
) -> Option<Evaluated<'index>> {
    // The numeric node always carries a field spec; the filter targets that
    // single field by index.
    assert!(
        !nf.field_spec.is_null(),
        "numeric node must have a non-null field spec"
    );
    // SAFETY: a well-formed numeric node has a valid, non-null `field_spec`,
    // so reading its `index` is sound.
    let field_index = unsafe { (*nf.field_spec).index };

    // Disk-index path: when the spec is backed by an on-disk index, delegate to
    // the enterprise numeric iterator instead of opening the in-memory range
    // tree.
    //
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2). `SearchDiskHandle::new` yields `None`
    // for the null (in-memory) case.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed numeric query");
        // SAFETY: the wrapped disk spec is valid for `'index` (`QueryEvalContext`
        // invariants 1/2) and single-threaded query evaluation gives us the only
        // live reference to it; the enterprise iterators are registered whenever
        // a disk index is in use; `field_index` belongs to the numeric node's
        // field spec; `snapshot` is the disk snapshot taken at query start.
        return match unsafe { disk.new_numeric_iterator(nf, field_index, snapshot) } {
            Ok(it) => Some(Evaluated::RustLeaf(it)),
            Err(err) => {
                // Surface the failure via `status` so the query aborts with an
                // error rather than silently returning empty results.
                ctx.status()
                    .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
                None
            }
        };
    }

    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };

    let min_union_iter_heap = config.min_union_iter_heap;
    // SAFETY: `build_numeric_filter_iterator` preconditions hold:
    // 1. `sctx`/`sctx.spec` are valid and outlive the iterator —
    //    `QueryEvalContext` invariants (1)/(2).
    // 2. `nf.field_spec` is a valid, non-null `FieldSpec` for a numeric field
    //    (well-formed numeric node).
    // 3. `field_ctx.field` is a field index, built as `Index` just above.
    let iter = unsafe {
        build_numeric_filter_iterator(
            ctx.sctx(),
            nf,
            min_union_iter_heap,
            &field_ctx,
            config.numeric_compress,
        )
    };

    iter.map(Evaluated::RustCompound)
}

/// `QN_GEO` — a geo-radius filter on a geo field.
///
/// Validates the geo filter (reporting any error into the query's status). When
/// the spec is backed by an on-disk index, delegates to the enterprise geo
/// iterator via [`SearchDiskHandle::new_geo_iterator`]. Otherwise builds a union
/// over the matching geohash ranges via [`build_geo_range_iterator`]. Returns
/// `None` — i.e. no iterator — when validation fails, the geo index does not
/// exist yet, no entries match, or the disk iterator could not be created (in
/// which case the failure is reported via [`status`](QueryEvalContext::status)).
fn eval_geo<'index>(
    ctx: &'index mut QueryEvalContext,
    gf: *mut ffi::GeoFilter,
    config: Config,
) -> Option<Evaluated<'index>> {
    let status = ctx.status_ptr();
    // SAFETY: `gf` is a valid, non-null `GeoFilter` (well-formed geo node) and
    // `status` is the query's valid `QueryError` accumulator
    // (`QueryEvalContext` invariant (2)).
    if unsafe { ffi::GeoFilter_Validate(gf, status) } == 0 {
        return None;
    }

    // Disk-index path: when the spec is backed by an on-disk index, delegate to
    // the enterprise geo iterator instead of opening the in-memory range tree.
    //
    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index`
    // (`QueryEvalContext` invariants 1/2). `SearchDiskHandle::new` yields `None`
    // for the null (in-memory) case.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed geo query");
        // SAFETY: `gf` is valid and, during single-threaded evaluation,
        // exclusively owned for `'index`, so a `&'index mut` is sound.
        let gf_ref = unsafe { &mut *gf };
        // SAFETY: a well-formed geo node has a valid, non-null `fieldSpec`, so
        // reading its `index` is sound.
        let field_index = unsafe { (*gf_ref.fieldSpec).index };
        // SAFETY: the wrapped disk spec is valid for `'index` (`QueryEvalContext`
        // invariants 1/2) and single-threaded query evaluation gives us the only
        // live reference to it; the enterprise iterators are registered whenever
        // a disk index is in use; `field_index` belongs to the geo node's field
        // spec; `snapshot` is the disk snapshot taken at query start.
        return match unsafe { disk.new_geo_iterator(gf_ref, field_index, snapshot) } {
            Ok(it) => Some(Evaluated::RustLeaf(it)),
            Err(err) => {
                // Surface the failure via `status` so the query aborts with an
                // error rather than silently returning empty results.
                ctx.status()
                    .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
                None
            }
        };
    }

    let sctx = NonNull::from(ctx.sctx());
    let min_union_iter_heap = config.min_union_iter_heap;
    // SAFETY: `gf` is valid and, during evaluation, exclusively owned, so a
    // `&mut` is sound.
    let gf_ref = unsafe { &mut *gf };
    // SAFETY: `build_geo_range_iterator` preconditions hold:
    // 1. `sctx`/`sctx.spec` are valid and outlive the iterator —
    //    `QueryEvalContext` invariants (1)/(2).
    // 2. `gf.fieldSpec` is a valid, non-null `FieldSpec` for a geo field
    //    (well-formed geo node).
    // 3. `gf.numericFilters` is NULL on entry (freshly parsed geo node) and is
    //    populated/owned by `gf`, freed by `GeoFilter_Free`.
    let iter = unsafe {
        build_geo_range_iterator(sctx, gf_ref, min_union_iter_heap, config.numeric_compress)
    };

    iter.map(Evaluated::RustCompound)
}

/// `QN_TOKEN` — a single-term lookup.
///
/// In the in-memory path the term's inverted index is opened via
/// [`Redis_OpenReaderIndex`](ffi::Redis_OpenReaderIndex) and wrapped in a term
/// iterator with [`build_term_iterator`]. In search-on-disk mode the work is
/// delegated to [`eval_token_disk`].
/// Returns `None` when the term has no matching inverted index (e.g. it is absent).
fn eval_token<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    tok: &ffi::RSToken,
    config: Config,
) -> Option<Evaluated<'index>> {
    // A `QN_TOKEN` node always carries a non-null term string.
    debug_assert!(!tok.str_.is_null(), "token string should not be null");
    let opts = node.opts();
    let weight = opts.weight;
    // the node's field mask narrowed to the query's.
    let effective_field_mask = opts.field_mask & ctx.opts().fieldmask;
    let token_id = ctx.next_token_id() as i32;
    // SAFETY: `tok.str_` points to `tok.len` valid bytes from the query node.
    let term_bytes = unsafe { std::slice::from_raw_parts(tok.str_.cast::<u8>(), tok.len) };
    let term = RSQueryTerm::new_bytes(term_bytes, token_id, tok.flags());

    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index` (`QueryEvalContext`
    // invariants 1/2). `SearchDiskHandle::new` yields `None` for the null
    // (in-memory) case, which falls through to the in-memory reader below.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        eval_token_disk(
            ctx,
            disk,
            tok,
            term,
            opts,
            weight,
            effective_field_mask,
            config,
        )
    } else {
        open_term_reader(ctx, tok, term, weight, effective_field_mask)
    }
}

/// Open an in-memory term reader for a `QN_TOKEN` node.
///
/// Opens and validates the term's inverted index and, on success, wraps it in a
/// term iterator that takes ownership of `term`. Returns `None` when the term
/// has no matching inverted index (absent, empty, or no results in the queried
/// field(s)), dropping `term`.
fn open_term_reader<'index>(
    ctx: &'index mut QueryEvalContext,
    tok: &ffi::RSToken,
    term: Box<RSQueryTerm>,
    weight: f64,
    effective_field_mask: FieldMask,
) -> Option<Evaluated<'index>> {
    debug_assert!(!ctx.sctx_ptr().is_null(), "sctx must not be null");

    // Open and validate the term's inverted index. A null result means the term
    // has no matching index (absent, empty, or no results in the queried
    // field(s)), so there is nothing to read.
    // SAFETY: `ctx.sctx_ptr()` is valid (`QueryEvalContext` invariant 2) and
    // `tok` is the query node's `RSToken`; `Redis_OpenReaderIndex` only reads the
    // token and does not retain it.
    let idx = unsafe {
        ffi::Redis_OpenReaderIndex(
            ctx.sctx_ptr(),
            std::ptr::from_ref(tok),
            effective_field_mask,
        )
    };
    let idx = NonNull::new(idx)?;

    // SAFETY: `ctx.sctx_ptr()` is non-null (`QueryEvalContext` invariant 2).
    let sctx = unsafe { NonNull::new_unchecked(ctx.sctx_ptr().cast_mut()) };
    // SAFETY: `idx` is the term's inverted index just opened for this spec and
    // stays valid for `'index` (`QueryEvalContext` invariants 1/2); `sctx` and its
    // spec are valid for `'index` (invariant 2); `term` is a freshly
    // heap-allocated query term whose ownership transfers to the iterator.
    let iter = unsafe {
        build_term_iterator(
            idx.as_ptr(),
            sctx,
            FieldMaskOrIndex::Mask(effective_field_mask),
            term,
            weight,
        )
    };

    Some(Evaluated::RustLeaf(Box::new(iter)))
}

/// Search-on-disk evaluation of a `QN_TOKEN` node.
///
/// Looks up the term's document count in the terms trie to compute the IDF
/// then builds a disk term iterator.
///
/// `disk` must wrap the spec's own disk index.
///
/// Returns `None` — after setting the query status —
/// when the disk iterator cannot be built.
#[expect(clippy::too_many_arguments)]
fn eval_token_disk<'index>(
    ctx: &'index mut QueryEvalContext,
    disk: SearchDiskHandle,
    tok: &ffi::RSToken,
    mut term: Box<RSQueryTerm>,
    opts: &QueryNodeOptions,
    weight: f64,
    effective_field_mask: FieldMask,
    config: Config,
) -> Option<Evaluated<'index>> {
    let spec = ctx.spec();
    // Look up the term's document count in the terms trie to compute IDF, then
    // build a disk term iterator through the enterprise API.
    // SAFETY: in search-on-disk mode the terms trie is always initialised.
    debug_assert!(!spec.terms.is_null(), "terms trie should be initialized");
    // A `QN_TOKEN` node always carries a non-null term string; the term lookup
    // below relies on it.
    debug_assert!(!tok.str_.is_null(), "token string should not be null");

    // SAFETY: `spec.terms` is a valid `Trie` (checked non-null above) that
    // outlives this lookup.
    let terms = unsafe { CTrieRef::from_raw(spec.terms) };
    // SAFETY: `tok.str_` points to `tok.len` valid bytes from the query node.
    let term_bytes = unsafe { std::slice::from_raw_parts(tok.str_.cast::<u8>(), tok.len) };
    let num_docs_in_term = terms.num_docs(term_bytes);
    let num_documents = spec.stats.scoring.numDocuments;
    let idf = idf::calculate_idf(num_documents, num_docs_in_term);
    let bm25_idf = idf::calculate_idf_bm25(num_documents, num_docs_in_term);
    term.set_idf(idf);
    term.set_bm25_idf(bm25_idf);

    let needs_offsets = expansion_needs_offsets(ctx, opts, config);

    let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
        .expect("query.sctx.diskSnapshot is null for a disk-backed token query");
    // SAFETY: `disk` wraps the spec's disk index, valid for `'index`
    // (`QueryEvalContext` invariants 1/2), and single-threaded query evaluation
    // gives us the only live reference to it; the enterprise iterators are
    // registered whenever a disk index is in use; `snapshot` is the disk snapshot
    // taken at query start.
    let iter = unsafe {
        disk.new_term_iterator(term, effective_field_mask, weight, needs_offsets, snapshot)
    };

    match iter {
        Ok(it) => Some(Evaluated::RustLeaf(it)),
        Err(err) => {
            // Surface the failure via `status` so the query aborts with an
            // error rather than silently returning empty results.
            ctx.status()
                .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
            None
        }
    }
}

/// `QN_GEOMETRY` — a geometry (WKT/GeoJSON) spatial predicate on a GEOSHAPE
/// field.
///
/// The GEOSHAPE index is a C++ R-tree exposed through a C API
/// ([`GeometryApi`](ffi::GeometryApi)); this evaluator opens the field's index
/// and dispatches to its `query` callback. On a query error the API returns
/// NULL and an error string, which is reported into the query status; the
/// iterator is `None`.
fn eval_geometry<'index>(
    ctx: &'index mut QueryEvalContext,
    geomq: *mut ffi::GeometryQuery,
) -> Option<Evaluated<'index>> {
    debug_assert!(
        !geomq.is_null(),
        "geometry node must carry a geometry query"
    );
    // SAFETY: a well-formed geometry node carries a valid, non-null
    // `GeometryQuery` whose `fs` is a valid, non-null `FieldSpec`.
    let gq = unsafe { &*geomq };
    let fs = gq.fs;
    debug_assert!(!fs.is_null(), "geometry query must have a field spec");
    // SAFETY: `fs` is a valid, non-null `FieldSpec`.
    let field_index = unsafe { (*fs).index };

    // TODO: pass `false` (don't create the index if missing) once the query
    // string is validated before reaching this evaluator. Today, if the index
    // has not been created yet and the query is invalid, not creating it would
    // return results as if the index were empty instead of raising an error, so
    // we create it eagerly to force the error path.
    //
    // SAFETY: `fs` is a valid `FieldSpec`. `OpenGeometryIndex` does not keep the
    // pointer; with create-if-missing it may mutate `fs` to lazily attach the
    // index, which is sound here because no live Rust borrow aliases `fs` (`gq`
    // borrows the `GeometryQuery`, a separate allocation).
    let index = unsafe { ffi::OpenGeometryIndex(fs.cast_mut(), true) };
    debug_assert!(
        !index.is_null(),
        "OpenGeometryIndex with create-if-missing must return a valid index"
    );
    // SAFETY: `index` is a valid `GeometryIndex` from `OpenGeometryIndex`.
    let api = unsafe { ffi::GeometryApi_Get(index) };
    debug_assert!(!api.is_null(), "GeometryApi_Get must return a valid api");

    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };
    let sctx = ctx.sctx_ptr();
    let mut err_msg: *mut ffi::RedisModuleString = std::ptr::null_mut();

    // SAFETY: `api` is a valid `GeometryApi` and its `query` callback is always
    // populated by `GeometryApi_Get`.
    let query_fn = unsafe { (*api).query }.expect("geometry api `query` must be set");
    // SAFETY: all pointers are valid for the duration of the call: `sctx` and
    // `field_ctx` outlive it, `index` is valid, `gq.str` points to `gq.str_len`
    // bytes, and `err_msg` is a valid out-pointer.
    let ret = unsafe {
        query_fn(
            sctx,
            std::ptr::from_ref(&field_ctx).cast(),
            index,
            gq.query_type,
            gq.format,
            gq.str_,
            gq.str_len,
            &mut err_msg,
        )
    };

    if ret.is_null() {
        let detail = if let Some(err) = NonNull::new(err_msg) {
            // SAFETY: these Redis API function-pointer are set once
            // during module load and never mutated afterwards, so reading them
            // during query evaluation cannot race.
            let string_ptr_len =
                unsafe { ffi::RedisModule_StringPtrLen }.expect("RedisModule_StringPtrLen unset");
            // SAFETY: set once at module load, never mutated afterwards (see above).
            let free_string =
                unsafe { ffi::RedisModule_FreeString }.expect("RedisModule_FreeString unset");
            // SAFETY: `err` is a valid `RedisModuleString` returned by the query.
            let str_ptr = unsafe { string_ptr_len(err.as_ptr(), std::ptr::null_mut()) };
            // SAFETY: `str_ptr` is a valid, NUL-terminated C string.
            let s = unsafe { CStr::from_ptr(str_ptr) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: `err` was allocated by the query and is freed exactly once.
            unsafe { free_string(std::ptr::null_mut(), err.as_ptr()) };
            s
        } else {
            String::new()
        };
        ctx.status().set_with_user_data(
            query_error::QueryErrorCode::BadVal,
            "Error querying geoshape index",
            &format!(": {detail}"),
        );
    }

    NonNull::new(ret).map(Evaluated::C)
}

/// Whether a term disk reader must carry term offsets: required when the node
/// forces slop/in-order matching, or when the effective scorer needs positions.
/// Used only on the disk path.
fn expansion_needs_offsets(
    ctx: &mut QueryEvalContext,
    opts: &QueryNodeOptions,
    config: Config,
) -> bool {
    // The query's own scorer wins; a query that sets none falls back to the
    // configured default, while a custom (non built-in) scorer conservatively
    // needs offsets since we can't resolve what it does.
    let scorer = match ctx.scorer() {
        RequestedScorer::Unset => config.default_scorer,
        RequestedScorer::Custom(_) => None,
        RequestedScorer::BuiltIn(scorer) => Some(scorer),
    };
    slop_forces_offsets(opts.max_slop, opts.in_order)
        || scorer.is_none_or(BuiltInScorer::needs_offsets)
}
