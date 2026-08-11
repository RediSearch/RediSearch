/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query evaluation: traverses a parsed query AST and builds an executable
//! iterator tree.
//!
//! [`eval_node`] converts a parsed query AST node into an executable iterator
//! tree by dispatching on the
//! [`QueryNodeType`](query_types::QueryNodeType) discriminant. Each node type is
//! evaluated by its own module — [`token`] for `QN_TOKEN`, [`union`] for
//! `QN_UNION`, and so on — mirroring the per-node-type layout of this crate's
//! integration tests. This module keeps what they share: the evaluator
//! [`Config`], the [`Evaluated`] outcome type, the dispatcher and its
//! [`qast_iterate`] entry point, and the helpers for evaluating a child node
//! ([`eval_child_iterator`]) and for delegating an unported node to C
//! ([`eval_node_c`]).

use std::ptr::NonNull;

use query_types::{QueryNodeOptions, scorers::slop_forces_offsets};
use rqe_iterators::{
    Empty, RQEIteratorPrintable, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
};

// The query wrapper types live in the `query` crate (`c_wrappers/query`), and
// the scorer/expander name modules in `query_types`; both are re-exported here
// so `query_eval` (and its FFI crate) can refer to them through a single module.
pub use query::{QueryEvalContext, QueryNode, QueryNodeMut, QueryNodeRef};
pub use query_types::{expanders, scorers};

use scorers::{BuiltInScorer, RequestedScorer};

mod config;
mod disk;
mod expansion;
mod nodes;

pub use config::Config;

use nodes::{
    geo, geometry, ids, missing, not, null, numeric, optional, phrase, prefix, token, union,
    wildcard,
};

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
    root: QueryNodeMut<'_>,
    config: Config,
) -> Evaluated<'index> {
    eval_node(ctx, root, config).unwrap_or_else(|| Evaluated::RustLeaf(Box::new(Empty)))
}

/// Evaluate a single query node, producing the corresponding iterator.
///
/// Returns `None` when the node produces no results.
///
/// The node is taken as an **exclusive** [`QueryNodeMut`], by value. Evaluation
/// mutates the AST — it narrows children's field masks, and the C evaluator it
/// falls back to rewrites tokens in place — so a shared borrow would be a lie.
/// Taking it by value is also what lets the borrow checker police that: an arm
/// that reads a payload out of the node (e.g. a token handle) keeps it
/// shared-borrowed, and can therefore neither mutate it nor hand it on to a
/// callee that might.
pub fn eval_node<'index>(
    ctx: &'index mut QueryEvalContext,
    node: QueryNodeMut<'_>,
    config: Config,
) -> Option<Evaluated<'index>> {
    match node.as_enum() {
        QueryNode::Null => Some(null::eval()),
        QueryNode::Wildcard => Some(wildcard::eval(ctx, &node)),
        QueryNode::Ids { keys, doc_ids } => Some(ids::eval(keys, doc_ids)),
        QueryNode::Missing { field } => missing::eval(ctx, field).map(Evaluated::RustLeaf),
        QueryNode::Optional => Some(optional::eval(ctx, node, config)),
        QueryNode::Not => Some(not::eval(ctx, node, config)),
        QueryNode::Phrase { exact } => phrase::eval(ctx, node, exact, config),
        QueryNode::Union => Some(union::eval(ctx, node, config)),
        QueryNode::Numeric { nf } => numeric::eval(ctx, nf, config),
        QueryNode::Geo { gf } => geo::eval(ctx, gf, config),
        QueryNode::Token { tok } => token::eval(ctx, &node, tok, config),
        QueryNode::Geometry { geomq } => geometry::eval(ctx, geomq),
        QueryNode::Prefix { tok, mode } => prefix::eval(ctx, &node, tok, mode, config),
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
///
/// Consumes `node`: the C dispatcher mutates the subtree it is given — it
/// rewrites wildcard and prefix tokens in place — so no borrow of it may still
/// be outstanding. Taking it by value makes the borrow checker enforce that.
fn eval_node_c<'index>(
    ctx: &'index mut QueryEvalContext,
    node: QueryNodeMut<'_>,
    config: Config,
) -> Option<Evaluated<'index>> {
    let q = ctx.as_non_null().as_ptr();
    let n = node.as_non_null().as_ptr();
    let config = (&raw const config).cast::<ffi::EvalConfig>();
    // SAFETY: `q` comes from a live `QueryEvalContext` (a valid `QueryEvalCtx`
    // with exclusive access, since `ctx` is `&mut`) and `n` from a live
    // `QueryNodeMut` (a valid `RSQueryNode` we hold exclusively, so C may mutate
    // it), satisfying `Query_EvalNode`'s contract. `config` points to a live
    // `Config` valid for the duration of the call.
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
    child: QueryNodeMut<'_>,
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

#[cfg(test)]
mod _test_link {
    extern crate redisearch_rs;
    redis_mock::mock_or_stub_missing_redis_c_symbols!();
}
