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
//! [`QueryIteratorTree::new`] entry point, and the helpers for evaluating a
//! child node ([`eval_child_iterator`]) and lowering an evaluated one
//! ([`into_child_iterator`]).

use std::{marker::PhantomData, ptr::NonNull};

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
    fuzzy, geo, geometry, ids, missing, not, null, numeric, optional, phrase, prefix, tag, token,
    union, vector, wildcard, wildcard_query,
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
/// - [`Evaluated::C`] — an iterator *built by* a C constructor a ported node
///   calls. Handed straight back to C so the C-side
///   optimizer/profiler keep seeing the original iterator.
/// - [`Evaluated::RustCompound`] — an owning C-ABI handle that Rust built and
///   already lowered, returned as-is rather than as a trait object (see the
///   variant docs for the two cases that need this shape).
// TODO: Remove this enum once C iterator constructors no longer return owning
// raw handles to the evaluator.
#[must_use = "an unconsumed `Evaluated` may leak its owning iterator handle; consume it via `into_c_iterator` or `into_boxed`"]
pub enum Evaluated<'index> {
    /// An iterator implemented in Rust, held as a boxed trait object.
    ///
    /// Lowered to the C ABI lazily (via [`RQEIteratorWrapper::boxed_new`]) only
    /// if and when it crosses back to C.
    RustLeaf(EvalResult<'index>),

    /// An owning C iterator handle built by a C constructor called by a ported
    /// node (e.g. [`ffi::NewVectorIterator`]).
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
                // a C iterator constructor, `Evaluated::RustCompound` from
                // `RQEIteratorWrapper::boxed_new_compound` — exactly the
                // preconditions of `CRQEIterator::new`.
                Box::new(unsafe { CRQEIterator::new(it) })
            }
        }
    }
}

/// The executable iterator tree built from a parsed query AST.
///
/// A dedicated type rather than a bare root iterator, so it can carry more as
/// the evaluator grows.
///
/// The tree borrows the [`QueryEvalContext`] it was built from for `'index`:
/// its iterators read index data reached through that context.
pub struct QueryIteratorTree<'index> {
    /// Owns every iterator in the tree, through its children.
    // Private, as a `CRQEIterator` carries no lifetime: safe code only gets it
    // out bound to `'index` (`into_root`) or as a raw pointer, which needs
    // `unsafe` to use beyond the borrow of the context.
    root: CRQEIterator,
    // FIXME: remove once CRQEIterator has been removed (MOD-14254) so `root` can carry the lifetime itself.
    _ctx: PhantomData<&'index mut QueryEvalContext>,
}

impl<'index> QueryIteratorTree<'index> {
    /// Build the executable iterator tree for a parsed query AST.
    ///
    /// Evaluates `root` via [`eval_node`], substituting an [`Empty`] iterator
    /// when it yields none.
    pub fn new(ctx: &'index mut QueryEvalContext, root: QueryNodeMut<'_>, config: Config) -> Self {
        Self::from_root(into_child_iterator(eval_node(ctx, root, config)))
    }

    const fn from_root(root: CRQEIterator) -> Self {
        Self {
            root,
            _ctx: PhantomData,
        }
    }

    /// Wrap every iterator in the tree in a profile iterator, via
    /// [`CRQEIterator::into_profiled`].
    ///
    /// # Panics
    ///
    /// Panics if the tree has already been profiled.
    pub fn into_profiled(self) -> Self {
        Self::from_root(self.root.into_profiled())
    }

    /// Consume the tree into its root iterator, which still borrows the
    /// context for `'index`.
    pub fn into_root(self) -> impl RQEIteratorPrintable<'index> + 'index {
        self.root
    }

    /// The root iterator, still owned by the tree.
    pub const fn root_ptr(&self) -> NonNull<ffi::QueryIterator> {
        self.root.as_raw()
    }

    /// Replace the root iterator with `root`, returning the previous one
    /// unfreed, typically because `root` already owns it as a child.
    pub fn replace_root(&mut self, root: CRQEIterator) -> NonNull<ffi::QueryIterator> {
        std::mem::replace(&mut self.root, root).into_raw()
    }

    /// Consume the tree into its owning root iterator handle.
    pub fn into_raw_root(self) -> NonNull<ffi::QueryIterator> {
        self.root.into_raw()
    }

    /// Detach the tree from the borrow of the context it was built from.
    ///
    /// # Safety
    ///
    /// For as long as the returned tree is alive:
    ///
    /// 1. The index data reached through the context it was built from must
    ///    stay valid, as required by the invariants of [`QueryEvalContext::new`].
    /// 2. No other iterator tree may be built from that context.
    pub unsafe fn erase_lifetime(self) -> QueryIteratorTree<'static> {
        QueryIteratorTree::from_root(self.root)
    }
}

/// Evaluate a single query node, producing the corresponding iterator.
///
/// Returns `None` when the node produces no results.
///
/// The node is taken as an **exclusive** [`QueryNodeMut`], by value. Evaluation
/// mutates the AST — it narrows children's field masks and normalizes tag,
/// prefix and wildcard tokens in place — so a shared borrow would be a lie.
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
        QueryNode::Missing { field_index } => {
            missing::eval(ctx, field_index).map(Evaluated::RustLeaf)
        }
        QueryNode::Optional => Some(optional::eval(ctx, node, config)),
        QueryNode::Not => Some(not::eval(ctx, node, config)),
        QueryNode::Phrase { exact } => phrase::eval(ctx, node, exact, config),
        QueryNode::Union => Some(union::eval(ctx, node, config)),
        QueryNode::Numeric { nf } => numeric::eval(ctx, nf, config),
        QueryNode::Geo { gf } => geo::eval(ctx, gf, config),
        QueryNode::Token { tok } => token::eval(ctx, &node, tok, config),
        QueryNode::Geometry { geomq } => geometry::eval(ctx, geomq),
        QueryNode::Vector { vq } => vector::eval(ctx, node, vq, config),
        QueryNode::Prefix { tok, mode } => prefix::eval(ctx, &node, tok, mode, config),
        QueryNode::Fuzzy { tok, max_dist } => fuzzy::eval(ctx, &node, tok, max_dist, config),
        QueryNode::Tag { field_index } => tag::eval(ctx, node, field_index, config),
        // Binds nothing, so the node stays free to be passed on by value —
        // evaluation rewrites its token.
        QueryNode::WildcardQuery { .. } => wildcard_query::eval(ctx, node, config),
    }
}

/// Evaluate a child node into an owning [`CRQEIterator`] for use as a child of
/// a Rust compound iterator, lowered by [`into_child_iterator`].
fn eval_child_iterator(
    ctx: &mut QueryEvalContext,
    child: QueryNodeMut<'_>,
    config: Config,
) -> CRQEIterator {
    into_child_iterator(eval_node(&mut *ctx, child, config))
}

/// Lower an evaluated node into an owning [`CRQEIterator`], for use as a child
/// of a Rust compound iterator or as the root of a [`QueryIteratorTree`].
///
/// A `None` node (no results) becomes a freshly boxed [`Empty`] so a reducer
/// can apply its empty-child rules, since a missing child is equivalent to one
/// that matches nothing.
fn into_child_iterator(evaluated: Option<Evaluated<'_>>) -> CRQEIterator {
    let ptr = match evaluated {
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
