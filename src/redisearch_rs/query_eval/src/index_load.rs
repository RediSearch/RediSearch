/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of index loading, the `$as` query attribute on a numeric clause.
//!
//! The parser admits it only on numeric nodes while unstable features are
//! enabled, marking the node with [`QueryNodeFlags::LoadsValue`] and storing
//! the alias on its [`dist_field`](query_types::QueryNodeOptions::dist_field).
//! Evaluation reserves a metric request under the alias and wraps the clause's
//! iterator in an [`IndexLoad`] decorator bound to it.

use query::MetricRequestId;
use query_error::QueryErrorCode;
use query_types::QueryNodeFlags;
use rqe_iterators::index_load::{self, IndexLoad};

use crate::{Evaluated, QueryEvalContext, QueryNodeRef, into_child_iterator};

/// Reserve the metric request of `node`'s alias, if it has one.
///
/// Reserved before anything can find the clause empty, so the pipeline learns
/// the alias even when nothing matches and the request stays unbound.
///
/// Returns `None`, with the reason reported into the query
/// [`status`](QueryEvalContext::status), when the clause is under a `NOT`: the
/// documents a `NOT` yields are the ones the clause did not match, so there is
/// no value to load.
pub(crate) fn reserve(
    ctx: &mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Option<Option<MetricRequestId>> {
    if !loads(node) {
        return Some(None);
    }
    if ctx.in_not_sub_tree() {
        ctx.status().set_error(
            QueryErrorCode::BadAttr,
            "`$as` is not supported under a negation",
        );
        return None;
    }
    let alias = node.opts().dist_field;
    debug_assert!(!alias.is_null(), "a loading node carries its alias");
    // SAFETY: (1) `alias` is the NUL-terminated name the parser stored on the
    // node's options. (2) The request borrows it; the node owns it, and the AST
    // outlives the metric-request array.
    Some(Some(unsafe { ctx.add_metric_request(alias, false) }))
}

/// Wrap `evaluated` in an index-loading decorator bound to `request`, or hand
/// it back unchanged when the clause loads nothing.
pub(crate) fn decorate<'index>(
    ctx: &mut QueryEvalContext,
    request: Option<MetricRequestId>,
    evaluated: Evaluated<'index>,
) -> Evaluated<'index> {
    let Some(request) = request else {
        return evaluated;
    };
    let mut decorator = IndexLoad::new(into_child_iterator(Some(evaluated)));
    // SAFETY: (1) `request` was reserved on `ctx` by `reserve`. (2) The key slot
    // is the decorator's own, which keeps its address until the decorator is
    // freed. (3) The handle is installed on the decorator just below. (4) The
    // AST, which owns the metric-request array, outlives the iterator tree.
    let handle = unsafe { ctx.bind_metric_request_key(request, decorator.key_slot()) };
    // SAFETY: (1) `handle` lives as long as the AST, and only this decorator,
    // which clears its validity flag when freed, writes it.
    unsafe { decorator.set_handle(Some(handle)) };
    Evaluated::RustCompound(index_load::boxed_new(decorator))
}

/// Whether `node` itself loads a value.
fn loads(node: &QueryNodeRef) -> bool {
    node.opts().flags.contains(QueryNodeFlags::LoadsValue)
}

/// Whether `node` or a node under it loads a value.
pub(crate) fn subtree_loads(node: &QueryNodeRef) -> bool {
    loads(node) || node.children().any(|child| subtree_loads(&child))
}
