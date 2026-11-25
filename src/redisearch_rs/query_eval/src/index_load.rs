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
//! The parser marks a loading node with [`QueryNodeFlags::LoadsValue`] and
//! stores the alias on its
//! [`dist_field`](query_types::QueryNodeOptions::dist_field), and admits it
//! only on numeric nodes while unstable features are enabled. Evaluation
//! reserves a metric request under the alias and wraps the clause's iterator
//! in an [`IndexLoad`](rqe_iterators::index_load::IndexLoad) decorator bound
//! to it.

use field_spec::FieldSpec;
use numeric_range_tree::NumericRangeTree;
use query::MetricRequestId;
use query_error::QueryErrorCode;
use query_types::QueryNodeFlags;
use rqe_iterators::{c2rust::CRQEIterator, index_load};

use crate::{Evaluated, QueryEvalContext, QueryNodeRef, metric_binding::bind_metric_request};

/// The clause was rejected, and the reason reported into the query status.
pub(crate) struct Rejected;

/// Reserve the metric request of `node`'s alias, if it has one.
///
/// Reserved before anything can find the clause empty, so the pipeline learns
/// the alias even when nothing matches and the request stays unbound.
///
/// A clause under a `NOT` is rejected: the documents a `NOT` yields are the
/// ones the clause did not match, so there is no value to load.
pub(crate) fn reserve(
    ctx: &mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Result<Option<MetricRequestId>, Rejected> {
    if !loads(node) {
        return Ok(None);
    }
    let alias = node.opts().dist_field;
    debug_assert!(!alias.is_null(), "a loading node carries its alias");
    if ctx.in_not_sub_tree() {
        ctx.status().set_error(
            QueryErrorCode::BadAttr,
            "`$as` is not supported under a negation",
        );
        return Err(Rejected);
    }
    ctx.count_loading_clause();
    // SAFETY: `alias` is non-null, as on every loading node, and is the
    // NUL-terminated name the parser stored on the node's options. The
    // request borrows it; the node owns it, and the AST outlives the
    // metric-request array.
    Ok(Some(unsafe { ctx.add_metric_request(alias, false) }))
}

/// Whether `field`'s numeric range tree was built with float compression,
/// which may store a value other than the indexed one, so loading from it is
/// rejected by [`reject_lossy`]. A field with no tree yet has nothing to load.
///
/// The codec is the tree's own, fixed when indexing creates it. Trees are not
/// persisted: on RDB load they are rebuilt from the keyspace under the
/// `search-_numeric-compress` value current at that time, so a restart can
/// change the answer either way, and the answer always matches the tree.
pub(crate) fn compresses_floats(field: &ffi::FieldSpec) -> bool {
    // SAFETY: `field` is a field of the live spec, which the query's spec read
    // lock keeps alive and unmodified, names included.
    unsafe { FieldSpec::from_raw(field) }
        .tree()
        .is_some_and(NumericRangeTree::compresses_floats)
}

/// Reject a loading clause on a field that [`compresses_floats`].
pub(crate) fn reject_lossy(ctx: &mut QueryEvalContext) {
    ctx.status().set_error(
        QueryErrorCode::Inval,
        "`$as` cannot load a numeric field indexed with `search-_numeric-compress`",
    );
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
    // SAFETY: `into_c_iterator` yields a valid, owning iterator handle with
    // all callbacks populated — exactly the precondition of `CRQEIterator::new`.
    let child = unsafe { CRQEIterator::new(evaluated.into_c_iterator()) };
    let it = index_load::boxed_new(child);
    // SAFETY: `it` was just built by its constructor and nothing else holds it;
    // `request` was reserved on `ctx` by `reserve`; the AST, which owns the
    // metric-request array, outlives the iterator tree.
    unsafe { bind_metric_request(ctx, request, it) };
    Evaluated::RustCompound(it)
}

/// Whether `node` itself loads a value.
fn loads(node: &QueryNodeRef) -> bool {
    node.opts().flags.contains(QueryNodeFlags::LoadsValue)
}
