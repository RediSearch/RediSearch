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

use std::{ffi::c_char, ptr::NonNull};

use query::MetricRequestId;
use query_error::QueryErrorCode;
use query_types::QueryNodeFlags;
use rqe_iterators::{
    c2rust::CRQEIterator,
    index_load::{self, IndexLoad},
};

use crate::{Evaluated, QueryEvalContext, QueryNodeRef};

/// Reserve the metric request of `node`'s alias, if it has one.
///
/// Reserved before anything can find the clause empty, so the pipeline learns
/// the alias even when nothing matches and the request stays unbound.
///
/// Returns `None`, with the reason reported into the query
/// [`status`](QueryEvalContext::status), when the clause is rejected. A clause
/// under a `NOT` is: the documents a `NOT` yields are the ones the clause did
/// not match, so there is no value to load.
pub(crate) fn reserve(
    ctx: &mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Option<Option<MetricRequestId>> {
    if !loads(node) {
        return Some(None);
    }
    let alias = alias(node);
    if ctx.in_not_sub_tree() {
        ctx.status().set_error(
            QueryErrorCode::BadAttr,
            "`$as` is not supported under a negation",
        );
        return None;
    }
    ctx.count_loading_clause();
    // SAFETY: (1) `alias` is the NUL-terminated name the parser stored on the
    // node's options. (2) The request borrows it; the node owns it, and the AST
    // outlives the metric-request array.
    Some(Some(unsafe {
        ctx.add_metric_request(alias.as_ptr(), false)
    }))
}

/// Wrap `evaluated` in an index-loading decorator bound to `request`, or hand
/// it back unchanged when the clause loads nothing.
///
/// Returns `None`, with the reason reported into the query
/// [`status`](QueryEvalContext::status), when the clause's iterator is of a
/// type the decorator may not report.
pub(crate) fn decorate<'index>(
    ctx: &mut QueryEvalContext,
    request: Option<MetricRequestId>,
    evaluated: Evaluated<'index>,
) -> Option<Evaluated<'index>> {
    let Some(request) = request else {
        return Some(evaluated);
    };
    // SAFETY: (1)–(4) `into_c_iterator` yields a valid, owning iterator handle
    // with every required callback populated and callable.
    let child = unsafe { CRQEIterator::new(evaluated.into_c_iterator()) };
    let mut decorator = match IndexLoad::new(child) {
        Ok(decorator) => decorator,
        Err(type_) => {
            ctx.status().set_error(
                QueryErrorCode::BadAttr,
                &format!("`$as` is not supported on a {type_} iterator"),
            );
            return None;
        }
    };
    // SAFETY: (1) `request` was reserved on `ctx` by `reserve`. (2) The slot is
    // a heap cell the decorator owns, so it stays valid across moves of the
    // decorator until it is freed. (3) The handle is installed on the decorator
    // just below. (4) The AST, which owns the metric-request array, outlives
    // the iterator tree.
    let handle = unsafe { ctx.bind_metric_request_key(request, decorator.key_slot().as_ptr()) };
    // SAFETY: (1) `handle` lives as long as the AST, and only this decorator,
    // which clears its validity flag when freed, writes it.
    unsafe { decorator.set_handle(Some(handle)) };
    Some(Evaluated::RustCompound(index_load::boxed_new(decorator)))
}

/// The alias of a loading `node`.
const fn alias(node: &QueryNodeRef) -> NonNull<c_char> {
    NonNull::new(node.opts().dist_field).expect("a loading node carries its alias")
}

/// Whether `node` itself loads a value.
fn loads(node: &QueryNodeRef) -> bool {
    node.opts().flags.contains(QueryNodeFlags::LoadsValue)
}
