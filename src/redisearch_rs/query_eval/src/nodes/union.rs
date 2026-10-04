/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_UNION` query nodes.

use rqe_iterators::{c2rust::CRQEIterator, union_opaque::build_union};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, eval_child_iterator};

/// `QN_UNION` — a logical OR over its children (matches any document matched by
/// at least one child).
///
/// The children are evaluated and combined with the Rust union iterator. Each
/// child's field mask is first intersected with the union node's own mask, and
/// `quick_exit` is enabled when the union only needs the matching id set rather
/// than per-child scores — i.e. inside a `NOT` subtree or when the node's weight
/// is zero.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
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
    let children: Vec<CRQEIterator> = (0..num_children)
        .map(|i| {
            let mut child = node.child_mut(i);
            child.and_field_mask(node_mask);
            eval_child_iterator(ctx, child, config)
        })
        .collect();

    let iter = build_union(children, quick_exit, min_union_iter_heap, node_type, weight);

    Evaluated::RustCompound(iter)
}
