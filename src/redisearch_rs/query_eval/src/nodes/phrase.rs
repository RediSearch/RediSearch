/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_PHRASE` query nodes.

use std::ptr::NonNull;

use rqe_iterators::{
    Empty,
    interop::RQEIteratorWrapper,
    intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator},
};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, eval_child_iterator, eval_node};

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
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    exact: bool,
    config: Config,
) -> Option<Evaluated<'index>> {
    let num_children = node.num_children();
    let node_mask = node.opts().field_mask;
    // A single-child intersection is just the child; return it directly.
    if num_children == 1 {
        let mut child = node.child_mut(0);
        child.and_field_mask(node_mask);
        return eval_node(ctx, child, config);
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
        children.push(eval_child_iterator(ctx, child, config));
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
