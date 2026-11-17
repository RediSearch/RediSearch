/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_SAME_ELEMENT` query nodes.

use std::ptr::NonNull;

use inverted_index::NumericFilter;
use query_error::QueryErrorCode;
use query_types::QueryNodeType;
use rqe_iterators::{
    Candidates, ElementPredicate, Empty, QueryElementFilter, SortableElementValues,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    intersection::{Intersection, NewIntersectionIterator, new_intersection_iterator},
    inverted_index::TagElements,
    union_opaque::build_union,
};

use crate::{
    Config, Evaluated, OffsetRequirement, QueryEvalContext, QueryNode, QueryNodeMut, QueryNodeRef,
    eval_child_iterator, into_child_iterator, nodes::tag,
};

/// `QN_SAME_ELEMENT` — conditions on the members of the NESTED group at `group` that must all
/// hold on one element of the group's array.
///
/// The children are split by [`GroupChildren`] and evaluated into one
/// [`ElementFilter`](rqe_iterators::ElementFilter).
///
/// The query parser rejects the children that `QueryNode_SameElementChild` does not accept. A
/// node that reaches evaluation with such a child fails the query, through
/// [`status`](QueryEvalContext::status).
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
    group: u16,
    config: Config,
) -> Option<Evaluated<'index>> {
    let Some(children) = GroupChildren::split(ctx.spec(), group, &node) else {
        ctx.status().set_error(
            QueryErrorCode::Generic,
            "Unexpected condition in a NESTED group",
        );
        return None;
    };

    let iterators: Vec<CRQEIterator> = match children.candidates {
        Candidates::CommonPositions => children
            .tags
            .iter()
            .map(|&i| eval_tags(ctx, node.child_mut(i), config))
            .collect(),
        // No child reports elements, so the numeric indexes find the documents.
        Candidates::AllElements => (0..node.num_children())
            .map(|i| {
                eval_child_iterator(ctx, node.child_mut(i), config, OffsetRequirement::Automatic)
            })
            .collect(),
    };

    let weight = node.opts().weight;
    let documents = match new_intersection_iterator(iterators) {
        NewIntersectionIterator::Empty => return Some(Evaluated::RustLeaf(Box::new(Empty))),
        NewIntersectionIterator::Single(child) => child,
        NewIntersectionIterator::Proceed(cs) => {
            // The filter picks the elements to test, so the intersection only has to agree
            // on the document.
            let intersection = RQEIteratorWrapper::boxed_new_compound(Intersection::new(
                cs,
                weight,
                config.prioritize_intersect_union_children,
            ));
            // SAFETY: `boxed_new_compound` returns a valid, owning `QueryIterator` with
            // all callbacks populated.
            unsafe { CRQEIterator::new(intersection) }
        }
    };

    let doc_table =
        NonNull::new(ctx.doc_table_mut()).expect("a query context holds a document table");
    // SAFETY: the document table stays valid for as long as the iterator tree
    // (`QueryEvalContext` invariant 2), and iterators only run under the spec read lock.
    let values = unsafe { SortableElementValues::new(doc_table) };
    // SAFETY: as in `not::eval`: `sctx` and its timeout outlive the iterator tree that the
    // context goes into.
    let timeout_ctx = unsafe { ctx.build_timeout_context() };
    let filter = QueryElementFilter::new(
        documents,
        weight,
        children.candidates,
        children.predicates,
        values,
        timeout_ctx,
    );
    Some(Evaluated::RustCompound(
        RQEIteratorWrapper::boxed_new_compound(filter),
    ))
}

/// Evaluate a child that is `SameElementChild_Tags` with [`TagElements::Reported`].
fn eval_tags(
    ctx: &mut QueryEvalContext,
    mut child: QueryNodeMut<'_>,
    config: Config,
) -> CRQEIterator {
    match child.as_enum() {
        QueryNode::Tag { field_index } => {
            let reported = TagElements::Reported;
            into_child_iterator(tag::eval(ctx, child, field_index, config, reported))
        }
        QueryNode::Union => {
            let alternatives = (0..child.num_children())
                .map(|i| eval_tags(ctx, child.child_mut(i), config))
                .collect();
            let union = build_union(
                alternatives,
                false,
                config.min_union_iter_heap,
                QueryNodeType::Union,
                child.opts().weight,
            );
            into_child_iterator(Some(Evaluated::RustCompound(union)))
        }
        _ => unreachable!("`QueryNode_SameElementChild` accepted the child"),
    }
}

/// The children of a same-element node, split by their `SameElementChild` kind.
struct GroupChildren {
    /// The `SameElementChild_Predicate` children.
    predicates: Vec<ElementPredicate>,
    /// Indexes of the `SameElementChild_Tags` children.
    tags: Vec<usize>,
    /// The elements that [`predicates`](Self::predicates) are tested on.
    candidates: Candidates,
}

impl GroupChildren {
    /// Returns `None` when the node has no child, or a child that
    /// `QueryNode_SameElementChild` does not accept.
    fn split(spec: &ffi::IndexSpec, group: u16, node: &QueryNodeRef) -> Option<Self> {
        let mut predicates = Vec::new();
        let mut tags = Vec::new();
        for (i, child) in node.children().enumerate() {
            // SAFETY: `spec` and the node are live for the evaluation, under the spec read
            // lock, and the function only reads them.
            let kind = unsafe {
                ffi::QueryNode_SameElementChild(spec, group, child.as_non_null().as_ptr())
            };
            match (kind, child.as_enum()) {
                (ffi::SameElementChild_SameElementChild_Predicate, QueryNode::Numeric { nf }) => {
                    predicates.push(element_predicate(spec, nf));
                }
                (ffi::SameElementChild_SameElementChild_Tags, _) => tags.push(i),
                _ => return None,
            }
        }
        let candidates = if !tags.is_empty() {
            Candidates::CommonPositions
        } else if !predicates.is_empty() {
            Candidates::AllElements
        } else {
            return None;
        };
        Some(Self {
            predicates,
            tags,
            candidates,
        })
    }
}

/// The range `nf`, on a NUMERIC member of the group, as a test on the value that the member
/// stores for an element in the sorting vector.
fn element_predicate(spec: &ffi::IndexSpec, nf: &NumericFilter) -> ElementPredicate {
    // SAFETY: `QueryNode_SameElementChild` found the field of `nf` in the `numFields`-sized
    // array that `spec.fields` points to.
    let field_ptr = unsafe { spec.fields.add(nf.field_index as usize) };
    // SAFETY: `field_ptr` points into the live spec's field array, which the query's spec
    // read lock keeps alive and unmodified throughout evaluation.
    let field = unsafe { field_ptr.as_ref() }.expect("a spec with fields has a field array");
    ElementPredicate {
        filter: *nf,
        slot: usize::try_from(field.sortIdx)
            .expect("a NUMERIC member of a NESTED group has a sorting index"),
    }
}
