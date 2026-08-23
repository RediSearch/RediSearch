/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_NOT` query nodes.

use std::ptr::NonNull;

use rqe_iterators::{
    interop::RQEIteratorWrapper,
    not_reducer::{NewNotIterator, new_not_iterator},
};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, eval_child_iterator};

/// `QN_NOT` — logical negation: matches every document *not* matched by its
/// single child.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
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
    let child = eval_child_iterator(ctx, node.child_mut(0), config);
    ctx.set_in_not_sub_tree(prev_in_not_sub_tree);

    // SAFETY: invariant (2) of `QueryEvalContext::new` guarantees that both things the
    // returned context may point at — `bcTimeoutAreq` and `sctx` — outlive every timeout
    // context derived from `ctx`, and the returned context is handed straight to
    // `new_not_iterator` below (never retained past this query), so it cannot be used after
    // either is freed. Writes to `sctx.time.timeout` never overlap a probe (see
    // `TimeoutContextDeadline::new`).
    let timeout_ctx = unsafe { ctx.build_timeout_context() };

    // SAFETY: the preconditions of `new_not_iterator` map to
    // `QueryEvalContext::new` invariants:
    // 1. `query` is a valid, non-null `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is valid and non-null — invariant (2).
    // 3. `query.sctx.spec` is valid and non-null — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1).
    // 5. The wildcard-iterator preconditions hold for the same reasons
    //    as in `wildcard::eval` (a properly initialised spec with its
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
