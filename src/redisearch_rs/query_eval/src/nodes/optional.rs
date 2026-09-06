/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_OPTIONAL` query nodes.

use std::ptr::NonNull;

use rqe_iterators::{
    interop::RQEIteratorWrapper,
    optional_reducer::{NewOptionalIterator, new_optional_iterator},
};

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeMut, eval_child_iterator};

/// `QN_OPTIONAL` — an optional match that boosts the score when its single
/// child matches but does not exclude documents that don't.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    mut node: QueryNodeMut<'_>,
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
    let child = eval_child_iterator(ctx, node.child_mut(0), config);

    // SAFETY: the preconditions of `new_optional_iterator` map to
    // `QueryEvalContext::new` invariants:
    // 1. `query` is a valid, non-null `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is valid and non-null — invariant (2).
    // 3. `query.sctx.spec` is valid and non-null — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1).
    // 5-7. The wildcard-iterator preconditions hold for the same reasons
    //    as in `wildcard::eval` (a properly initialised spec with its
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
