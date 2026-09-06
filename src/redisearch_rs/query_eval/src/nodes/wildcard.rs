/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_WILDCARD` query nodes.

use crate::{Evaluated, QueryEvalContext, QueryNodeRef};

/// `QN_WILDCARD` — the `*` query that matches every document.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
) -> Evaluated<'index> {
    let weight = node.opts().weight;
    // SAFETY: `new_wildcard_iterator` preconditions map to
    // `QueryEvalContext::new` invariants as follows:
    // 1. `query` is a valid `QueryEvalCtx` — invariant (1).
    // 2. `query.sctx` is a valid, non-null `RedisSearchCtx` — invariant (2).
    // 3. `query.sctx.spec` is a valid, non-null `IndexSpec` — invariant (2).
    // 4. `spec.rule`, when non-null, is a valid `SchemaRule` — part of (1),
    //    a properly initialised `QueryEvalCtx` is built from a valid spec.
    // 5. `new_wildcard_iterator_optimized` preconditions hold when
    //    `rule.index_all` is true — the spec's `existingDocs` inverted
    //    index is initialised during `IndexSpec_Init`.
    // 6. `query.docTable` is a valid, non-null `DocTable` — invariant (2).
    // 7. `spec.diskSpec`, when non-null, is a valid
    //    `RedisSearchDiskIndexSpec` — part of (1).
    // 8. `SEARCH_ENTERPRISE_ITERATORS` is initialised when `diskSpec` is
    //    non-null — the enterprise module sets it during `OnLoad`.
    let it = unsafe { rqe_iterators::wildcard::new_wildcard_iterator(ctx.as_non_null(), weight) };
    Evaluated::RustLeaf(Box::new(it))
}
