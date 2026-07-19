/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable bindings for the Rust query-evaluation dispatcher
//! ([`query_eval::eval`]).

use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
};

use ffi::{
    AREQ, QueryAST, QueryError, QueryEvalCtx, QueryIterator, RSQueryNode, RSSearchOptions,
    RedisSearchCtx,
};
use query_eval::{
    QueryEvalContext, QueryNodeRef, eval,
    eval::Config,
    scorers::{BuiltInScorer, slop_forces_offsets},
};
use query_types::QueryNodeOptions;

/// Snapshot the evaluator's configuration from the process-wide
/// [`ffi::RSGlobalConfig`].
///
/// This is the single point where the query evaluator reads the global config;
/// the resulting [`Config`] is threaded through evaluation as a parameter.
fn eval_config() -> Config {
    // SAFETY: `RSGlobalConfig` is the process-wide config instance, fully
    // initialised before any query is evaluated, and read-only here. Each field
    // is a `Copy` scalar read directly out of the static, without forming a
    // reference to it.
    let numeric_compress = unsafe { ffi::RSGlobalConfig.numericCompress };
    // SAFETY: as above.
    let prioritize_intersect_union_children =
        unsafe { ffi::RSGlobalConfig.prioritizeIntersectUnionChildren };
    // SAFETY: as above. `defaultScorer` is a `Copy` pointer to the process-wide
    // default scorer name, null when unset. It is read (not retained) here to
    // resolve the built-in `Scorer` once, so `Config` carries no raw pointer.
    let default_scorer_ptr = unsafe { ffi::RSGlobalConfig.defaultScorer };
    let default_scorer = NonNull::new(default_scorer_ptr.cast_mut()).and_then(|ptr| {
        // SAFETY: `defaultScorer` is non-null here and points to a valid
        // NUL-terminated C string owned by the process-wide config.
        let name = unsafe { CStr::from_ptr(ptr.as_ptr()) };
        // A non-UTF-8 or custom (non-built-in) default resolves to `None`, which
        // the evaluator treats the same as an unset default.
        BuiltInScorer::from_c_str(name)
    });

    Config {
        numeric_compress,
        prioritize_intersect_union_children,
        default_scorer,
    }
}

/// Resolve a C scorer name to a built-in [`BuiltInScorer`], applying the configured
/// default when `scorer_name` is null.
///
/// Returns [`None`] when the resolved name is unset or not a built-in name (a
/// custom scorer) — cases the caller treats conservatively (as needing term
/// offsets).
///
/// # Safety
///
/// `scorer_name` must be null or a valid NUL-terminated C string.
unsafe fn resolve_scorer(scorer_name: *const c_char) -> Option<BuiltInScorer> {
    // A null scorer name means "use the configured default scorer".
    let name = if scorer_name.is_null() {
        // SAFETY: `RSGlobalConfig` is the process-wide config, read-only here;
        // `defaultScorer` is a `Copy` pointer read directly out of the static.
        unsafe { ffi::RSGlobalConfig.defaultScorer }
    } else {
        scorer_name
    };

    NonNull::new(name.cast_mut()).and_then(|ptr| {
        // SAFETY: `name` is non-null here and points to a valid NUL-terminated C
        // string: either `scorer_name` (by this function's contract) or, in the
        // default branch, `RSGlobalConfig.defaultScorer`, which the config layer
        // guarantees is a valid NUL-terminated C string.
        BuiltInScorer::from_c_str(unsafe { CStr::from_ptr(ptr.as_ptr()) })
    })
}

/// Whether the scorer named `scorer_name` needs term offset data.
///
/// A null `scorer_name` falls back to the configured default scorer
/// ([`ffi::RSGlobalConfig`]'s `defaultScorer`), and a custom or
/// otherwise unrecognised name conservatively needs offsets.
///
/// # Safety
///
/// `scorer_name` must be null or a valid NUL-terminated C string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn scorerNeedsOffsets(scorer_name: *const c_char) -> bool {
    // SAFETY: `scorer_name` upholds this function's contract (null or a valid
    // NUL-terminated C string).
    let scorer = unsafe { resolve_scorer(scorer_name) };
    scorer.is_none_or(BuiltInScorer::needs_offsets)
}

/// Whether a query node needs term offset data.
///
/// # Safety
///
/// `scorer_name` must be null or a valid NUL-terminated C string; `opts` must be
/// null or point to a valid [`QueryNodeOptions`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn queryNeedsOffsets(
    scorer_name: *const c_char,
    opts: *const QueryNodeOptions,
) -> bool {
    // A phrase/slop constraint forces offsets regardless of the scorer, so check
    // it first and return before resolving the scorer — which would otherwise
    // read the process-wide default scorer needlessly. A null `opts` carries no
    // such constraint.
    // SAFETY: `opts` is null or a valid `QueryNodeOptions` (this function's contract).
    if let Some(opts) = unsafe { opts.as_ref() }
        && slop_forces_offsets(opts.max_slop, opts.in_order)
    {
        return true;
    }
    // No phrase/slop constraint: the scorer alone decides.
    // SAFETY: `scorer_name` upholds this function's contract (null or a valid
    // NUL-terminated C string).
    unsafe { scorerNeedsOffsets(scorer_name) }
}

/// Evaluate a single query AST node, producing the corresponding
/// [`QueryIterator`].
///
/// Returns a null pointer when the node produces no results (e.g. a
/// missing-field node for a field that has no missing values).
///
/// # Safety
///
/// 1. `q` must be a non-null pointer to a valid [`QueryEvalCtx`] that satisfies
///    all the invariants documented on [`QueryEvalContext::new`] and remains
///    valid for the lifetime of the returned iterator.
/// 2. `n` must be a non-null pointer to a valid [`RSQueryNode`].
#[unsafe(no_mangle)]
// TODO: remove the '_Rs' suffix once fully ported.
pub unsafe extern "C" fn Query_EvalNode_Rs(
    q: *mut QueryEvalCtx,
    n: *mut RSQueryNode,
) -> *mut QueryIterator {
    let q = NonNull::new(q).expect("Query_EvalNode_Rs: q is null");
    let n = NonNull::new(n).expect("Query_EvalNode_Rs: n is null");

    // SAFETY: `q` is a non-null pointer to a valid `QueryEvalCtx` upholding the
    // `QueryEvalContext::new` invariants (precondition 1). The wrapper borrows
    // it exclusively for the duration of this call.
    let mut ctx = unsafe { QueryEvalContext::new(q) };
    // SAFETY: `n` is a non-null pointer to a valid `RSQueryNode` (precondition 2),
    // borrowed only for the duration of this call.
    let node = unsafe { QueryNodeRef::new(n) };

    match eval::eval_node(&mut ctx, &node, eval_config()) {
        // The returned handle is heap-allocated and self-owning; erasing its
        // borrow is sound because the index data it reads outlives it
        // (precondition 1).
        Some(it) => it.into_c_iterator(),
        None => std::ptr::null_mut(),
    }
}

/// Build the executable iterator tree for a parsed query AST and return its
/// root [`QueryIterator`].
///
/// Assembles the [`QueryEvalCtx`] from the request pieces, then evaluates
/// `qast`'s root node. The returned pointer is never NULL — an empty iterator
/// is substituted when the query produces no results.
///
/// # Safety
///
/// 1. `qast` must be a non-null pointer to a valid [`QueryAST`] whose `root` is
///    a valid [`RSQueryNode`]; it (and its `metricRequests`/`config` fields)
///    must stay valid and exclusively borrowed for the duration of the call.
/// 2. `opts` must be a non-null pointer to a valid [`RSSearchOptions`].
/// 3. `sctx` must be a non-null pointer to a valid [`RedisSearchCtx`] whose
///    `spec` is a valid, non-null [`IndexSpec`](ffi::IndexSpec).
/// 4. `status` must be a non-null pointer to a valid [`QueryError`].
/// 5. `areq`, when non-null, must point to a valid [`AREQ`].
///
/// Together these are exactly the invariants documented on
/// [`QueryEvalContext::new`] for the assembled context, which remains valid for
/// the lifetime of the returned iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QAST_Iterate(
    qast: *mut QueryAST,
    opts: *const RSSearchOptions,
    sctx: *mut RedisSearchCtx,
    reqflags: u32,
    areq: *mut AREQ,
    status: *mut QueryError,
) -> *mut QueryIterator {
    // SAFETY: `qast` is a valid, non-null pointer (precondition 1), held
    // exclusively for the duration of the call.
    let qast = unsafe { &mut *qast };
    // SAFETY: `sctx` is a valid, non-null pointer (precondition 3).
    let spec = unsafe { (*sctx).spec };

    // Wire the Blocked Client Timeout dispatch to this request only when it
    // opted into `skipTimeoutChecks`; otherwise leave it unset so iterators use
    // the clock-based timeout instead.
    // SAFETY: `areq`, when non-null (checked first), is a valid `AREQ`
    // (precondition 5).
    let bc_timeout_areq = if !areq.is_null() && unsafe { (*areq).skipTimeoutChecks } {
        areq
    } else {
        std::ptr::null_mut()
    };

    // Assemble the evaluation context for this query. `tokenId` starts at 0 and
    // is bumped as token iterators are created during evaluation.
    let mut qectx = QueryEvalCtx {
        sctx,
        opts,
        status,
        metricRequestsP: &raw mut qast.metricRequests,
        tokenId: 0,
        // SAFETY: `spec` is a valid, non-null `IndexSpec` (precondition 3).
        docTable: unsafe { &raw mut (*spec).docs },
        reqFlags: reqflags,
        config: &raw mut qast.config,
        inNotSubTree: false,
        bcTimeoutAreq: bc_timeout_areq,
    };

    let root = NonNull::new(qast.root).expect("QAST_Iterate: qast root is null");
    let q = NonNull::from(&mut qectx);

    // SAFETY: `q` points to the freshly built, valid `QueryEvalCtx` above; its
    // pointer fields satisfy the `QueryEvalContext::new` invariants per the
    // preconditions, and it is borrowed exclusively for the call.
    let mut ctx = unsafe { QueryEvalContext::new(q) };
    // SAFETY: `root` is a valid `RSQueryNode` (precondition 1).
    let node = unsafe { QueryNodeRef::new(root) };

    // The returned handle is heap-allocated and self-owning; erasing its borrow
    // of the transient `qectx` is sound because the index data it reads
    // (reachable via `sctx`/`spec`) outlives it.
    eval::qast_iterate(&mut ctx, &node, eval_config()).into_c_iterator()
}
