/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable bindings for the Rust query-evaluation dispatcher
//! ([`query_eval`]).

use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
};

use ffi::{QueryAST, QueryError, QueryEvalCtx, QueryIterator, RSSearchOptions, RedisSearchCtx};
use query_eval::{
    Config, QueryEvalContext, QueryNodeMut, qast_iterate,
    scorers::{BuiltInScorer, slop_forces_offsets},
};
use query_types::QueryNodeOptions;
use rqe_iterators::IteratorsConfig;

/// Snapshot the evaluator's configuration.
///
/// Fields that have no per-query snapshot are read from the process-wide config, each
/// through its [`global_config`] accessor. Anything covered by [`IteratorsConfig`] is
/// instead taken from `iterators` rather than the live global.
///
/// The resulting [`Config`] is threaded through evaluation as a parameter, so evaluation
/// itself never re-reads the global. [`resolve_scorer`] does, on the path that decides
/// whether a query needs term offsets, so a `CONFIG SET` landing between the two can still
/// leave that decision and this snapshot disagreeing.
fn eval_config(iterators: &IteratorsConfig) -> Config {
    // The default scorer is resolved (not retained) here, so `Config` carries no pointer
    // into config memory that a later `CONFIG SET` can free.
    let default_scorer = global_config::default_scorer().and_then(|ptr| {
        // SAFETY: `ptr` points to the configured default scorer name, a valid
        // NUL-terminated C string owned by the process-wide config.
        let name = unsafe { CStr::from_ptr(ptr.as_ptr()) };
        // A non-UTF-8 or custom (non-built-in) default resolves to `None`, which
        // the evaluator treats the same as an unset default.
        BuiltInScorer::from_c_str(name)
    });

    Config {
        numeric_compress: global_config::numeric_compress(),
        prioritize_intersect_union_children: global_config::prioritize_intersect_union_children(),
        default_scorer,
        min_term_prefix: iterators.min_term_prefix,
        max_prefix_expansions: iterators.max_prefix_expansions as usize,
        min_union_iter_heap: iterators.min_union_iter_heap as usize,
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
        global_config::default_scorer()
    } else {
        NonNull::new(scorer_name.cast_mut())
    };

    name.and_then(|ptr| {
        // SAFETY: `ptr` is non-null and points to a valid NUL-terminated C string:
        // either `scorer_name` (by this function's contract) or, in the default
        // branch, the configured default scorer name, which the config layer
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
///    a valid [`RSQueryNode`](ffi::RSQueryNode); it (and its
///    `metricRequests`/`config` fields) must stay valid and exclusively borrowed
///    for the duration of the call. The root's subtree must also satisfy
///    invariants (4) and (5) of [`QueryNodeMut::new`], since evaluation
///    rewrites tokens in place.
/// 2. `opts` must be a non-null pointer to a valid [`RSSearchOptions`].
/// 3. `sctx` must be a non-null pointer to a valid [`RedisSearchCtx`] whose
///    `spec` is a valid, non-null [`IndexSpec`](ffi::IndexSpec). `sctx` and the
///    request timeout reached through `sctx.timeout` must stay valid at stable
///    addresses for the lifetime of the returned iterator. Timeout source
///    changes and deadline writes may occur only between iterator probes;
///    only the blocked-client flag may change concurrently.
/// 4. `status` must be a non-null pointer to a valid [`QueryError`].
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
    status: *mut QueryError,
) -> *mut QueryIterator {
    // SAFETY: `qast` is a valid, non-null pointer (precondition 1), held
    // exclusively for the duration of the call.
    let qast = unsafe { &mut *qast };
    // SAFETY: `sctx` is a valid, non-null pointer (precondition 3).
    let spec = unsafe { (*sctx).spec };

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
    };

    let root = NonNull::new(qast.root).expect("QAST_Iterate: qast root is null");
    let q = NonNull::from(&mut qectx);

    // SAFETY: `q` points to the freshly built, valid `QueryEvalCtx` above; its
    // pointer fields satisfy the `QueryEvalContext::new` invariants per the
    // preconditions, and it is borrowed exclusively for the call.
    let mut ctx = unsafe { QueryEvalContext::new(q) };
    // SAFETY: `root` is a valid `RSQueryNode` and the whole AST is borrowed
    // exclusively for the duration of this call (precondition 1), so evaluation
    // owns the tree: no other wrapper or reference into it, or into any token
    // string it points at, is live. Precondition 1 also carries invariants (4) and
    // (5).
    let node = unsafe { QueryNodeMut::new(root) };

    let config = eval_config(ctx.config());
    // The returned handle is heap-allocated and self-owning; erasing its borrow
    // of the transient `qectx` is sound because the index data it reads
    // (reachable via `sctx`/`spec`) and request timeout outlive it (precondition 3).
    qast_iterate(&mut ctx, node, config).into_c_iterator()
}
