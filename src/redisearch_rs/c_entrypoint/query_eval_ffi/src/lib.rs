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

use std::{ffi::CStr, ptr::NonNull};

use ffi::{
    QueryAST, QueryError, QueryEvalCtx, QueryIterator, RSSearchOptions, RedisSearchCtx,
    ResultProcessor,
};
use query_eval::{
    Config, QueryEvalContext, QueryIteratorTree, QueryNodeMut, scorers::BuiltInScorer,
};
use rqe_iterators::{IteratorsConfig, c2rust::CRQEIterator};
use slots_tracker::SlotRangeArray;

/// Snapshot the evaluator's configuration.
///
/// Fields that have no per-query snapshot are read from the process-wide config, each
/// through its [`global_config`] accessor. Anything covered by [`IteratorsConfig`] is
/// instead taken from `iterators` rather than the live global.
///
/// The resulting [`Config`] is threaded through evaluation as a parameter, so evaluation
/// itself never re-reads the global.
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

/// Build the executable [`QueryIteratorTree`] for a parsed query AST.
///
/// Assembles the [`QueryEvalCtx`] from the request pieces, then evaluates
/// `qast`'s root node with [`QueryIteratorTree::new`]. The caller owns the
/// returned tree and releases it with [`QueryIteratorTree_Free`] or
/// [`QueryIteratorTree_IntoResultProcessor`].
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
///    addresses for the lifetime of the returned tree. Timeout source
///    changes and deadline writes may occur only between iterator probes;
///    only the blocked-client flag may change concurrently.
/// 4. `status` must be a non-null pointer to a valid [`QueryError`].
///
/// Together these are exactly the invariants documented on
/// [`QueryEvalContext::new`] for the assembled context, which remains valid for
/// the lifetime of the returned tree.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QAST_Iterate(
    qast: *mut QueryAST,
    opts: *const RSSearchOptions,
    sctx: *mut RedisSearchCtx,
    reqflags: u32,
    status: *mut QueryError,
) -> NonNull<QueryIteratorTree<'static>> {
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
    let tree = QueryIteratorTree::new(&mut ctx, node, config);
    // SAFETY: the index data and request timeout outlive the tree
    // (precondition 3), and no other tree can be built from `ctx`, which is
    // dropped on return.
    let tree = unsafe { tree.erase_lifetime() };
    NonNull::from(Box::leak(Box::new(tree)))
}

/// Free a [`QueryIteratorTree`] and every iterator it owns. NULL is a no-op.
///
/// # Safety
///
/// 1. `tree` must be NULL, or a tree returned by [`QAST_Iterate`] that has not
///    been released yet.
/// 2. The tree's root must not have been freed or handed to another owner.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryIteratorTree_Free(tree: Option<NonNull<QueryIteratorTree<'static>>>) {
    if let Some(tree) = tree {
        // SAFETY: `tree` came from `Box::leak` and has not been released yet
        // (precondition 1). Dropping it frees its root, which the tree still
        // exclusively owns (precondition 2).
        drop(unsafe { Box::from_raw(tree.as_ptr()) });
    }
}

/// Return the root iterator of `tree`, which the tree keeps owning.
///
/// It stops being the root once [`QueryIteratorTree_SetRoot`] or
/// [`QueryIteratorTree_Profile`] replaces it.
///
/// # Safety
///
/// 1. `tree` must be a tree returned by [`QAST_Iterate`] that has not been
///    released yet.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn QueryIteratorTree_Root(
    tree: NonNull<QueryIteratorTree<'static>>,
) -> NonNull<QueryIterator> {
    // SAFETY: `tree` points to a live tree (precondition 1).
    unsafe { tree.as_ref() }.root_ptr()
}

/// Replace the root iterator of `tree` with `root`, as
/// [`QueryIteratorTree::replace_root`] does.
///
/// The previous root is not freed: `root` must take ownership of it, typically
/// as a child, or it leaks.
///
/// # Safety
///
/// 1. `tree` must be a tree returned by [`QAST_Iterate`] that has not been
///    released yet.
/// 2. `tree` must not be accessed through any other pointer, from this or
///    another thread, for the duration of the call.
/// 3. `root` must satisfy the safety invariants of
///    [`CRQEIterator::new`](rqe_iterators::c2rust::CRQEIterator::new).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryIteratorTree_SetRoot(
    mut tree: NonNull<QueryIteratorTree<'static>>,
    root: NonNull<QueryIterator>,
) {
    // SAFETY: `tree` points to a live tree (precondition 1), not aliased for
    // the call (precondition 2).
    let tree = unsafe { tree.as_mut() };
    // SAFETY: `root` satisfies the `CRQEIterator::new` invariants (precondition 3).
    let root = unsafe { CRQEIterator::new(root) };
    let _ = tree.replace_root(root);
}

/// Profile `tree` in place, via [`QueryIteratorTree::into_profiled`].
///
/// # Safety
///
/// 1. `tree` must be a tree returned by [`QAST_Iterate`] that has not been
///    released yet.
/// 2. `tree` must not be accessed through any other pointer, from this or
///    another thread, for the duration of the call.
///
/// # Panics
///
/// Aborts the process if `tree` has already been profiled.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryIteratorTree_Profile(tree: NonNull<QueryIteratorTree<'static>>) {
    // SAFETY: `tree` came from `Box::leak` and has not been released yet
    // (precondition 1), and is not aliased for the call (precondition 2).
    let mut tree = unsafe { Box::from_raw(tree.as_ptr()) };
    *tree = (*tree).into_profiled();
    // Reuse the allocation so the caller's handle stays valid.
    let _ = Box::into_raw(tree);
}

/// Consume `tree` into the result processor that reads its documents.
///
/// The caller owns the returned processor, whose `Free` callback also frees the
/// tree's root and `query_slots`.
///
/// # Safety
///
/// 1. `tree` must be a tree returned by [`QAST_Iterate`] that has not been
///    released yet.
/// 2. `query_slots` must be NULL, or a [`SlotRangeArray`] allocated with the
///    Redis allocator and not owned by anything else.
/// 3. `sctx` must be a valid [`RedisSearchCtx`] with a non-NULL `timeout` and
///    whose `spec` is a valid, non-null [`IndexSpec`](ffi::IndexSpec), and
///    must outlive the returned processor.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn QueryIteratorTree_IntoResultProcessor(
    tree: NonNull<QueryIteratorTree<'static>>,
    query_slots: *const SlotRangeArray,
    key_space_version: u32,
    sctx: NonNull<RedisSearchCtx>,
) -> NonNull<ResultProcessor> {
    // SAFETY: `tree` came from `Box::leak` and has not been released yet
    // (precondition 1).
    let tree = unsafe { Box::from_raw(tree.as_ptr()) };
    let root = tree.into_raw_root();
    // SAFETY: `root` is a valid, owning iterator handed over with the tree
    // (precondition 1); `query_slots` and `sctx` satisfy preconditions 2 and 3.
    let rp = unsafe {
        ffi::RPQueryIterator_New(
            root.as_ptr(),
            query_slots.cast(),
            key_space_version,
            sctx.as_ptr(),
        )
    };
    NonNull::new(rp).expect("the query iterator result processor must not be NULL")
}
