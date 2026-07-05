/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use query::{QueryEvalContext, mock::MockQueryEvalCtx};
use query_flags::QEFlag;
use rqe_iterators::utils::AnyTimeoutContext;

#[test]
fn sctx_returns_inner_ref() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let sctx = ctx.sctx();
    assert!(std::ptr::eq(sctx, mock.sctx_ptr()));
}

#[test]
fn opts_returns_search_options() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let opts = ctx.opts();
    assert_eq!(opts.slop, 42);
}

#[test]
fn status_returns_default_query_error() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let status = ctx.status();
    assert!(status.is_ok());
}

#[test]
fn status_mutations_are_visible() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    ctx.status().set_code(query_error::QueryErrorCode::Syntax);
    assert!(!ctx.status().is_ok());
    assert_eq!(ctx.status().code(), query_error::QueryErrorCode::Syntax);
}

#[test]
fn metric_requests_ptr_returns_inner_ref() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let ptr = ctx.metric_requests_ptr();
    assert!(std::ptr::eq(ptr, mock.metric_requests_p().cast()));
}

#[test]
fn doc_table_returns_inner_ref() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let dt = ctx.doc_table();
    assert!(std::ptr::eq(dt, mock.doc_table_ptr()));
}

#[test]
fn req_flags_empty_by_default() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert!(ctx.req_flags().is_empty());
}

#[test]
fn req_flags_round_trips() {
    let flags = QEFlag::IsSearch | QEFlag::IsHybridSearchSubquery;
    let mut mock = MockQueryEvalCtx::with_req_flags(flags);
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert_eq!(ctx.req_flags(), flags);
    assert!(ctx.req_flags().contains(QEFlag::IsSearch));
    assert!(ctx.req_flags().contains(QEFlag::IsHybridSearchSubquery));
    assert!(!ctx.req_flags().contains(QEFlag::IsAggregate));
}

#[test]
fn config_returns_iterators_config() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    let config = ctx.config();
    assert_eq!(config.max_prefix_expansions, 200);
    assert_eq!(config.min_term_prefix, 2);
    assert_eq!(config.min_stem_length, 4);
    assert_eq!(config.min_union_iter_heap, 20);
}

#[test]
fn in_not_sub_tree_default_false() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert!(!ctx.in_not_sub_tree());
}

#[test]
fn set_in_not_sub_tree_returns_previous_and_updates() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    let prev = ctx.set_in_not_sub_tree(true);
    assert!(!prev);
    assert!(ctx.in_not_sub_tree());

    let prev = ctx.set_in_not_sub_tree(false);
    assert!(prev);
    assert!(!ctx.in_not_sub_tree());
}

#[test]
fn next_token_id_post_increments() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    assert_eq!(ctx.next_token_id(), 0);
    assert_eq!(ctx.next_token_id(), 1);
    assert_eq!(ctx.next_token_id(), 2);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "clock-based path calls libc::clock_gettime(CLOCK_MONOTONIC_RAW), unsupported by Miri"
)]
fn build_timeout_context_without_blocked_client_uses_sctx() {
    // No `bcTimeoutAreq` wired in → the source is derived from `sctx.time`, never
    // the Blocked Client path. The mock's zeroed `sctx.time` is a past deadline,
    // so the clock-based variant is selected.
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    assert!(matches!(
        ctx.build_timeout_context(),
        AnyTimeoutContext::Clock(_)
    ));
}

#[test]
fn build_timeout_context_prefers_blocked_client_when_wired() {
    // A non-null `bcTimeoutAreq` selects the Blocked Client Timeout source,
    // overriding `sctx.time` — mirroring the C evaluator's NOT-node behavior.
    let mut mock = MockQueryEvalCtx::new();
    mock.enable_blocked_client_timeout();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    assert!(matches!(
        ctx.build_timeout_context(),
        AnyTimeoutContext::BlockedClient(_)
    ));
}
