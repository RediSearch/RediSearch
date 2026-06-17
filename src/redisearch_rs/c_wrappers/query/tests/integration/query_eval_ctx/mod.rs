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
fn not_subtree_default_false() {
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert!(!ctx.not_subtree());
}

#[test]
fn set_not_subtree_returns_previous_and_updates() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    let prev = ctx.set_not_subtree(true);
    assert!(!prev);
    assert!(ctx.not_subtree());

    let prev = ctx.set_not_subtree(false);
    assert!(prev);
    assert!(!ctx.not_subtree());
}

#[test]
fn next_token_id_post_increments() {
    let mut mock = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    assert_eq!(ctx.next_token_id(), 0);
    assert_eq!(ctx.next_token_id(), 1);
    assert_eq!(ctx.next_token_id(), 2);
}
