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
use query_types::scorers::{BuiltInScorer, RequestedScorer};
use rqe_iterators::utils::TimeoutContext;

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
fn add_metric_request_appends_and_returns_its_index() {
    let first = c"__v_score";
    let second = c"__w_score";

    let mut mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    // SAFETY: both are static C string literals — non-null, NUL-terminated, and
    // outliving the list.
    let (a, b) = unsafe {
        (
            ctx.add_metric_request(first.as_ptr(), false),
            ctx.add_metric_request(second.as_ptr(), true),
        )
    };
    // Indices are handed out in append order and stay valid as the list grows
    // — a later request never displaces an earlier one.
    assert_eq!((a.index(), b.index()), (0, 1));

    let requests = ctx.metric_requests();
    assert_eq!(requests.len(), 2);
    // The names are borrowed, not copied.
    assert!(std::ptr::eq(
        requests[0].metric_name().as_ptr(),
        first.as_ptr()
    ));
    assert!(!requests[0].is_internal());
    assert!(std::ptr::eq(
        requests[1].metric_name().as_ptr(),
        second.as_ptr()
    ));
    assert!(requests[1].is_internal());
    assert!(
        requests.iter().all(|r| r.key_handle().is_none()),
        "a freshly reserved request carries no key handle"
    );
}

#[test]
fn bind_metric_request_key_fills_in_a_valid_handle() {
    // Stands in for the key slot inside an iterator. Declared before the mock
    // so that it outlives the handle, which the mock frees with the list.
    let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();

    let mut mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    // SAFETY: a static C string literal — non-null, NUL-terminated, and
    // outliving the list.
    let id = unsafe { ctx.add_metric_request(c"__v_score".as_ptr(), false) };
    // Kept because binding spends the id, and the entry is read back below.
    let idx = id.index();

    // SAFETY: `key` outlives the handle, and nothing writes through the handle.
    let handle = unsafe { ctx.bind_metric_request_key(id, &mut key) };

    // SAFETY: `handle` is the freshly allocated, fully initialised handle.
    let handle_ref = unsafe { &*handle };
    assert!(std::ptr::eq(handle_ref.key_ptr, &raw mut key));
    assert!(
        handle_ref.is_valid,
        "the pipeline gates the key write on this flag, so a fresh handle must \
         have it set"
    );
    // The two pointers carry unrelated key lifetimes, so compare them by
    // address rather than making the types line up.
    assert_eq!(
        ctx.metric_requests()[idx]
            .key_handle()
            .map(|h| h.as_ptr().addr()),
        Some(handle.addr())
    );
}

/// The order a vector node actually produces: reserve, evaluate the child
/// subtree — which reserves and binds requests of its own — and only then bind
/// the outer request. So binds arrive out of order, at a non-zero index, and
/// across growth of the whole list.
///
/// The reservations between the two binds are the ones that matter: they grow
/// the list past any spare capacity, so its storage is reallocated between the
/// binds and may move. That makes this the test that would catch a handle
/// stored inline in an element, an off-by-one in the index, or storage cached
/// across the append — none of which a bind at index 0 on a one-element list
/// can distinguish. The write through the inner handle afterwards is what the
/// inner iterator does when freed, and under Miri it checks that the handle's
/// provenance survived the reallocation.
#[test]
fn binds_survive_the_list_growing_under_them() {
    // Stand in for the key slots inside two iterators, which outlive the
    // handles as the iterators do in production.
    let mut outer_key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
    let mut inner_key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();

    let mut mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    // SAFETY: static C string literals — non-null, NUL-terminated, and
    // outliving the list.
    let (outer_id, inner_id) = unsafe {
        (
            ctx.add_metric_request(c"__v_score".as_ptr(), false),
            ctx.add_metric_request(c"__w_score".as_ptr(), false),
        )
    };
    // Kept because binding spends the ids, and both entries are read back
    // below.
    let (outer, inner) = (outer_id.index(), inner_id.index());

    // The inner request binds first, as the child subtree finishes first.
    // SAFETY: `inner_key` outlives the handle.
    let inner_handle = unsafe { ctx.bind_metric_request_key(inner_id, &mut inner_key) };

    // More reservations reallocate the list's storage, possibly moving it out
    // from under the handle just stored, which is what the bind below has to
    // tolerate.
    const MORE: usize = 16;
    for _ in 0..MORE {
        // SAFETY: as for the reservations above.
        let _ = unsafe { ctx.add_metric_request(c"__x_score".as_ptr(), false) };
    }
    assert_eq!((outer, inner), (0, 1));

    // SAFETY: `outer_key` outlives the handle.
    let outer_handle = unsafe { ctx.bind_metric_request_key(outer_id, &mut outer_key) };

    // The inner iterator is freed, clearing its handle through its own copy of
    // the pointer.
    // SAFETY: the handle is live — the list owning it is.
    unsafe { (*inner_handle).is_valid = false };

    let requests = ctx.metric_requests();
    assert_eq!(requests.len(), 2 + MORE);
    // The growth did not disturb the earlier binding, and the later one landed
    // on its own entry rather than overwriting a neighbour.
    let addr = |i: usize| requests[i].key_handle().map(|h| h.as_ptr().addr());
    assert_eq!(addr(inner), Some(inner_handle.addr()));
    assert_eq!(addr(outer), Some(outer_handle.addr()));
    assert_ne!(inner_handle.addr(), outer_handle.addr());
    assert!(
        requests[2..].iter().all(|r| r.key_handle().is_none()),
        "a request reserved but never bound keeps no handle"
    );

    // Each handle still points at its own key slot, and only the freed
    // iterator's handle reads as invalid.
    // SAFETY: both handles are live and fully initialised.
    let inner_handle = unsafe { &*inner_handle };
    // SAFETY: as above.
    let outer_handle = unsafe { &*outer_handle };
    assert!(std::ptr::eq(inner_handle.key_ptr, &raw mut inner_key));
    assert!(std::ptr::eq(outer_handle.key_ptr, &raw mut outer_key));
    assert!(!inner_handle.is_valid);
    assert!(outer_handle.is_valid);
}

/// A [`MetricRequestId`](query::MetricRequestId) records no context, so one can
/// be bound against a context that never reserved it — the misuse the type
/// cannot rule out. The bind cannot detect it in general, but rejects an id
/// that names no request of this context.
///
/// The empty case: nothing was reserved, so there is no list to bind into.
#[test]
#[should_panic(expected = "must come from `add_metric_request`")]
fn binding_an_id_against_a_context_that_reserved_nothing_panics() {
    let mut reserved_mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut reserved = unsafe { QueryEvalContext::new(reserved_mock.as_non_null()) };
    // SAFETY: a static C string literal — non-null, NUL-terminated, and
    // outliving the list.
    let id = unsafe { reserved.add_metric_request(c"__v_score".as_ptr(), false) };

    let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
    let mut empty_mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut empty = unsafe { QueryEvalContext::new(empty_mock.as_non_null()) };
    // SAFETY: none for precondition (1), which the foreign `id` breaks on
    // purpose: the bind rejects it before using it. `key` outlives the handle,
    // were one created.
    unsafe { empty.bind_metric_request_key(id, &mut key) };
}

/// The non-empty case: a context that *has* reserved, but fewer times than the
/// id's index, so the id names no request of this list — which is what makes
/// this the pair's other half rather than a restatement of it.
#[test]
#[should_panic(expected = "must come from `add_metric_request`")]
fn binding_an_id_past_the_end_of_another_context_panics() {
    let mut longer_mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut longer = unsafe { QueryEvalContext::new(longer_mock.as_non_null()) };
    // SAFETY: static C string literals — non-null, NUL-terminated, and
    // outliving the list.
    let id = unsafe {
        let _ = longer.add_metric_request(c"__v_score".as_ptr(), false);
        longer.add_metric_request(c"__w_score".as_ptr(), false)
    };

    let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
    let mut shorter_mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut shorter = unsafe { QueryEvalContext::new(shorter_mock.as_non_null()) };
    // One reservation against the other context's two, so the id's index is one
    // past this list's only element.
    // SAFETY: as above.
    let _ = unsafe { shorter.add_metric_request(c"__x_score".as_ptr(), false) };
    assert_eq!(id.index(), shorter.metric_requests().len());

    // SAFETY: as in the empty case above.
    unsafe { shorter.bind_metric_request_key(id, &mut key) };
}

#[test]
fn metric_requests_is_empty_before_any_are_reserved() {
    // The list is created by the first reservation, so a context that made none
    // has no list to read at all.
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert!(ctx.metric_requests().is_empty());
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
fn build_timeout_context_uses_clock_source_from_sctx() {
    // The mock's request timeout is a past clock deadline.
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    // SAFETY: `mock` outlives the returned context.
    let mut timeout = unsafe { ctx.build_timeout_context() };
    for _ in 0..rqe_iterators::not_reducer::TIMEOUT_CHECK_GRANULARITY - 1 {
        assert!(timeout.check_timeout().is_ok());
    }
    assert!(timeout.check_timeout().is_err());
}

#[cfg_attr(miri, ignore = "miri cannot call the C blocked-client timeout helper")]
#[test]
fn build_timeout_context_uses_blocked_client_source_from_sctx() {
    let mut mock = MockQueryEvalCtx::new();
    mock.enable_blocked_client_timeout();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    // SAFETY: `mock` outlives the returned context.
    let mut timeout = unsafe { ctx.build_timeout_context() };
    assert!(timeout.check_timeout().is_ok());
}

/// Build a context whose query scorer name is `name`, keeping the backing
/// [`CString`] alive for the returned context.
fn ctx_with_scorer_name(mock: &mut MockQueryEvalCtx, name: &std::ffi::CStr) -> QueryEvalContext {
    // SAFETY: `mock.opts_ptr()` is a valid, exclusively-owned `RSSearchOptions`;
    // `name` outlives the returned context.
    unsafe { (*mock.opts_ptr()).scorerName = name.as_ptr() };
    unsafe { QueryEvalContext::new(mock.as_non_null()) }
}

#[test]
fn scorer_unset_query_is_unset() {
    // The mock zero-inits `opts`, so `scorerName` is null: the query requested
    // no scorer, so `scorer()` reports `Unset` and leaves the fallback to the
    // caller.
    let mut mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    assert_eq!(ctx.scorer(), RequestedScorer::Unset);
}

#[test]
fn scorer_builtin_query_resolves_to_that_scorer() {
    let name = std::ffi::CString::new("BM25STD").unwrap();
    let mut mock = MockQueryEvalCtx::new();
    let ctx = ctx_with_scorer_name(&mut mock, &name);
    assert_eq!(
        ctx.scorer(),
        RequestedScorer::BuiltIn(BuiltInScorer::Bm25Std)
    );
}

#[test]
fn scorer_custom_query_is_custom() {
    // A set-but-custom query scorer is not one of the built-ins, so it resolves
    // to `Custom` (distinct from an unset scorer).
    let name = std::ffi::CString::new("MY_CUSTOM_SCORER").unwrap();
    let mut mock = MockQueryEvalCtx::new();
    let ctx = ctx_with_scorer_name(&mut mock, &name);
    assert_eq!(ctx.scorer(), RequestedScorer::Custom(name.as_c_str()));
}
