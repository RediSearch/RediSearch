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

/// Run `f` against a context, releasing the metric-request array the appends
/// grow.
///
/// The mock owns the head *slot* but not the array behind it — it hands out an
/// empty (null) head and never grows one — so whatever the appends allocate is
/// this function's to free rather than its teardown's.
fn with_metric_requests(f: impl FnOnce(&mut QueryEvalContext)) {
    /// Releases the array on the way out, whether `f` returns or panics: an
    /// assertion failure that skipped the release would leave a non-empty head
    /// for the mock's teardown to assert on mid-unwind, aborting the process
    /// instead of reporting which assertion failed.
    struct FreeOnDrop(*mut *mut rlookup::MetricRequest<'static>);

    impl Drop for FreeOnDrop {
        fn drop(&mut self) {
            // SAFETY: the head slot outlives this guard (the mock owning it is
            // dropped later), and whatever the appends left on it is either
            // null or a tracked array owned by nothing else.
            unsafe {
                let head = *self.0;
                if head.is_null() {
                    return;
                }
                for request in
                    std::slice::from_raw_parts(head, ffi::array_len_func(head.cast()) as _)
                {
                    if !request.key_handle.is_null() {
                        // Through the same allocator `bind_metric_request_key`
                        // took the handle from, rather than the shim that
                        // happens to back it in this build.
                        let free = redis_module::RedisModule_Free.expect("RedisModule_Free unset");
                        free(request.key_handle.cast());
                    }
                }
                ffi::array_free(head.cast());
                // Back to the empty state the mock handed out, so its teardown
                // does not outlive the array through a dangling head.
                *self.0 = std::ptr::null_mut();
            }
        }
    }

    let mut mock = MockQueryEvalCtx::new();
    // Declared after `mock` so it runs first, while the head slot is still live.
    let _free = FreeOnDrop(mock.metric_requests_p());

    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };
    f(&mut ctx);
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_ensure_append_n_func)")]
fn add_metric_request_appends_and_returns_its_index() {
    let first = c"__v_score";
    let second = c"__w_score";

    with_metric_requests(|ctx| {
        // SAFETY: both are static C string literals — non-null,
        // NUL-terminated, and outliving the array.
        let (a, b) = unsafe {
            (
                ctx.add_metric_request(first.as_ptr(), false),
                ctx.add_metric_request(second.as_ptr(), true),
            )
        };
        // Indices are handed out in append order and stay valid as the array
        // grows — a later request never displaces an earlier one.
        assert_eq!((a, b), (0, 1));

        let requests = ctx.metric_requests();
        assert_eq!(requests.len(), 2);
        assert!(std::ptr::eq(requests[0].metric_name, first.as_ptr()));
        assert!(!requests[0].is_internal);
        assert!(std::ptr::eq(requests[1].metric_name, second.as_ptr()));
        assert!(requests[1].is_internal);
        assert!(
            requests.iter().all(|r| r.key_handle.is_null()),
            "a freshly reserved request carries no key handle"
        );
    });
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_ensure_append_n_func)")]
fn bind_metric_request_key_fills_in_a_valid_handle() {
    with_metric_requests(|ctx| {
        // SAFETY: a static C string literal — non-null, NUL-terminated,
        // and outliving the array.
        let idx = unsafe { ctx.add_metric_request(c"__v_score".as_ptr(), false) };

        // Stands in for the key slot inside an iterator; it outlives the
        // handle, which this test frees before it goes out of scope.
        let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
        // SAFETY: `idx` was just reserved on this context and `key` outlives
        // the handle.
        let handle = unsafe { ctx.bind_metric_request_key(idx, &mut key) };

        // SAFETY: `handle` is the freshly allocated, fully initialised handle.
        let handle_ref = unsafe { &*handle };
        assert!(std::ptr::eq(handle_ref.key_ptr, &mut key));
        assert!(
            handle_ref.is_valid,
            "the pipeline gates the key write on this flag, so it must be set \
             explicitly — the module allocator does not zero"
        );
        // The two pointers carry unrelated key lifetimes, so compare them by
        // address rather than making the types line up.
        assert_eq!(ctx.metric_requests()[idx].key_handle.addr(), handle.addr());
    });
}

/// The order a vector node actually produces: reserve, evaluate the child
/// subtree — which reserves and binds requests of its own, reallocating the
/// array — and only then bind the outer request. So binds arrive out of order,
/// at a non-zero index, and across a move of the whole array.
///
/// The third reservation is the one that matters: it lands with no spare
/// capacity, so the array is reallocated between the two binds and the head may
/// move. That makes this the test that would catch a wrong element stride, an
/// off-by-one in the index, or a head cached across the append — none of which
/// a bind at index 0 on a one-element array can distinguish.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (array_ensure_append_n_func)")]
fn binds_survive_the_array_moving_under_them() {
    with_metric_requests(|ctx| {
        let mut outer_key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
        let mut inner_key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();

        // SAFETY: static C string literals — non-null, NUL-terminated, and
        // outliving the array.
        let (outer, inner) = unsafe {
            (
                ctx.add_metric_request(c"__v_score".as_ptr(), false),
                ctx.add_metric_request(c"__w_score".as_ptr(), false),
            )
        };

        // The inner request binds first, as the child subtree finishes first.
        // SAFETY: `inner` was just reserved on this context, and `inner_key`
        // outlives the handle.
        let inner_handle = unsafe { ctx.bind_metric_request_key(inner, &mut inner_key) };

        // A third reservation moves the array out from under the handle just
        // stored, which is what the bind below has to tolerate.
        // SAFETY: as for the reservations above.
        let third = unsafe { ctx.add_metric_request(c"__x_score".as_ptr(), false) };
        assert_eq!((outer, inner, third), (0, 1, 2));

        // SAFETY: `outer` was reserved on this context and not yet bound, and
        // `outer_key` outlives the handle.
        let outer_handle = unsafe { ctx.bind_metric_request_key(outer, &mut outer_key) };

        let requests = ctx.metric_requests();
        assert_eq!(requests.len(), 3);
        // The move did not disturb the earlier binding, and the later one
        // landed on its own entry rather than overwriting a neighbour.
        assert_eq!(requests[inner].key_handle.addr(), inner_handle.addr());
        assert_eq!(requests[outer].key_handle.addr(), outer_handle.addr());
        assert_ne!(inner_handle.addr(), outer_handle.addr());
        assert!(
            requests[third].key_handle.is_null(),
            "a request reserved but never bound keeps a null handle"
        );

        // Each handle still points at its own key slot: a wrong stride would
        // have crossed them over while leaving the assertions above intact.
        // SAFETY: both handles are freshly allocated and fully initialised.
        unsafe {
            assert!(std::ptr::eq((*inner_handle).key_ptr, &mut inner_key));
            assert!(std::ptr::eq((*outer_handle).key_ptr, &mut outer_key));
        }
    });
}

/// Binding twice orphans the first handle — nothing can reach it through the
/// array afterwards, so nothing frees it. The debug-only assertion is what
/// turns that silent leak into a located failure, so it is worth a test of its
/// own.
#[test]
#[cfg(debug_assertions)] // the assertion only exists in debug builds
#[cfg_attr(miri, ignore = "requires C FFI (array_ensure_append_n_func)")]
#[should_panic(expected = "was already bound")]
fn binding_a_metric_request_twice_panics() {
    with_metric_requests(|ctx| {
        // SAFETY: a static C string literal — non-null, NUL-terminated,
        // and outliving the array.
        let idx = unsafe { ctx.add_metric_request(c"__v_score".as_ptr(), false) };

        let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
        // SAFETY: `idx` was just reserved on this context and `key` outlives
        // the handle.
        unsafe { ctx.bind_metric_request_key(idx, &mut key) };
        // SAFETY: as above, except for the very precondition under test — the
        // assertion fires before the second bind reaches the array.
        unsafe { ctx.bind_metric_request_key(idx, &mut key) };
    });
}

/// An index that was never reserved has no entry to bind, and on an empty list
/// there is no length header to bounds-check it against either — so the
/// assertion has to come before the length read.
#[test]
#[cfg(debug_assertions)] // the assertion only exists in debug builds
#[should_panic(expected = "must come from `add_metric_request`")]
fn binding_without_reserving_panics() {
    let mut mock = MockQueryEvalCtx::new();
    // SAFETY: the mock is a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    let mut key: *mut rlookup::RLookupKey<'_> = std::ptr::null_mut();
    // SAFETY: none — this violates precondition (1) on purpose, and the
    // assertion fires before anything reads the null head.
    unsafe { ctx.bind_metric_request_key(0, &mut key) };
}

#[test]
fn metric_requests_is_empty_before_any_are_reserved() {
    // A tracked array reads as a null head until its first append, which is not
    // a length of zero to read but the absence of a length header altogether.
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
fn build_timeout_context_without_blocked_client_uses_sctx() {
    // No `bcTimeoutAreq` wired in → the source is derived from `sctx.time`, never
    // the Blocked Client path. The mock's zeroed `sctx.time` is a past deadline,
    // so the clock-based variant is selected.
    let mut mock = MockQueryEvalCtx::new();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    // SAFETY: `mock` (and its AREQ, if any) outlives the returned context, which
    // is dropped at the end of the assertion — never used past the AREQ.
    let timeout = unsafe { ctx.build_timeout_context() };
    assert!(matches!(timeout, AnyTimeoutContext::Clock(_)));
}

#[test]
fn build_timeout_context_prefers_blocked_client_when_wired() {
    // A non-null `bcTimeoutAreq` selects the Blocked Client Timeout source,
    // overriding `sctx.time` — mirroring the C evaluator's NOT-node behavior.
    let mut mock = MockQueryEvalCtx::new();
    mock.enable_blocked_client_timeout();
    let ctx = unsafe { QueryEvalContext::new(mock.as_non_null()) };

    // SAFETY: `mock`'s AREQ outlives the returned context, which is dropped at
    // the end of the assertion — never used past the AREQ.
    let timeout = unsafe { ctx.build_timeout_context() };
    assert!(matches!(timeout, AnyTimeoutContext::BlockedClient(_)));
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
