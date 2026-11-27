/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_SPARSE_VECTOR → the metric request its score is yielded under.
//!
//! The request is reserved before the field's index is opened, so a field no
//! document has written to — which has no index and matches nothing — is
//! enough to observe it.
//!
//! Disabled under Miri: [`TestContext`] calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use std::ffi::{c_char, c_void};

use query::mock::MockQueryNode;
use query_eval::{Config, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::{QueryNodeFlags, QueryNodeType};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};
use sparse_vector_index::SparseMetric;

/// The name the node yields its score under.
const SCORE_FIELD: &[u8] = b"__sp_score";

/// The query vector, one `(dimension, weight)` pair in the HASH value encoding.
fn query_blob() -> Vec<u8> {
    [1u32.to_le_bytes(), 1.0f32.to_le_bytes()].concat()
}

/// Copy `s` into a NUL-terminated buffer owned by the module allocator, which
/// is what frees the node's distance field.
fn module_string(s: &[u8]) -> *mut c_char {
    let ptr = redis_mock::allocator::alloc_shim(s.len() + 1).cast::<c_char>();
    assert!(!ptr.is_null(), "allocation failed");
    // SAFETY: `ptr` owns `s.len() + 1` writable bytes, which cannot overlap `s`.
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr().cast::<c_char>(), ptr, s.len());
        *ptr.add(s.len()) = 0;
    }
    ptr
}

/// Whether the node is flagged with [`QueryNodeFlags::HideVectorDistanceField`].
#[derive(Clone, Copy)]
enum Score {
    Shown,
    Hidden,
}

/// Evaluate a sparse-vector node over a field with no index, and report
/// whether the metric request it reserved is internal.
fn reserved_request_is_internal(score: Score) -> bool {
    let _guard = GlobalGuard::default();
    let mut context = TestContext::numeric(std::iter::empty(), false);
    // SAFETY: `context.qctx()` returns a valid, exclusively-owned `QueryEvalCtx`.
    let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

    let field_spec = context.add_field(b"sp");
    // SAFETY: `field_spec` was just added and nothing else observes it yet.
    let field = unsafe { field_spec.as_ptr().as_mut() }.expect("non-null");
    field.set_types(ffi::FieldType_INDEXFLD_T_SPARSE_VECTOR);
    // Writing a union member needs no `unsafe`. `sparseVecOpts` is the one a sparse vector
    // field reads; its index stays null, as no document wrote to it.
    field.__bindgen_anon_1.sparseVecOpts.dim = 10;
    field.__bindgen_anon_1.sparseVecOpts.metric = SparseMetric::InnerProduct as u32;

    let blob = query_blob();
    let mut node = MockQueryNode::new(QueryNodeType::SparseVector);
    node.set_sparse_vector(field.index, &blob);
    node.opts_mut().dist_field = module_string(SCORE_FIELD);
    if let Score::Hidden = score {
        node.opts_mut().flags |= QueryNodeFlags::HideVectorDistanceField;
    }

    // SAFETY: `node` is a valid, live `RSQueryNode` for the call.
    let node_ref = unsafe { QueryNodeMut::new(node.as_non_null()) };
    assert!(
        eval_node(&mut ctx, node_ref, Config::default()).is_none(),
        "a field with no index matches nothing"
    );

    let requests = ctx.metric_requests();
    assert_eq!(requests.len(), 1);
    let is_internal = requests[0].is_internal;
    drop(ctx);
    // The evaluation borrowed the distance field without taking it, so it is
    // still this test's allocation from the module allocator.
    redis_mock::allocator::free_shim(node.opts_mut().dist_field.cast::<c_void>());
    is_internal
}

#[test]
fn eval_sparse_vector_yields_its_score_in_the_response() {
    // A named score is part of the response, as in FT.SEARCH.
    assert!(!reserved_request_is_internal(Score::Shown));
}

#[test]
fn eval_sparse_vector_hidden_score_reserves_an_internal_metric_request() {
    // An FT.HYBRID VSIM arm without YIELD_SCORE_AS flags its default score name
    // hidden: computed, but kept out of the response, as a dense arm's is.
    assert!(reserved_request_is_internal(Score::Hidden));
}
