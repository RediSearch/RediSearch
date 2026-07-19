/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_MISSING → Missing inverted-index iterator
//!
//! These require a real `IndexSpec` with a populated `missingFieldDict`, which
//! the lightweight `MockQueryEvalCtx` cannot provide, so they rely on the
//! full-FFI `TestContext`.
//!
//! Disabled under Miri: `TestContext` calls into the C library (e.g. to build
//! the spec and its `missingFieldDict`), and Miri cannot execute foreign
//! (non-Rust) functions.
#![cfg(not(miri))]

use ffi::IndexFlags_Index_StoreFreqs;
use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_types::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

use query::mock::MockQueryNode;

#[test]
fn eval_missing_returns_iterator_over_missing_docs() {
    let _guard = GlobalGuard::default();
    // Real spec whose `missingFieldDict` records docs 1, 2 and 3 as
    // missing a value for the indexed field.
    let context = TestContext::missing([1u64, 2, 3].into_iter());

    // The `QueryEvalCtx` is backed by the real `sctx`, so `eval_missing`
    // sees the populated `missingFieldDict`.
    let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
    mock_node.set_missing_field(context.field_spec());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("field has missing values, so should not be None")
        .into_boxed();

    assert_eq!(it.type_(), IteratorType::InvIdxMissing);
    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn eval_missing_no_values_returns_none() {
    let _guard = GlobalGuard::default();
    // Term context: the field exists but has no entry in `missingFieldDict`
    // (no document is missing a value for it).
    let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

    let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
    mock_node.set_missing_field(context.field_spec());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    assert!(
        eval::eval_node(&mut ctx, &node, Config::default()).is_none(),
        "no missing values for the field, so eval should return None"
    );
}

#[test]
fn qast_iterate_substitutes_empty_for_none() {
    let _guard = GlobalGuard::default();
    // Term context: the field has no entry in `missingFieldDict`, so a
    // QN_MISSING node makes `eval_node` return `None`. `qast_iterate` must
    // substitute an `Empty` iterator instead of propagating that `None`.
    let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

    let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Missing);
    mock_node.set_missing_field(context.field_spec());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::qast_iterate(&mut ctx, &node, Config::default()).into_boxed();

    assert_eq!(it.type_(), IteratorType::Empty);
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}
