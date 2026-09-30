/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_IDS → IdList

use query_eval::{Config, QueryEvalContext, QueryNodeMut, eval_node};
use query_types::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::ContractChecker;

use query::mock::{MockQueryEvalCtx, MockQueryNode};

use crate::util::MockKeys;

#[test]
fn eval_ids_with_pre_resolved_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(3);
    let mut doc_ids: Vec<u64> = vec![10, 5, 20];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it = ContractChecker::new(
        eval_node(&mut ctx, node, Config::default())
            .expect("should not be None")
            .into_boxed(),
    );

    assert_eq!(it.type_(), IteratorType::IdListSorted);
    assert!(!it.at_eof());

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 10);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 20);
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn eval_ids_deduplicates() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(4);
    let mut doc_ids: Vec<u64> = vec![3, 3, 7, 7];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it = ContractChecker::new(
        eval_node(&mut ctx, node, Config::default())
            .expect("should not be None")
            .into_boxed(),
    );

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 3);
    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 7);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_filters_zero_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(3);
    let mut doc_ids: Vec<u64> = vec![0, 5, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it = ContractChecker::new(
        eval_node(&mut ctx, node, Config::default())
            .expect("should not be None")
            .into_boxed(),
    );

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_all_zero_produces_empty_list() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(2);
    let mut doc_ids: Vec<u64> = vec![0, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it = ContractChecker::new(
        eval_node(&mut ctx, node, Config::default())
            .expect("should not be None")
            .into_boxed(),
    );

    // Nothing to yield, but nothing read yet either: `at_eof()` is the negation
    // of `current()`, so it flips once a read has run past the end.
    assert!(!it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn eval_ids_empty_keys() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(0);
    let mut doc_ids: Vec<u64> = Vec::new();

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), 0);
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let mut it = ContractChecker::new(
        eval_node(&mut ctx, node, Config::default())
            .expect("should not be None")
            .into_boxed(),
    );

    // Nothing to yield, but nothing read yet either: `at_eof()` is the negation
    // of `current()`, so it flips once a read has run past the end.
    assert!(!it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
#[should_panic(expected = "QN_IDS nodes must carry pre-resolved doc_ids")]
fn eval_ids_requires_pre_resolved_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys = MockKeys::nulls(1);

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), std::ptr::null_mut(), keys.len());
    let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

    let _ = eval_node(&mut ctx, node, Config::default());
}

// ---------------------------------------------------------------------------
// QN_IDS → IdList, consuming pre-resolved doc ids
//
// Exercised against the full-FFI `TestContext` so the ids come from real
// registered documents rather than literals.
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` calls into the C library, which Miri
// cannot execute.
#[cfg(not(miri))]
mod ids_doctable {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;
    use crate::util::MockKeys;

    #[test]
    fn eval_ids_consumes_pre_resolved_doc_ids() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Register two documents; ids are assigned incrementally from 1.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // Both known keys plus an unknown one, whose id 0 must be filtered out.
        let keys = MockKeys::new(&["doc_b", "ghost", "doc_a"]);
        let mut doc_ids: Vec<ffi::t_docId> = vec![id_b, 0, id_a];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
        let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

        let mut it = ContractChecker::new(
            eval_node(&mut ctx, node, Config::default())
                .expect("should not be None")
                .into_boxed(),
        );

        assert_eq!(it.type_(), IteratorType::IdListSorted);
        // Results are sorted; the unknown id (0) is dropped.
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_a);
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_b);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn eval_ids_all_unresolved_produce_empty_list() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        let keys = MockKeys::new(&["nope", "missing"]);
        let mut doc_ids: Vec<ffi::t_docId> = vec![0, 0];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
        let node = unsafe { QueryNodeMut::new(mock_node.as_non_null()) };

        let mut it = ContractChecker::new(
            eval_node(&mut ctx, node, Config::default())
                .expect("should not be None")
                .into_boxed(),
        );

        // Nothing to yield, but nothing read yet either.
        assert!(!it.at_eof());
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }
}
