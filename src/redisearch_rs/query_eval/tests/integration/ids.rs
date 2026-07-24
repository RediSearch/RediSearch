/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_IDS → IdList

use query_eval::{QueryEvalContext, QueryNodeRef, eval, eval::Config};
use query_types::QueryNodeType;
use rqe_iterators::{IteratorType, RQEIterator};

use query::mock::{MockQueryEvalCtx, MockQueryNode};

#[test]
fn eval_ids_with_pre_resolved_doc_ids() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 3];
    let mut doc_ids: Vec<u64> = vec![10, 5, 20];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

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

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 4];
    let mut doc_ids: Vec<u64> = vec![3, 3, 7, 7];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

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

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 3];
    let mut doc_ids: Vec<u64> = vec![0, 5, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    let r = it.read().unwrap().unwrap();
    assert_eq!(r.doc_id, 5);
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_all_zero_produces_empty_list() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    mock_ctx.enable_disk_mode();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let keys: Vec<ffi::sds> = vec![std::ptr::null_mut(); 2];
    let mut doc_ids: Vec<u64> = vec![0, 0];

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_ids_empty_keys() {
    let mut mock_ctx = MockQueryEvalCtx::new();
    let mut ctx = unsafe { QueryEvalContext::new(mock_ctx.as_non_null()) };

    let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
    mock_node.set_ids(std::ptr::null(), std::ptr::null_mut(), 0);
    let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

    let mut it = eval::eval_node(&mut ctx, &node, Config::default())
        .expect("should not be None")
        .into_boxed();

    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

// ---------------------------------------------------------------------------
// QN_IDS → IdList, consuming pre-resolved doc ids
//
// key -> docId is resolved on the main thread during query construction (INKEYS
// handling in `aggregate_request.c`), so the query node carries pre-resolved
// `doc_ids`. These tests verify that `eval_ids` sorts, deduplicates, and drops
// the misses (encoded as id 0).
// ---------------------------------------------------------------------------

// Disabled under Miri: `TestContext` and SDS creation call into the C library,
// which Miri cannot execute.
#[cfg(not(miri))]
mod ids_doctable {
    use ffi::IndexFlags_Index_StoreFreqs;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use super::*;
    use crate::util::new_sds;

    #[test]
    fn eval_ids_consumes_pre_resolved_doc_ids() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        // Register two documents; ids are assigned incrementally from 1.
        let id_a = context.add_document("doc_a");
        let id_b = context.add_document("doc_b");
        assert_eq!((id_a, id_b), (1, 2));

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // The node carries pre-resolved `doc_ids` positionally matching `keys`;
        // the unknown key ("ghost") is encoded as id 0 and must be dropped.
        let keys: Vec<ffi::sds> = vec![new_sds("doc_b"), new_sds("ghost"), new_sds("doc_a")];
        let mut doc_ids: Vec<ffi::t_docId> = vec![id_b, 0, id_a];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert_eq!(it.type_(), IteratorType::IdListSorted);
        // Results are sorted; the unknown id (0) is dropped.
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_a);
        let r = it.read().unwrap().unwrap();
        assert_eq!(r.doc_id, id_b);
        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }

    #[test]
    fn eval_ids_all_unresolved_produce_empty_list() {
        let _guard = GlobalGuard::default();
        let context = TestContext::term(IndexFlags_Index_StoreFreqs, std::iter::empty(), false);

        let mut ctx = unsafe { QueryEvalContext::new(context.qctx()) };

        // All keys unresolved: their pre-resolved ids are all 0.
        let keys: Vec<ffi::sds> = vec![new_sds("nope"), new_sds("missing")];
        let mut doc_ids: Vec<ffi::t_docId> = vec![0, 0];

        let mut mock_node = MockQueryNode::new(QueryNodeType::Ids);
        mock_node.set_ids(keys.as_ptr(), doc_ids.as_mut_ptr(), keys.len());
        let node = unsafe { QueryNodeRef::new(mock_node.as_non_null()) };

        let mut it = eval::eval_node(&mut ctx, &node, Config::default())
            .expect("should not be None")
            .into_boxed();

        assert!(it.at_eof());
        assert!(matches!(it.read(), Ok(None)));

        for key in keys {
            // SAFETY: each `key` was allocated by `sdsnewlen` and is freed once.
            unsafe { ffi::sdsfree(key) };
        }
    }
}
