/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! QN_TOKEN → single-term inverted-index reader.
//!
//! Covers the in-memory evaluation path, which opens a term reader on the
//! spec's inverted index. The search-on-disk path is not exercised here (it
//! requires the enterprise on-disk index).
//!
//! Disabled under Miri: `TestContext` calls into the C library, which Miri
//! cannot execute.
#![cfg(not(miri))]

use std::ffi::CString;

use ffi::{
    IndexFlags_Index_StoreByteOffsets, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreTermOffsets,
};
use index_result::{RSIndexResult, RSOffsetSlice};
use query::mock::MockQueryNode;
use query_eval::{
    QueryEvalContext, QueryNodeRef,
    eval::{self, Config, EvalResult},
};
use query_term::RSQueryTerm;
use query_types::QueryNodeType;
use rqe_core::{FieldMask, RS_FIELDMASK_ALL};
use rqe_iterators::{IteratorType, RQEIterator};
use rqe_iterators_test_utils::{GlobalGuard, TestContext};

/// The term populated by [`TokenFixture`]. `TestContext::term` registers the
/// inverted index under this key, and the in-memory reader looks it up by the
/// query token's string, so a query token must match this exactly to hit it.
const INDEXED_TERM: &str = "term";

/// Owns everything a `QN_TOKEN` evaluation borrows — the FFI `TestContext`, the
/// [`QueryEvalContext`], the node, and the token's backing string — so
/// [`eval`](Self::eval) can hand the evaluated iterator back to the caller.
struct TokenFixture {
    _guard: GlobalGuard,
    _context: TestContext,
    ctx: QueryEvalContext,
    node: MockQueryNode,
    // Backs the token's `str`/`len` pointer; must outlive `node`.
    _token: CString,
}

impl TokenFixture {
    /// Term inverted index over documents `1..=3`, all carrying [`INDEXED_TERM`],
    /// with a `QN_TOKEN` node querying `token`.
    fn new(token: &str) -> Self {
        let _guard = GlobalGuard::default();

        let flags = IndexFlags_Index_StoreFreqs
            | IndexFlags_Index_StoreTermOffsets
            | IndexFlags_Index_StoreFieldFlags
            | IndexFlags_Index_StoreByteOffsets;

        // A single term offset per record is enough; the reader only needs a
        // well-formed posting list to walk.
        const OFFSETS: &[u8] = &[0];
        let records = (1u64..=3).map(|doc_id| {
            let mut term = RSQueryTerm::new(INDEXED_TERM, 1, 0);
            term.set_idf(5.0);
            term.set_bm25_idf(10.0);
            RSIndexResult::build_term()
                .borrowed_record(Some(term), RSOffsetSlice::from_slice(OFFSETS))
                .doc_id(doc_id)
                // All (low 32) field-mask bits set so the reader's field-mask
                // filter never excludes a document. Wider values can overflow
                // the field-mask encoder for a non-wide schema.
                .field_mask(u32::MAX as FieldMask)
                .frequency(1)
                .build()
        });
        let context = TestContext::term(flags, records, false);

        let qctx = context.qctx();
        // The in-memory token path intersects the node's field mask with the
        // query-wide one (`opts.fieldmask`); the zero-init options leave it 0,
        // which would mask out every field. Set it to "all fields".
        // SAFETY: `qctx.opts` is a valid, exclusively-owned `RSSearchOptions`.
        unsafe {
            let opts = (*qctx.as_ptr()).opts.cast_mut();
            (*opts).fieldmask = !0;
        };
        // SAFETY: `qctx` upholds the `QueryEvalContext::new` invariants and is
        // exclusively owned by this fixture.
        let ctx = unsafe { QueryEvalContext::new(qctx) };

        let token = CString::new(token).expect("token must not contain a NUL byte");
        let mut node = MockQueryNode::new(QueryNodeType::Token);
        node.opts_mut().weight = 1.0;
        node.opts_mut().field_mask = RS_FIELDMASK_ALL;
        node.set_token(token.as_ptr().cast_mut(), token.as_bytes().len());

        Self {
            _guard,
            _context: context,
            ctx,
            node,
            _token: token,
        }
    }

    /// Evaluate the node, returning the boxed iterator, or `None` when
    /// evaluation produced no iterator.
    fn eval(&mut self) -> Option<EvalResult<'_>> {
        // SAFETY: `self.node` is a valid, live `RSQueryNode` for the call.
        let node_ref = unsafe { QueryNodeRef::new(self.node.as_non_null()) };
        eval::eval_node(&mut self.ctx, &node_ref, Config::default()).map(|e| e.into_boxed())
    }
}

#[test]
fn eval_token_reads_matching_docs() {
    let mut fixture = TokenFixture::new(INDEXED_TERM);
    let mut it = fixture
        .eval()
        .expect("a term present in the index must build an iterator");
    assert_eq!(it.type_(), IteratorType::InvIdxTerm);

    for expected in [1, 2, 3] {
        let r = it.read().unwrap().expect("should have a result");
        assert_eq!(r.doc_id, expected);
    }
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn eval_token_absent_term_yields_no_iterator() {
    // A term with no inverted index in the spec: the reader cannot be opened,
    // so evaluation short-circuits to `None` (no iterator).
    let mut fixture = TokenFixture::new("absent");
    assert!(
        fixture.eval().is_none(),
        "a term absent from the index must not build an iterator"
    );
}
