/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use rqe_iterators::{
    RQEIterator,
    empty::Empty,
    optional_reducer::{OptionalReduction, new_optional_iterator},
    wildcard::Wildcard,
};

mod optional_reducer_tests {
    use super::*;

    /// Build the minimum valid FFI context required to exercise the fallback
    /// path through [`new_wildcard_iterator`]: `diskSpec` and `rule` are null,
    /// so a plain [`Wildcard`] is returned for the given `max_doc_id`.
    ///
    /// The structs are heap-allocated so their addresses remain stable for the
    /// duration of the call and the required pointer-chain is well-defined.
    ///
    /// # Safety
    ///
    /// All pointers in the returned [`ffi::QueryEvalCtx`] are valid for the
    /// lifetime of the returned boxes and must not outlive them.
    unsafe fn build_fallback_query(
        max_doc_id: ffi::t_docId,
    ) -> (
        Box<ffi::DocTable>,
        Box<ffi::IndexSpec>,
        Box<ffi::RedisSearchCtx>,
        Box<ffi::QueryEvalCtx>,
    ) {
        // SAFETY: All four types are `repr(C)` C structs whose fields are
        // either pointers (nullable) or arithmetic types.  Zero-initialising
        // them produces null pointers and zero numeric values, which is the
        // intended state for the fallback path (diskSpec=null, rule=null).
        let mut doc_table: Box<ffi::DocTable> = Box::new(unsafe { std::mem::zeroed() });
        doc_table.maxDocId = max_doc_id;

        let spec: Box<ffi::IndexSpec> = Box::new(unsafe { std::mem::zeroed() });

        let mut sctx: Box<ffi::RedisSearchCtx> = Box::new(unsafe { std::mem::zeroed() });
        sctx.spec = &raw const *spec as *mut _;

        let mut query: Box<ffi::QueryEvalCtx> = Box::new(unsafe { std::mem::zeroed() });
        query.sctx = &raw mut *sctx;
        query.docTable = &raw mut *doc_table;

        (doc_table, spec, sctx, query)
    }

    /// Shortcircuit 1: when the child iterator is at EOF, the factory drops it
    /// and returns a wildcard that covers the full document range.
    #[test]
    fn shortcircuit_1_empty_child_returns_wildcard_fallback() {
        const MAX_DOC_ID: ffi::t_docId = 10;

        // SAFETY:
        // - `build_fallback_query` produces a valid QueryEvalCtx whose
        //   pointer-chain satisfies the preconditions of `new_optional_iterator`.
        // - diskSpec=null and rule=null, so `new_wildcard_iterator` takes the
        //   fallback path and never invokes any C code beyond pointer reads.
        // - The returned boxes outlive the `new_optional_iterator` call.
        let result = unsafe {
            let (_doc_table, _spec, _sctx, mut query_box) = build_fallback_query(MAX_DOC_ID);
            let query = NonNull::new(&raw mut *query_box).unwrap();
            new_optional_iterator(Empty, 1.0, query, MAX_DOC_ID)
        };

        let OptionalReduction::WildcardFallback(wc) = result else {
            panic!("expected WildcardFallback, got a different variant");
        };

        // The fallback wildcard covers all documents up to maxDocId.
        assert_eq!(wc.num_estimated(), MAX_DOC_ID as usize);
        // It starts before the first document and is not yet at EOF.
        assert!(!wc.at_eof());
    }

    /// Shortcircuit 2: when the child iterator is a wildcard (and not at EOF),
    /// the factory returns it as-is after applying the requested weight to the
    /// current result.
    ///
    /// The `query` pointer is never dereferenced in this branch, so a dangling
    /// pointer is sufficient.
    #[test]
    fn shortcircuit_2_wildcard_child_returned_as_passthrough_with_weight_applied() {
        const INITIAL_WEIGHT: f64 = 1.0;
        const NEW_WEIGHT: f64 = 3.5;
        const MAX_DOC_ID: ffi::t_docId = 100;

        let mut child = Wildcard::new(100, INITIAL_WEIGHT);
        // Advance the child so that `current()` holds a real document result.
        let read_result = child.read().unwrap().expect("first read must succeed");
        assert_eq!(read_result.doc_id, 1);
        assert_eq!(read_result.weight, INITIAL_WEIGHT);

        // SAFETY: the wildcard passthrough branch never dereferences `query`.
        let result =
            unsafe { new_optional_iterator(child, NEW_WEIGHT, NonNull::dangling(), MAX_DOC_ID) };

        let OptionalReduction::WildcardPassthrough(mut child) = result else {
            panic!("expected WildcardPassthrough, got a different variant");
        };

        // The factory must apply the new weight to the current result.
        let current = child.current().expect("wildcard current must be Some");
        assert_eq!(
            current.weight, NEW_WEIGHT,
            "factory must apply weight to current result"
        );
        // The read position must be preserved.
        assert_eq!(current.doc_id, 1, "read position must not change");
    }
}
