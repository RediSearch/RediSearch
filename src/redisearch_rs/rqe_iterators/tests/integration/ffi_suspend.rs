/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Suspend/resume of the iterators built by the FFI constructors, driven through the C vtable
//! the way the query pipeline drives them across a spec read-lock release.

use ffi::{
    IteratorStatus_ITERATOR_OK, QueryIterator, ValidateStatus, ValidateStatus_VALIDATE_ABORTED,
    ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
};

/// Drive the lock-release protocol on `it`: `Suspend` before the lock is dropped, then
/// `Revalidate` once it is re-acquired.
///
/// # Safety
///
/// `it` is a live iterator built by an FFI constructor, and `spec` is the spec it is revalidated
/// against.
unsafe fn suspend_then_revalidate(
    it: *mut QueryIterator,
    spec: *mut ffi::IndexSpec,
) -> ValidateStatus {
    // SAFETY: guaranteed by the caller; every FFI constructor populates both callbacks.
    unsafe {
        ((*it).Suspend.expect("Suspend must be populated"))(it);
        ((*it).Revalidate.expect("Revalidate must be populated"))(it, spec)
    }
}

/// Read the next document id off `it`, or `None` once it stops reporting `ITERATOR_OK`.
///
/// # Safety
///
/// `it` is a live, active iterator built by an FFI constructor.
unsafe fn read_doc_id(it: *mut QueryIterator) -> Option<u64> {
    // SAFETY: guaranteed by the caller.
    unsafe { ((*it).Read.expect("Read must be populated"))(it) == IteratorStatus_ITERATOR_OK }
        // SAFETY: as above; a successful read leaves the header pointing at the result.
        .then(|| unsafe { (*it).lastDocId })
}

/// Free `it` through its vtable.
///
/// # Safety
///
/// `it` is the owning pointer returned by an FFI constructor and is not used afterwards.
unsafe fn free(it: *mut QueryIterator) {
    // SAFETY: guaranteed by the caller.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };
}

#[cfg(not(miri))]
mod tag {
    use super::*;
    use field::FieldMaskOrIndex;
    use index_result::RSIndexResult;
    use inverted_index::{doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding};
    use iterators_ffi::inverted_index::NewInvIndIterator_TagQuery;
    use query_term::RSQueryTerm;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    /// A tag iterator over `test_tag` in `ctx`, lowered to the C ABI.
    fn tag_iterator(ctx: &TestContext) -> *mut QueryIterator {
        let ii: *const ffi::InvertedIndex = (ctx.tag_inverted_index()
            as *mut inverted_index::opaque::InvertedIndex)
            .cast_const()
            .cast();
        let term = Box::into_raw(RSQueryTerm::new("test_tag", 0, 0));
        // SAFETY: `ctx` owns a valid search context, tag index and its `test_tag` inverted index,
        // all outliving the iterator; ownership of `term` moves into the iterator.
        unsafe {
            NewInvIndIterator_TagQuery(
                ii,
                ctx.tag_index().as_ptr(),
                ctx.sctx.as_ptr(),
                FieldMaskOrIndex::mask_all(),
                term,
                1.0,
            )
        }
    }

    #[test]
    fn a_suspended_tag_iterator_resumes_where_it_left_off() {
        let _guard = GlobalGuard::default();
        let ctx = TestContext::tag([1, 3, 5].into_iter());
        let it = tag_iterator(&ctx);

        // SAFETY: `it` is live, and `ctx.sctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(read_doc_id(it), Some(1));
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx.as_ptr()).spec),
                ValidateStatus_VALIDATE_OK
            );
            assert_eq!((*it).lastDocId, 1, "resume must keep the cached position");
            assert_eq!(read_doc_id(it), Some(3));
            assert_eq!(read_doc_id(it), Some(5));
            assert_eq!(read_doc_id(it), None);
            free(it);
        }
    }

    /// A tag whose inverted index was collected while the lock was released cannot be resumed;
    /// the abort has to reach C so it frees the iterator.
    #[test]
    fn a_tag_iterator_whose_index_was_collected_aborts_on_resume() {
        let _guard = GlobalGuard::default();
        let ctx = TestContext::tag([1, 3, 5].into_iter());
        let it = tag_iterator(&ctx);
        // SAFETY: `it` is live.
        assert_eq!(unsafe { read_doc_id(it) }, Some(1));

        let old_ii: *mut inverted_index::opaque::InvertedIndex = ctx.tag_inverted_index();
        // SAFETY: the tag index was created by the test context and its `values` is a valid
        // TrieMap.
        let trie = unsafe {
            &mut *ctx
                .tag_index()
                .as_ref()
                .values
                .cast::<trie_rs::TrieMapOpaque>()
        };
        assert!(trie.remove(b"test_tag").is_some());

        // SAFETY: `it` is live, and `ctx.sctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx.as_ptr()).spec),
                ValidateStatus_VALIDATE_ABORTED
            );
            free(it);
        }
        // SAFETY: removing the TrieMap entry made this test the sole owner of the inverted index,
        // which was heap-allocated by the test context.
        unsafe { drop(Box::from_raw(old_ii)) };
    }

    /// A tag whose current document was garbage-collected while the lock was released resumes
    /// on the next document; C has to learn it moved and see the result it moved to.
    #[test]
    fn a_tag_iterator_whose_current_document_was_collected_reports_a_move() {
        let _guard = GlobalGuard::default();
        let ctx = TestContext::tag([1, 3, 5].into_iter());
        let it = tag_iterator(&ctx);
        // SAFETY: `it` is live.
        unsafe {
            assert_eq!(read_doc_id(it), Some(1));
            assert_eq!(read_doc_id(it), Some(3));
        }

        let ii = DocIdsOnly::from_mut_opaque(ctx.tag_inverted_index());
        let delta = ii
            .scan_gc(
                |doc_id| doc_id != 3,
                None::<fn(&RSIndexResult, &inverted_index::RepairContext<'_>)>,
            )
            .expect("scan GC failed")
            .expect("no GC scan delta");
        assert_eq!(ii.apply_gc(delta).entries_removed, 1);

        // SAFETY: `it` is live, and `ctx.sctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx.as_ptr()).spec),
                ValidateStatus_VALIDATE_MOVED
            );
            assert_eq!((*it).lastDocId, 5, "resume lands on the next document");
            assert_eq!(
                (*(*it).current).docId,
                5,
                "C reads the result the iterator moved to"
            );
            assert_eq!(read_doc_id(it), None);
            free(it);
        }
    }
}
