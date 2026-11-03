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
    ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK, ValidateStatus_VALIDATE_TIMEOUT,
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
    let status = unsafe { ((*it).Read.expect("Read must be populated"))(it) };
    // SAFETY: as above.
    (status == IteratorStatus_ITERATOR_OK).then(|| unsafe { (*it).lastDocId })
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

mod not {
    use super::*;
    use iterators_ffi::not::NewNotIterator;
    use rqe_iterators::interop::RQEIteratorWrapper;
    use rqe_iterators_test_utils::MockContext;

    use crate::utils::{Mock, MockRevalidateResult};

    /// A NOT iterator over a Rust child matching `2` and `4`, up to doc id `6`, lowered to the
    /// C ABI.
    fn not_iterator(ctx: &MockContext, child_outcome: MockRevalidateResult) -> *mut QueryIterator {
        let child = Mock::new([2, 4]);
        child.data().set_revalidate_result(child_outcome);
        let child = RQEIteratorWrapper::boxed_new(child);
        // SAFETY: `child` is a fresh, unaliased iterator, and `ctx` owns a valid query evaluation
        // context, search context and spec, all outliving the iterator.
        unsafe { NewNotIterator(child, 6, 1.0, ctx.qctx().as_ptr()) }
    }

    /// A parent's aggregate result points into this iterator's result, so the cycle must keep
    /// the result where it was; while suspended, C is handed no result at all.
    #[test]
    fn a_suspended_not_iterator_resumes_in_place() {
        let ctx = MockContext::new(6, 6);
        let it = not_iterator(&ctx, MockRevalidateResult::Ok);

        // SAFETY: `it` is live, and `ctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(read_doc_id(it), Some(1));
            let current = (*it).current;
            ((*it).Suspend.expect("Suspend must be populated"))(it);
            assert!(
                (*it).current.is_null(),
                "a suspended iterator exposes no result"
            );
            assert_eq!(
                ((*it).Revalidate.expect("Revalidate must be populated"))(
                    it,
                    (*ctx.sctx().as_ptr()).spec
                ),
                ValidateStatus_VALIDATE_OK
            );
            assert_eq!((*it).current, current, "the result must not have moved");
            assert_eq!(read_doc_id(it), Some(3));
            assert_eq!(read_doc_id(it), Some(5));
            assert_eq!(read_doc_id(it), Some(6));
            assert_eq!(read_doc_id(it), None);
            free(it);
        }
    }

    /// A child that times out on resume takes the NOT iterator down with it; the timeout has to
    /// reach C, and the iterator it consumed must not be freed twice.
    #[test]
    fn a_not_iterator_whose_child_times_out_reports_a_timeout() {
        let ctx = MockContext::new(6, 6);
        let it = not_iterator(&ctx, MockRevalidateResult::TimedOut);

        // SAFETY: `it` is live, and `ctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(read_doc_id(it), Some(1));
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec),
                ValidateStatus_VALIDATE_TIMEOUT
            );
            free(it);
        }
    }
}

/// NOT over a spec that indexes every document, which takes the optimized path: the complement is
/// enumerated over the spec's existing-documents index rather than a plain id range.
#[cfg(not(miri))]
mod not_optimized {
    use super::*;
    use iterators_ffi::not::NewNotIterator;
    use rqe_iterators::interop::RQEIteratorWrapper;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    use crate::utils::Mock;

    /// When the existing-documents index NOT enumerates over is collected while the lock is
    /// released, there is nothing left to take the complement of: the abort has to reach C, and
    /// the iterator the resume consumed must not be freed again.
    #[test]
    fn a_not_iterator_whose_wildcard_base_was_collected_aborts_on_resume() {
        let _guard = GlobalGuard::default();
        let ctx = TestContext::wildcard(1..=6);
        let spec = ctx.spec;
        // SAFETY: the test context's spec is live and was parsed with a schema rule.
        unsafe {
            let rule = (*spec).rule;
            assert!(!rule.is_null(), "the test spec must carry a schema rule");
            (*rule).index_all = true;
        }
        let child = RQEIteratorWrapper::boxed_new(Mock::new([2, 4]));
        // SAFETY: `child` is a fresh, unaliased iterator, and `ctx` owns a valid query evaluation
        // context, search context and spec, all outliving the iterator.
        let it = unsafe { NewNotIterator(child, 6, 1.0, ctx.qctx().as_ptr()) };
        // SAFETY: `it` is live.
        unsafe {
            assert_eq!((*it).type_, rqe_iterators::IteratorType::NotOptimized);
            assert_eq!(read_doc_id(it), Some(1));
        }

        // SAFETY: the spec is live; detaching its existing-documents index is what collecting it
        // looks like from the iterator's side.
        let existing_docs =
            unsafe { std::mem::replace(&mut (*spec).existingDocs, std::ptr::null_mut()) };

        // SAFETY: `it` is live, and `ctx.sctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx.as_ptr()).spec),
                ValidateStatus_VALIDATE_ABORTED
            );
            free(it);
            // Hand the index back so the test context frees it with the spec.
            (*spec).existingDocs = existing_docs;
        }
    }
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

    /// The resume semantics are covered through the safe API in `inverted_index::tag`; what
    /// this checks is that both outcomes reach C through the tag shim with the header in sync.
    /// An unchanged index must keep `current` where it was, since a parent's aggregate result
    /// points into it. A garbage-collected current document resumes on the next one, and C has to
    /// learn it moved and see the result it moved to.
    #[test]
    fn a_tag_iterator_keeps_the_header_in_sync_across_resumes() {
        let _guard = GlobalGuard::default();
        let ctx = TestContext::tag([1, 3, 5].into_iter());
        let it = tag_iterator(&ctx);
        // SAFETY: `it` is live, and `ctx.sctx` carries the spec the iterator was built against.
        unsafe {
            assert_eq!(read_doc_id(it), Some(1));
            assert_eq!(read_doc_id(it), Some(3));
            let current = (*it).current;
            assert_eq!(
                suspend_then_revalidate(it, (*ctx.sctx.as_ptr()).spec),
                ValidateStatus_VALIDATE_OK
            );
            assert_eq!((*it).lastDocId, 3, "resume must keep the cached position");
            assert_eq!((*it).current, current, "the result must not have moved");
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

    /// A tag whose inverted index was collected while the lock was released cannot be resumed:
    /// the abort has to reach C, and the shim frees its box without dropping the payload the
    /// resume already consumed, so C can then free the wrapper.
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
}
