/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Suspend/resume of [`NotIteratorEnum`], for each of its variants.

use std::cell::Cell;

use ffi::{IndexSpec, QueryIterator, ValidateStatus};
use index_result::RSIndexResult;
use iterators_ffi::not::{NotFfi, NotIteratorEnum, NotOptimizedFfi};
use rqe_core::DocId;
use rqe_iterators::{
    NewWildcardIterator, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    ResumeOutcome,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    utils::{AnyTimeoutContext, NoTimeoutChecker},
    wildcard::Wildcard,
};
use rqe_iterators_test_utils::MockContext;
use rstest::rstest;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const MAX_DOC_ID: DocId = 5;

#[derive(Debug, Clone, Copy)]
enum Variant {
    Not,
    NotOptimized,
}

/// A NOT iterator over `1..=MAX_DOC_ID` excluding 2 and 4, built as `variant`.
fn not_iterator(variant: Variant) -> Box<NotIteratorEnum<'static>> {
    not_iterator_over(variant, child())
}

/// The C child excluding 2 and 4.
fn child() -> CRQEIterator {
    CRQEIterator::from_rust_leaf(IdListSorted::new(vec![2, 4]))
}

/// The `Revalidate` callback of a [`child_resuming_with`] child.
type Revalidate = unsafe extern "C" fn(*mut QueryIterator, *mut IndexSpec) -> ValidateStatus;

thread_local! {
    /// The callback [`child_resuming_with`] replaced, which its stub forwards to.
    static REVALIDATE: Cell<Option<Revalidate>> = const { Cell::new(None) };
}

/// Resumes through the replaced callback, then reports `STATUS`, so the child
/// is left active whatever its parent then does with it.
unsafe extern "C" fn revalidate_then_report<const STATUS: ValidateStatus>(
    it: *mut QueryIterator,
    spec: *mut IndexSpec,
) -> ValidateStatus {
    let revalidate = REVALIDATE.get().expect("the replaced callback was saved");
    // SAFETY: forwarding the caller's arguments to the callback they were meant for.
    let status = unsafe { revalidate(it, spec) };
    assert_eq!(status, ffi::ValidateStatus_VALIDATE_OK);
    STATUS
}

/// [`child`], whose resume reports `STATUS` instead of the real outcome.
fn child_resuming_with<const STATUS: ValidateStatus>() -> CRQEIterator {
    let child = child();
    // SAFETY: `child` exclusively owns its header.
    let header = unsafe { &mut *child.as_raw().as_ptr() };
    REVALIDATE.set(header.Revalidate);
    // The stub resumes through the replaced callback, so every field stays valid.
    header.Revalidate = Some(revalidate_then_report::<STATUS>);
    child
}

/// A NOT iterator over `1..=MAX_DOC_ID` excluding `child`'s ids, built as `variant`.
fn not_iterator_over(variant: Variant, child: CRQEIterator) -> Box<NotIteratorEnum<'static>> {
    let timeout = || AnyTimeoutContext::NoTimeout(NoTimeoutChecker);
    Box::new(match variant {
        Variant::Not => NotIteratorEnum::Not(NotFfi::new(child, MAX_DOC_ID, 1.0, timeout())),
        Variant::NotOptimized => NotIteratorEnum::NotOptimized(NotOptimizedFfi::new(
            NewWildcardIterator::NotOptimized(Wildcard::new(MAX_DOC_ID, 1.0)),
            child,
            MAX_DOC_ID,
            1.0,
            timeout(),
        )),
    })
}

/// The address of the allocation behind `b`.
const fn address<T>(b: &T) -> *const u8 {
    std::ptr::from_ref(b).cast()
}

/// Panics unless `it` is `variant`.
#[track_caller]
fn assert_is(it: &NotIteratorEnum<'_>, variant: Variant) {
    assert!(matches!(
        (variant, it),
        (Variant::Not, NotIteratorEnum::Not(_))
            | (Variant::NotOptimized, NotIteratorEnum::NotOptimized(_))
    ));
}

fn next_doc_id(it: &mut NotIteratorEnum<'_>) -> Option<DocId> {
    it.read().expect("read failed").map(|r| r.doc_id)
}

/// A NOT resumes where it left off, in the same allocation and variant.
#[rstest]
fn a_resumed_not_iterator_keeps_its_variant_allocation_and_position(
    #[values(Variant::Not, Variant::NotOptimized)] variant: Variant,
) {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = not_iterator(variant);
    assert_eq!(next_doc_id(&mut it), Some(1));
    let estimate = it.num_estimated();
    let allocation = address(&*it);
    let result = address(it.current().expect("positioned on a document"));

    let suspended = it.suspend();
    assert_eq!(address(&*suspended), allocation);
    assert_eq!(suspended.last_doc_id(), 1);
    assert_eq!(suspended.num_estimated(), estimate);

    let Ok(ResumeOutcome::Ok(mut it)) = suspended.resume(&mock_ctx.spec_read()) else {
        panic!("an untouched NOT resumes unchanged");
    };
    assert_eq!(address(&*it), allocation);
    assert_is(&it, variant);
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(
        address(it.current().expect("still on a document")),
        result,
        "a parent's aggregate result points into the result, so it must not move"
    );
    assert_eq!(next_doc_id(&mut it), Some(3));
    assert_eq!(next_doc_id(&mut it), Some(5));
    assert_eq!(next_doc_id(&mut it), None);
}

/// A NOT holding a non-virtual result aborts on resume.
#[rstest]
fn a_not_iterator_holding_a_non_virtual_result_aborts_on_resume(
    #[values(Variant::Not, Variant::NotOptimized)] variant: Variant,
) {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = not_iterator(variant);
    assert_eq!(next_doc_id(&mut it), Some(1));
    *it.current().expect("positioned on a document") = RSIndexResult::build_numeric(1.0).build();

    assert!(matches!(
        it.suspend().resume(&mock_ctx.spec_read()),
        Ok(ResumeOutcome::Aborted)
    ));
}

/// A NOT keeps its own cursor, so a child that moved while suspended leaves it
/// where it was.
#[rstest]
fn a_not_iterator_whose_child_moved_resumes_unchanged(
    #[values(Variant::Not, Variant::NotOptimized)] variant: Variant,
) {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = not_iterator_over(
        variant,
        child_resuming_with::<{ ffi::ValidateStatus_VALIDATE_MOVED }>(),
    );
    assert_eq!(next_doc_id(&mut it), Some(1));
    let allocation = address(&*it);

    let Ok(ResumeOutcome::Ok(mut it)) = it.suspend().resume(&mock_ctx.spec_read()) else {
        panic!("a child move does not move a NOT");
    };
    assert_eq!(address(&*it), allocation);
    assert_is(&it, variant);
    assert_eq!(it.last_doc_id(), 1);
    assert_eq!(next_doc_id(&mut it), Some(3));
}

/// A child that fails to resume fails the NOT's resume, which frees the
/// iterator without dropping the child twice.
#[rstest]
fn a_not_iterator_whose_child_times_out_fails_to_resume(
    #[values(Variant::Not, Variant::NotOptimized)] variant: Variant,
) {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = not_iterator_over(
        variant,
        child_resuming_with::<{ ffi::ValidateStatus_VALIDATE_TIMEOUT }>(),
    );
    assert_eq!(next_doc_id(&mut it), Some(1));

    assert!(matches!(
        it.suspend().resume(&mock_ctx.spec_read()),
        Err(RQEIteratorError::Stopped)
    ));
}

/// An optimized NOT whose current document was garbage-collected from the
/// existing documents while suspended moves onto the next one it does not
/// exclude, in the same allocation.
#[test]
#[cfg(not(miri))]
fn an_optimized_not_iterator_whose_current_document_was_collected_moves_on_resume() {
    use inverted_index::{RepairContext, doc_ids_only::DocIdsOnly, opaque::OpaqueEncoding};
    use rqe_iterators::wildcard::RawOptimizedWildcard;
    use rqe_iterators_test_utils::{GlobalGuard, TestContext};

    let _guard = GlobalGuard::default();
    let ctx = TestContext::wildcard([1, 2, 3, 5].into_iter());
    let existing = DocIdsOnly::from_opaque(ctx.wildcard_inverted_index());
    let wildcard = NewWildcardIterator::Optimized(RawOptimizedWildcard::DocIdsOnly(
        rqe_iterators::inverted_index::Wildcard::new(existing.reader(), 1.0),
    ));
    let mut it = Box::new(NotIteratorEnum::NotOptimized(NotOptimizedFfi::new(
        wildcard,
        child(),
        MAX_DOC_ID,
        1.0,
        AnyTimeoutContext::NoTimeout(NoTimeoutChecker),
    )));
    assert_eq!(next_doc_id(&mut it), Some(1));
    assert_eq!(next_doc_id(&mut it), Some(3));
    let allocation = address(&*it);

    let suspended = it.suspend();
    let existing = DocIdsOnly::from_mut_opaque(ctx.wildcard_inverted_index());
    let delta = existing
        .scan_gc(
            |doc_id| doc_id != 3,
            None::<fn(&RSIndexResult, &RepairContext<'_>)>,
        )
        .expect("scan GC failed")
        .expect("no GC scan delta");
    assert_eq!(existing.apply_gc(delta).entries_removed, 1);

    let Ok(ResumeOutcome::Moved(mut it)) = suspended.resume(&ctx.spec_read()) else {
        panic!("losing the current document moves the iterator");
    };
    assert_eq!(address(&*it), allocation);
    assert_is(&it, Variant::NotOptimized);
    assert_eq!(it.last_doc_id(), 5);
    assert_eq!(it.current().map(|r| r.doc_id), Some(5));
    assert_eq!(next_doc_id(&mut it), None);
}

/// `FT.PROFILE` prints the tree after the query has suspended it, so a suspended NOT must print
/// as its active form did, child included.
// The reply mock uses extern static items, which miri cannot run.
#[cfg(not(miri))]
#[rstest]
fn a_suspended_not_iterator_prints_the_profile_of_its_active_form(
    #[values(Variant::Not, Variant::NotOptimized)] variant: Variant,
) {
    let mut it = not_iterator(variant);
    assert_eq!(next_doc_id(&mut it), Some(1));
    let suspended = rqe_iterators_test_utils::assert_suspended_prints_as_active(it);

    assert_eq!(
        format!("{suspended:?}"),
        r#"{"Type": "NOT", "Child iterator": {"Type": "ID-LIST-SORTED"}}"#,
    );
}
