/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome, TypeErasedRQEIterator,
    maybe_empty::MaybeEmpty,
};

#[derive(Default)]
#[repr(C)]
struct Infinite<'index>(index_result::RSIndexResult<'index>);

/// Suspended counterpart of [`Infinite`].
///
/// Parameterised by the query lifetime `'query` and holding the `Suspended`
/// representation of the result — *not* an `RSIndexResult<'static>`. `current()`
/// hands out `&mut RSIndexResult`, so a caller may populate the result with
/// borrowed query data (a term or metric); widening that to `'static` would let
/// the suspended value outlive the borrow. Carrying the real `'query` and
/// transitioning through [`RSIndexResult::into_suspended`] keeps the borrow
/// correctly tracked, matching the real iterators.
///
/// `#[repr(C)]` so the byte layout matches [`Infinite`] for the in-place field
/// transition in `suspend`/`resume` (proven layout-identical by the `const _`
/// block below).
#[repr(C)]
struct InfiniteSuspended<'query>(index_result::SuspendedIndexResult<'query>);

// Compile-time proof that `Infinite` and its suspended counterpart are
// layout-identical, so the in-place field transition can reinterpret the same
// allocation. Both are single-field tuple structs over `RawIndexResult<Rf>`
// (layout-compatible across `Rf`, proven in `index_result`), so field 0 sits at
// offset 0 in both and only size/alignment need pinning here.
const _: () = {
    type A<'a> = Infinite<'a>;
    type S<'a> = InfiniteSuspended<'a>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index> RQEIteratorBoxed<'index> for Infinite<'index> {
    type Suspended = InfiniteSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the `result` field to its `Suspended` representation in
        // place, reusing the allocation — rather than casting the whole struct's
        // lifetime to `'static`. Reusing the allocation also keeps the box (and
        // the result it hands out) at a stable address across the cycle.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, uniquely owned). `&raw mut` forms a field pointer to the
        // `result` slot without creating a reference.
        let result_slot = unsafe { &raw mut (*raw).0 };
        // SAFETY: `result_slot` points at an initialised `RSIndexResult<'index>`
        // and is unaliased; `into_suspended_in_place` is a safe widening
        // conversion with no further precondition.
        unsafe { <index_result::RSIndexResult<'index>>::into_suspended_in_place(result_slot) };
        // SAFETY: `Infinite<'index>` and `InfiniteSuspended<'index>` are
        // layout-identical (const proof above); the allocation is reused, so the
        // box address is preserved and the field is now the suspended form.
        unsafe { Box::from_raw(raw as *mut InfiniteSuspended<'index>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for InfiniteSuspended<'query> {
    type Resumed<'a>
        = Infinite<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // Mirror `suspend`: transition the `result` field back to its active form
        // in place, reusing the allocation.
        //
        // SAFETY: `raw` came from `Box::into_raw`; `&raw mut` forms a field
        // pointer to the `result` slot without creating a reference.
        let result_slot = unsafe { &raw mut (*raw).0 };
        // SAFETY: `result_slot` points at an initialised
        // `SuspendedIndexResult<'query>` and is unaliased. `into_active`'s
        // preconditions hold: this mock only ever stores a virtual result over
        // owned data (it just bumps `doc_id`), so there are no index-backed
        // pointers to re-validate; any borrowed query-pipeline data is covered by
        // the `'query: 'a` bound.
        unsafe {
            <index_result::SuspendedIndexResult<'query>>::into_active_in_place::<'a>(result_slot)
        };
        // SAFETY: layout-identical (const proof above); the allocation is reused,
        // so the box address is preserved and the field is now active for `'a`.
        let active = unsafe { Box::from_raw(raw as *mut Infinite<'a>) };
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> ffi::t_docId {
        self.0.doc_id
    }

    fn num_estimated(&self) -> usize {
        usize::MAX
    }
}

impl<'index> RQEIterator<'index> for Infinite<'index> {
    fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
        Some(&mut self.0)
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, RQEIteratorError> {
        self.0.doc_id += 1;
        Ok(Some(&mut self.0))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.0.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.0)))
    }

    fn rewind(&mut self) {
        self.0.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        usize::MAX
    }

    fn last_doc_id(&self) -> DocId {
        self.0.doc_id
    }

    fn at_eof(&self) -> bool {
        false
    }

    fn revalidate(
        &mut self,
        _spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

#[test]
fn type_empty() {
    let it = MaybeEmpty::<Infinite>::new_empty();
    assert_eq!(it.type_(), IteratorType::Empty);
}

#[test]
fn type_not_empty() {
    let it = MaybeEmpty::new(Infinite::default());
    assert_eq!(it.type_(), IteratorType::Mock);
}

#[test]
fn initial_state_empty() {
    let it = MaybeEmpty::<Infinite>::new_empty();

    assert_eq!(it.last_doc_id(), 0);
    assert!(it.at_eof());
    assert_eq!(it.num_estimated(), 0);
}

#[test]
fn initial_state_not_empty() {
    let it = MaybeEmpty::new(Infinite::default());

    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());
    assert_eq!(it.num_estimated(), usize::MAX);
}

#[test]
fn read_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert_eq!(it.num_estimated(), 0);
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn read_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());
    for expected_id in 1..=5 {
        let result = it.read();
        let result = result.unwrap();
        let doc = result.unwrap();
        assert_eq!(doc.doc_id, expected_id);
        assert_eq!(it.last_doc_id(), expected_id);
        assert!(!it.at_eof());
    }
}

#[test]
fn skip_to_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert!(matches!(it.skip_to(1), Ok(None)));
    assert!(it.at_eof());

    assert!(matches!(it.skip_to(42), Ok(None)));
    assert!(matches!(it.skip_to(1000), Ok(None)));
}

#[test]
fn skip_to_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());

    for i in 1..=5 {
        let id = (i * 5) as DocId;
        let outcome = it.skip_to(id).unwrap();
        assert_eq!(
            outcome,
            Some(SkipToOutcome::Found(
                &mut index_result::RSIndexResult::build_virt().doc_id(id).build()
            ))
        );
        assert_eq!(it.last_doc_id(), id);
        assert!(!it.at_eof());
    }
}

#[test]
fn rewind_empty() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());

    it.rewind();
    assert!(it.at_eof());

    assert!(matches!(it.read(), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn rewind_not_empty() {
    let mut it = MaybeEmpty::new(Infinite::default());

    // Read some documents
    for _i in 1..=3 {
        let result = it.read().unwrap();
        assert!(result.is_some());
    }

    assert_eq!(it.last_doc_id(), 3);

    // Rewind
    it.rewind();

    // Check state after rewind
    assert_eq!(it.last_doc_id(), 0);
    assert!(!it.at_eof());

    // Should be able to read from beginning again
    let result = it.read().unwrap();
    let doc = result.unwrap();

    assert_eq!(doc.doc_id, 1);
    assert_eq!(it.last_doc_id(), 1);
}

#[test]
fn revalidate_empty() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let mut it = MaybeEmpty::<Infinite>::new_empty();
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}

#[test]
fn revalidate_not_empty() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let mut it = MaybeEmpty::new(Infinite::default());
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}

#[test]
fn current_empty_returns_none() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();
    assert!(it.current().is_none());
}

#[test]
fn current_not_empty_returns_some() {
    let mut it = MaybeEmpty::new(Infinite::default());
    let current = it.current().unwrap();
    assert_eq!(current.doc_id, 0);
}

#[test]
fn take_iterator_from_some_returns_inner() {
    let mut it = MaybeEmpty::new(Infinite::default());
    let inner = it.take_iterator();
    assert!(inner.is_some());

    // After taking, the MaybeEmpty should behave as empty
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

#[test]
fn take_iterator_from_empty_returns_none() {
    let mut it = MaybeEmpty::<Infinite>::new_empty();
    let inner = it.take_iterator();
    assert!(inner.is_none());

    // Still behaves as empty
    assert!(it.at_eof());
    assert!(matches!(it.read(), Ok(None)));
}

mod via_resume {
    use super::*;
    use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};
    use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};

    #[test]
    fn revalidate_empty() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = Box::new(MaybeEmpty::<Infinite>::new_empty());
        // Resuming an empty wrapper stays at the same (empty) position.
        revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
            .expect("resume failed")
            .expect_ok();
    }

    #[test]
    fn revalidate_not_empty() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = Box::new(MaybeEmpty::new(Infinite::default()));
        // The mock child resumes as `Ok`, so the wrapper does too.
        revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
            .expect("resume failed")
            .expect_ok();
    }

    /// Regression: both `suspend` and `resume` must **reuse the allocation**.
    /// `MaybeEmpty` delegates `current()`/`read()` into its inline child, so the
    /// FFI's cached `header.current` points into the box; rebuilding via
    /// `Box::new` (the previous `resume`) would move the child and dangle it.
    #[test]
    fn resume_preserves_box_address() {
        use rqe_iterators::{RQEIteratorBoxed, RQESuspendedIterator};

        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = Box::new(MaybeEmpty::new(Infinite::default()));
        let addr_before = &*it as *const _ as usize;

        let suspended = it.suspend();
        assert_eq!(
            &*suspended as *const _ as usize, addr_before,
            "suspend must reuse the allocation"
        );

        let active = match suspended.resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Aborted => panic!("infinite child should not abort"),
        };
        assert_eq!(
            &*active as *const _ as usize, addr_before,
            "resume must reuse the allocation (MaybeEmpty delegates into its inline child)"
        );
    }

    // The local `Infinite` mock always resumes `Ok`/`Unchanged`, so the shared
    // `Mock` (steerable via `MockData`) drives the remaining resume outcomes:
    // Moved, Aborted, and a child-resume error.

    /// `Some` child that resumes `Moved` → the wrapper forwards `Moved`.
    #[test]
    fn resume_some_child_moved_forwards_moved() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([1, 2, 3]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Move);
        let it = Box::new(MaybeEmpty::new(child));

        let outcome = it.suspend().resume(&guard).expect("resume must not error");
        assert!(
            matches!(outcome, ResumeOutcome::Moved(_)),
            "a child that moved must forward Moved",
        );
    }

    /// `Some` child that aborts → the whole wrapper aborts (MaybeEmpty has no
    /// virtual fallback); the moved-from child slot is torn down as
    /// `None(Empty)` before the box is freed.
    #[test]
    fn resume_some_child_aborted_forwards_aborted() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([1, 2, 3]);
        child
            .data()
            .set_revalidate_result(MockRevalidateResult::Abort);
        let it = Box::new(MaybeEmpty::new(child));

        let outcome = it.suspend().resume(&guard).expect("resume must not error");
        assert!(
            matches!(outcome, ResumeOutcome::Aborted),
            "an aborted child must abort the whole MaybeEmpty",
        );
    }

    /// `Some` child whose resume itself fails → the error propagates out, after
    /// the slot is restored to `None(Empty)` so the box drops soundly.
    #[test]
    fn resume_some_child_error_propagates() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let child = Mock::new([1, 2, 3]);
        child
            .data()
            .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));
        let it = Box::new(MaybeEmpty::new(child));

        let result = it.suspend().resume(&guard);
        assert!(
            matches!(result, Err(RQEIteratorError::TimedOut)),
            "a child resume error must propagate out of MaybeEmpty::resume",
        );
    }

    /// Suspended-form accessors on the `None` arm: an empty wrapper reports
    /// position 0 and estimate 0 (delegating to `Empty`).
    #[test]
    fn suspended_accessors_none_arm() {
        let it = Box::new(MaybeEmpty::<Infinite>::new_empty());
        let suspended = it.suspend();
        assert_eq!(RQESuspendedIterator::last_doc_id(&*suspended), 0);
        assert_eq!(RQESuspendedIterator::num_estimated(&*suspended), 0);
    }

    /// Suspended-form accessors on the `Some` arm: they delegate to the child's
    /// suspended accessors.
    #[test]
    fn suspended_accessors_some_arm() {
        let mut it = Box::new(MaybeEmpty::new(Mock::new([10, 20, 30])));
        let _ = it.read().unwrap(); // advance the child to doc 10
        let suspended = it.suspend();
        assert_eq!(RQESuspendedIterator::last_doc_id(&*suspended), 10);
        assert_eq!(RQESuspendedIterator::num_estimated(&*suspended), 3);
    }
}
