/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, SkipToOutcome,
    maybe_empty::MaybeEmpty,
};

#[derive(Default)]
#[repr(C)]
struct Infinite<'index>(index_result::RSIndexResult<'index>);

/// Suspended counterpart of [`Infinite`]. The mock holds only owned data
/// (no live index borrows), so the suspended form is byte-identical to the
/// active form at any lifetime.
#[repr(C)]
struct InfiniteSuspended(index_result::RSIndexResult<'static>);

impl<'index> RQEIteratorBoxed<'index> for Infinite<'index> {
    type Suspended = InfiniteSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `Infinite<'index>` and `InfiniteSuspended` are both
        // `#[repr(C)]` over `RSIndexResult`; the mock holds no live index
        // borrows, so the lifetime parameter is phantom and the layouts
        // are byte-identical. Box::from_raw reuses the same heap.
        unsafe { Box::from_raw(raw as *mut InfiniteSuspended) }
    }
}

impl RQESuspendedIterator for InfiniteSuspended {
    type Resumed<'a> = Infinite<'a>;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ffi::ValidateStatus) {
        let raw = Box::into_raw(self);
        // SAFETY: layout-identical — see [`Infinite::suspend`].
        let active = unsafe { Box::from_raw(raw as *mut Infinite<'a>) };
        (active, ffi::ValidateStatus_VALIDATE_OK)
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
        doc_id: ffi::t_docId,
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

    fn last_doc_id(&self) -> ffi::t_docId {
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
        let id = (i * 5) as ffi::t_docId;
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
    use rqe_iterators_test_utils::revalidate_via_resume;

    #[test]
    fn revalidate_empty() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = Box::new(MaybeEmpty::<Infinite>::new_empty());
        let (_it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ffi::ValidateStatus_VALIDATE_OK);
    }

    #[test]
    fn revalidate_not_empty() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = Box::new(MaybeEmpty::new(Infinite::default()));
        let (_it, status) = revalidate_via_resume(it, &guard);
        assert_eq!(status, ffi::ValidateStatus_VALIDATE_OK);
    }
}
