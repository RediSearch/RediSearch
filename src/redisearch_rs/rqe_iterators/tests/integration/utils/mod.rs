/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod mock_enterprise_iterators;
mod mock_iterator;
mod wildcard_helper;
pub(crate) use mock_enterprise_iterators::{MOCK_DISK_WILDCARD_TOP_ID, init_enterprise_iterators};
pub(crate) use mock_iterator::{Mock, MockData, MockIteratorError, MockRevalidateResult, MockVec};
pub(crate) use wildcard_helper::WildcardHelper;

use index_result::RSIndexResult;
use rqe_iterators::{BoxedRQEIterator, IteratorType, RQEIterator, RQEIteratorError, SkipToOutcome};

/// A mock iterator that produces results with a specific [`FieldMask`](inverted_index::FieldMask).
///
/// Each [`read`](RQEIterator::read) yields the next doc_id from the
/// pre-configured list with the fixed `mask` written into
/// `RSIndexResult::field_mask`.
pub(crate) struct FieldMaskMock<'index> {
    doc_ids: Vec<u64>,
    next: usize,
    result: RSIndexResult<'index>,
    mask: inverted_index::FieldMask,
}

impl<'index> FieldMaskMock<'index> {
    pub(crate) fn new(doc_ids: Vec<u64>, mask: inverted_index::FieldMask) -> Self {
        Self {
            doc_ids,
            next: 0,
            result: RSIndexResult::build_virt().build(),
            mask,
        }
    }
}

impl<'index> RQEIterator<'index> for FieldMaskMock<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.next >= self.doc_ids.len() {
            return Ok(None);
        }
        self.result.doc_id = self.doc_ids[self.next];
        self.result.field_mask = self.mask;
        self.next += 1;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: u64,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        while self.next < self.doc_ids.len() && self.doc_ids[self.next] < doc_id {
            self.next += 1;
        }
        if self.next >= self.doc_ids.len() {
            return Ok(None);
        }
        self.result.doc_id = self.doc_ids[self.next];
        self.result.field_mask = self.mask;
        self.next += 1;
        if self.result.doc_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.next = 0;
    }

    fn num_estimated(&self) -> usize {
        self.doc_ids.len()
    }

    fn last_doc_id(&self) -> u64 {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.next >= self.doc_ids.len()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::Empty
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// Suspended counterpart of [`FieldMaskMock`].
///
/// Layout-identical to [`FieldMaskMock`] (the lifetime parameter is phantom
/// in the test helper — `result` borrows nothing).
pub(crate) struct FieldMaskMockSuspended {
    _doc_ids: Vec<u64>,
    _next: usize,
    _result: RSIndexResult<'static>,
    _mask: inverted_index::FieldMask,
}

impl<'index> rqe_iterators::RQEIteratorBoxed<'index> for FieldMaskMock<'index> {
    type Suspended = FieldMaskMockSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `FieldMaskMock` and `FieldMaskMockSuspended` have identical layout.
        unsafe { Box::from_raw(raw as *mut FieldMaskMockSuspended) }
    }
}

impl rqe_iterators::RQESuspendedIterator for FieldMaskMockSuspended {
    type Resumed<'a> = FieldMaskMock<'a>;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ffi::ValidateStatus) {
        let raw = Box::into_raw(self);
        // SAFETY: layout-identical (see [`FieldMaskMock`]'s suspend).
        let active = unsafe { Box::from_raw(raw as *mut FieldMaskMock<'a>) };
        (active, ffi::ValidateStatus_VALIDATE_OK)
    }

    fn last_doc_id(&self) -> DocId {
        self._result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self._doc_ids.len()
    }
}

use rqe_core::DocId;
use std::collections::BTreeSet;

/// Drain all documents from an iterator and return their doc_ids.
pub(crate) fn drain_doc_ids<'index, I: RQEIterator<'index>>(it: &mut I) -> Vec<DocId> {
    let mut docs = Vec::new();
    while let Some(r) = it.read().unwrap() {
        docs.push(r.doc_id);
    }
    docs
}

/// Create a single [`Mock`] child and return it as a [`BoxedRQEIterator`]
/// together with a handle to its [`MockData`].
pub(crate) fn create_mock_1<const N: usize>(
    doc_ids: [DocId; N],
) -> (BoxedRQEIterator<'static>, MockData) {
    let mock = Mock::new(doc_ids);
    let data = mock.data();
    (BoxedRQEIterator::new(Box::new(mock)), data)
}

/// Create two [`Mock`] children and return them as a `Vec` of
/// [`BoxedRQEIterator`]s together with a two-element array of
/// [`MockData`] handles.
pub(crate) fn create_mock_2<const A: usize, const B: usize>(
    a: [DocId; A],
    b: [DocId; B],
) -> (Vec<BoxedRQEIterator<'static>>, [MockData; 2]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let data = [m1.data(), m2.data()];
    let children: Vec<BoxedRQEIterator<'static>> = vec![
        BoxedRQEIterator::new(Box::new(m1)),
        BoxedRQEIterator::new(Box::new(m2)),
    ];
    (children, data)
}

/// Create three [`Mock`] children and return them as a `Vec` of
/// [`BoxedRQEIterator`]s together with a three-element array of
/// [`MockData`] handles.
pub(crate) fn create_mock_3<const A: usize, const B: usize, const C: usize>(
    a: [DocId; A],
    b: [DocId; B],
    c: [DocId; C],
) -> (Vec<BoxedRQEIterator<'static>>, [MockData; 3]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let m3 = Mock::new(c);
    let data = [m1.data(), m2.data(), m3.data()];
    let children: Vec<BoxedRQEIterator<'static>> = vec![
        BoxedRQEIterator::new(Box::new(m1)),
        BoxedRQEIterator::new(Box::new(m2)),
        BoxedRQEIterator::new(Box::new(m3)),
    ];
    (children, data)
}

/// Create `num_children` [`MockVec`] children whose document-id lists are
/// produced by multiplying each element in `base_result_set` by the child
/// index (1-based).  Returns the children and the sorted, deduplicated
/// expected output.
pub(crate) fn create_union_children(
    num_children: usize,
    base_result_set: &[u64],
) -> (Vec<BoxedRQEIterator<'static>>, Vec<DocId>) {
    let mut expected = BTreeSet::new();
    let children: Vec<BoxedRQEIterator<'static>> = (1..=num_children)
        .map(|i| {
            let doc_ids: Vec<DocId> = base_result_set.iter().map(|&x| x * i as u64).collect();
            expected.extend(doc_ids.iter().copied());
            MockVec::new_boxed(doc_ids)
        })
        .collect();
    (children, expected.into_iter().collect())
}
