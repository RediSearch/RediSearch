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

use inverted_index::RSIndexResult;
use rqe_iterators::{IteratorType, RQEIterator, RQEIteratorError, SkipToOutcome};

/// A mock iterator that produces results with a specific `t_fieldMask`.
///
/// Each [`read`](RQEIterator::read) yields the next doc_id from the
/// pre-configured list with the fixed `mask` written into
/// `RSIndexResult::field_mask`.
pub(crate) struct FieldMaskMock {
    doc_ids: Vec<u64>,
    next: usize,
    result: RSIndexResult<'static>,
    mask: inverted_index::t_fieldMask,
}

impl FieldMaskMock {
    pub(crate) fn new(doc_ids: Vec<u64>, mask: inverted_index::t_fieldMask) -> Self {
        Self {
            doc_ids,
            next: 0,
            result: RSIndexResult::build_virt().build(),
            mask,
        }
    }
}

impl RQEIterator<'static> for FieldMaskMock {
    fn current(&mut self) -> Option<&mut RSIndexResult<'static>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'static>>, RQEIteratorError> {
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
    ) -> Result<Option<SkipToOutcome<'_, 'static>>, RQEIteratorError> {
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

    unsafe fn revalidate(
        &mut self,
        _spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'static>, RQEIteratorError> {
        Ok(rqe_iterators::RQEValidateStatus::Ok)
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

use ffi::t_docId;
use std::collections::BTreeSet;

/// Drain all documents from an iterator and return their doc_ids.
pub(crate) fn drain_doc_ids<'index, I: RQEIterator<'index>>(it: &mut I) -> Vec<t_docId> {
    let mut docs = Vec::new();
    while let Some(r) = it.read().unwrap() {
        docs.push(r.doc_id);
    }
    docs
}

/// Create a single [`Mock`] child and return it as a boxed trait object
/// together with a handle to its [`MockData`].
pub(crate) fn create_mock_1<const N: usize>(
    doc_ids: [t_docId; N],
) -> (Box<dyn RQEIterator<'static>>, MockData) {
    let mock = Mock::new(doc_ids);
    let data = mock.data();
    (Box::new(mock), data)
}

/// Create two [`Mock`] children and return them as a `Vec` of boxed trait
/// objects together with a two-element array of [`MockData`] handles.
pub(crate) fn create_mock_2<const A: usize, const B: usize>(
    a: [t_docId; A],
    b: [t_docId; B],
) -> (Vec<Box<dyn RQEIterator<'static>>>, [MockData; 2]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let data = [m1.data(), m2.data()];
    let children: Vec<Box<dyn RQEIterator<'static>>> = vec![Box::new(m1), Box::new(m2)];
    (children, data)
}

/// Create three [`Mock`] children and return them as a `Vec` of boxed trait
/// objects together with a three-element array of [`MockData`] handles.
pub(crate) fn create_mock_3<const A: usize, const B: usize, const C: usize>(
    a: [t_docId; A],
    b: [t_docId; B],
    c: [t_docId; C],
) -> (Vec<Box<dyn RQEIterator<'static>>>, [MockData; 3]) {
    let m1 = Mock::new(a);
    let m2 = Mock::new(b);
    let m3 = Mock::new(c);
    let data = [m1.data(), m2.data(), m3.data()];
    let children: Vec<Box<dyn RQEIterator<'static>>> =
        vec![Box::new(m1), Box::new(m2), Box::new(m3)];
    (children, data)
}

/// Create `num_children` [`MockVec`] children whose document-id lists are
/// produced by multiplying each element in `base_result_set` by the child
/// index (1-based).  Returns the children and the sorted, deduplicated
/// expected output.
pub(crate) fn create_union_children(
    num_children: usize,
    base_result_set: &[u64],
) -> (Vec<Box<dyn RQEIterator<'static>>>, Vec<t_docId>) {
    let mut expected = BTreeSet::new();
    let children: Vec<Box<dyn RQEIterator<'static>>> = (1..=num_children)
        .map(|i| {
            let doc_ids: Vec<t_docId> = base_result_set.iter().map(|&x| x * i as u64).collect();
            expected.extend(doc_ids.iter().copied());
            MockVec::new_boxed(doc_ids)
        })
        .collect();
    (children, expected.into_iter().collect())
}
