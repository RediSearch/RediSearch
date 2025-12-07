/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{cell::RefCell, rc::Rc};

use ffi::{RS_FIELDMASK_ALL, t_docId};
use inverted_index::RSIndexResult;
use rqe_iterators::RQEIterator;

/// Test iterator used in unit tests that expect an [`RQEIterator`]
/// child which produces a fixed sequence of document identifiers.
///
/// `MockIterator` simulates a very small posting list:
///
/// * It owns a fixed array of document ids that must be sorted in
///   increasing order.
/// * Calls to [`RQEIterator::read`] walk that array from left to right
///   and copy the current id into a reusable [`RSIndexResult`] that is
///   stored inside the iterator.
/// * Calls to [`RQEIterator::skip_to`] advance `next_index` until it
///   reaches the requested id or the first id that is greater than it.
///
/// The iterator is intentionally simple and deterministic so that
/// higher level iterators can be tested without depending on the
/// actual inverted index implementation.  For example it is used as
/// the child iterator in the `Optional` iterator tests to verify
/// interaction between real and virtual results, end of input handling
/// and validation.
///
/// The iterator also owns a [`MockData`] value that is stored inside a
/// reference counted cell.  Test code can obtain a handle to this
/// state through [`MockIterator::data`] in order to:
///
/// * Inspect how many times [`RQEIterator::revalidate`] was called.
/// * Inspect how many times [`RQEIterator::read`] was called.
/// * Configure what [`RQEIterator::revalidate`] will return through
///   [`MockData::set_revalidate_result`].
/// * Configure an error that will be returned once the iterator
///   reaches the end of the document ids through
///   [`MockData::set_error_at_done`].
///
/// Taken from the C++tests in
/// `tests/cpptests/iterator_util.h`.
pub struct MockIterator<'index, const N: usize> {
    result: RSIndexResult<'index>,
    doc_ids: [t_docId; N],
    next_index: usize,
    data: MockData,
}

/// Error that can be injected into a [`MockIterator`] from tests.
///
/// This type is intentionally small and is translated into the
/// public [`rqe_iterators::RQEIteratorError`] type through
/// [`MockIteratorError::as_rqe_iterator_error`].
///
/// This allows tests to
/// express expectations in terms of the real error type while still
/// using a simple local enum to control behaviour.
#[derive(Debug, Clone, Copy)]
pub(crate) enum MockIteratorError {
    /// Simulate a timeout in the child iterator.
    TimeoutError,
}

impl MockIteratorError {
    /// Convert this mock error into the public [`rqe_iterators::RQEIteratorError`] type.
    ///
    /// This helper keeps the mock specific error type private to the
    /// test utilities while still surfacing the correct error value to
    /// production code and test assertions.
    fn as_rqe_iterator_error(self) -> rqe_iterators::RQEIteratorError {
        match self {
            Self::TimeoutError => rqe_iterators::RQEIteratorError::TimedOut,
        }
    }
}

/// Shared mutable test state that belongs to a [`MockIterator`].
///
/// The value is reference counted so that:
///
/// * The iterator can own it.
/// * Tests can keep their own handle obtained through
///   [`MockIterator::data`] and observe or mutate the state while the
///   iterator is in use.
///
/// Most tests treat `MockData` as a light weight handle that can be
/// cloned cheaply and passed around by value.
pub struct MockData(Rc<RefCell<MockDataInternal>>);

impl MockData {
    /// Create a new [`MockData`] instance with default behaviour.
    ///
    /// The initial state is:
    ///
    /// * `revalidate_result` set to [`MockRevalidateResult::Ok`].
    /// * `validation_count` equal to zero.
    /// * `read_count` equal to zero.
    /// * `error_at_done` set to `None` which means no error will be
    ///   raised at end of input.
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MockDataInternal {
            revalidate_result: MockRevalidateResult::default(),
            validation_count: 0,
            read_count: 0,
            error_at_done: None,
        })))
    }

    /// Configure the result that [`MockIterator::revalidate`] will report.
    ///
    /// This value is read on every call to `revalidate` of the owning
    /// iterator.  Tests can update it at any time to change how the
    /// iterator reacts to validation requests.
    pub fn set_revalidate_result(&mut self, result: MockRevalidateResult) -> &mut Self {
        self.0.borrow_mut().revalidate_result = result;
        self
    }

    /// Configure the error that is returned once the iterator reaches
    /// end of input.
    ///
    /// If `maybe_err` is `Some`, the next call to [`RQEIterator::read`]
    /// or [`RQEIterator::skip_to`] that reaches end of the document id
    /// array will immediately return that error instead of `Ok(None)`.
    ///
    /// If `maybe_err` is `None`, end of input is reported as `Ok(None)`.
    pub fn set_error_at_done(&mut self, maybe_err: Option<MockIteratorError>) -> &mut Self {
        self.0.borrow_mut().error_at_done = maybe_err;
        self
    }

    /// Number of times [`MockIterator::revalidate`] was called.
    ///
    /// This counter is incremented whenever the owning iterator calls
    /// `revalidate` on its child.  Tests use this to assert that a
    /// particular code path triggers the expected number of validation
    /// attempts.
    pub fn revalidate_count(&self) -> usize {
        self.0.borrow().validation_count
    }

    /// Number of times [`MockIterator::read`] was called.
    ///
    /// This counter is incremented whenever the owning iterator calls
    /// `read` or performs a `skip_to` that internally delegates to
    /// `read`.  It is useful when tests need to verify how often a
    /// child iterator was advanced.
    #[expect(
        unused,
        reason = "code will be required later, as we advance in porting Redis C/C++ code"
    )]
    pub fn read_count(&self) -> usize {
        self.0.borrow().read_count
    }
}

struct MockDataInternal {
    revalidate_result: MockRevalidateResult,
    validation_count: usize,
    read_count: usize,
    error_at_done: Option<MockIteratorError>,
}

/// Result configured through [`MockData`] that controls what
/// [`MockIterator::revalidate`] reports.
///
/// The enum mirrors the conceptual outcomes of validation that are
/// relevant for the higher level iterators under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MockRevalidateResult {
    #[default]
    Ok,
    Abort,
    Move,
}

impl<'index, const N: usize> MockIterator<'index, N> {
    /// Create a new [`MockIterator`] over a fixed array of document ids.
    ///
    /// The ids in `doc_ids` must be sorted in increasing order because
    /// the iterator assumes monotonic forward progress when serving
    /// `read` and `skip_to` calls.
    ///
    /// The internal [`RSIndexResult`] is created as a virtual result
    /// with weight equal to `1.0` and field mask set to
    /// `RS_FIELDMASK_ALL`.  Each call to `read` or `skip_to` overwrites
    /// `doc_id` in that single result instance.
    pub fn new(doc_ids: [t_docId; N]) -> Self {
        Self {
            result: RSIndexResult::virt()
                .weight(1.)
                .field_mask(RS_FIELDMASK_ALL),
            doc_ids,
            next_index: 0,
            data: MockData::new(),
        }
    }

    /// Return a handle to the shared [`MockData`] of this iterator.
    ///
    /// The returned value clones the underlying `Rc` so it is cheap to
    /// copy and can outlive any particular borrow of the iterator.
    /// Mutations performed through this handle are immediately visible
    /// to the iterator and to other handles that were cloned from it.
    pub fn data(&self) -> MockData {
        MockData(self.data.0.clone())
    }
}

impl<'index, const N: usize> RQEIterator<'index> for MockIterator<'index, N> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        let mut data = self.data.0.borrow_mut();

        data.read_count += 1;
        if self.at_eof() {
            return if let Some(err) = data.error_at_done {
                Err(err.as_rqe_iterator_error())
            } else {
                Ok(None)
            };
        }

        self.result.doc_id = self.doc_ids[self.next_index];
        self.next_index += 1;

        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        let mut data = self.data.0.borrow_mut();

        data.read_count += 1;

        assert!(
            self.result.doc_id < doc_id,
            "skipTo: requested to skip backwards",
        );

        if self.at_eof() {
            return if let Some(err) = data.error_at_done {
                Err(err.as_rqe_iterator_error())
            } else {
                Ok(None)
            };
        }

        while self.next_index < N && self.doc_ids[self.next_index] < doc_id {
            self.next_index += 1;
        }

        data.read_count -= 1; // Decrement the read count before calling Read
        drop(data);

        Ok(self.read()?.map(|result| {
            if result.doc_id == doc_id {
                rqe_iterators::SkipToOutcome::Found(result)
            } else {
                rqe_iterators::SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        let mut data = self.data.0.borrow_mut();

        data.validation_count += 1;

        Ok(match data.revalidate_result {
            MockRevalidateResult::Ok => rqe_iterators::RQEValidateStatus::Ok,
            MockRevalidateResult::Abort => rqe_iterators::RQEValidateStatus::Aborted,
            MockRevalidateResult::Move => {
                rqe_iterators::RQEValidateStatus::Moved {
                    current: (self.next_index < N).then(|| {
                        // Simulate a move by incrementing nextIndex
                        self.result.doc_id = self.doc_ids[self.next_index];
                        self.next_index += 1;
                        &mut self.result
                    }),
                }
            }
        })
    }

    fn rewind(&mut self) {
        self.next_index = 0;
        self.result.doc_id = 0;

        let mut data = self.data.0.borrow_mut();
        data.read_count = 0;
    }

    fn num_estimated(&self) -> usize {
        N
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.next_index >= N
    }
}
