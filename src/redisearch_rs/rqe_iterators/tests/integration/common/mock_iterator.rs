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

// C-Note: sleep/timeout has not been ported for this mock type
// as the Rust iterators do not have any facilities around this ATM.

/// Taken from C++ Tests
///
/// Original: tests/cpptests/iterator_util.h
pub struct MockIterator<'index, const N: usize> {
    result: RSIndexResult<'index>,
    doc_ids: [t_docId; N],
    next_index: usize,
    data: MockData,
}

#[derive(Debug, Clone, Copy)]
#[allow(unused)] // Will be used when optional iterator tests are added
pub enum MockIteratorError {
    TimeoutError,
}

impl MockIteratorError {
    fn as_rqe_iterator_error(self) -> rqe_iterators::RQEIteratorError {
        match self {
            Self::TimeoutError => rqe_iterators::RQEIteratorError::TimedOut,
        }
    }
}

pub struct MockData(Rc<RefCell<MockDataInternal>>);

impl MockData {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(MockDataInternal {
            revalidate_result: MockRevalidateResult::default(),
            validation_count: 0,
            read_count: 0,
            error_at_done: None,
        })))
    }

    pub fn set_revalidate_result(&mut self, result: MockRevalidateResult) -> &mut Self {
        self.0.borrow_mut().revalidate_result = result;
        self
    }

    #[allow(unused)] // Will be used when optional iterator tests are added
    pub fn set_error_at_done(&mut self, maybe_err: Option<MockIteratorError>) -> &mut Self {
        self.0.borrow_mut().error_at_done = maybe_err;
        self
    }

    #[allow(unused)] // Will be used when optional iterator tests are added
    pub fn revalidate_count(&self) -> usize {
        self.0.borrow().validation_count
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MockRevalidateResult {
    #[default]
    Ok,
    Abort,
    Move,
}

impl<'index, const N: usize> MockIterator<'index, N> {
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
