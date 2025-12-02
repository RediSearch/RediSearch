/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A thin wrapper to override revalidate() behavior for testing.

use std::cell::Cell;
use std::ops::{Deref, DerefMut};

use ffi::t_docId;
use inverted_index::RSIndexResult;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, id_list::SortedIdList};

/// What revalidate() should return
#[derive(Clone, Copy, Debug, Default)]
pub enum MockRevalidateResult {
    #[default]
    Ok,
    Moved,
    MovedToEof,
    Aborted,
}

/// Wraps SortedIdList to override only revalidate().
/// Uses Deref so all other methods go directly to inner.
pub struct MockIterator<'index>(pub SortedIdList<'index>, pub Cell<MockRevalidateResult>);

impl<'index> MockIterator<'index> {
    pub fn new(doc_ids: Vec<t_docId>) -> Self {
        Self(SortedIdList::new(doc_ids), Cell::new(MockRevalidateResult::Ok))
    }

    pub fn set_revalidate_result(&self, result: MockRevalidateResult) {
        self.1.set(result);
    }
}

// Deref to SortedIdList for convenience (not used by trait, but nice to have)
impl<'index> Deref for MockIterator<'index> {
    type Target = SortedIdList<'index>;
    fn deref(&self) -> &Self::Target { &self.0 }
}
impl<'index> DerefMut for MockIterator<'index> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

// Implement trait - delegate everything except revalidate
impl<'index> RQEIterator<'index> for MockIterator<'index> {
    #[inline] fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> { self.0.read() }
    #[inline] fn skip_to(&mut self, id: t_docId) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> { self.0.skip_to(id) }
    #[inline] fn rewind(&mut self) { self.0.rewind() }
    #[inline] fn current(&mut self) -> Option<&mut RSIndexResult<'index>> { self.0.current() }
    #[inline] fn last_doc_id(&self) -> t_docId { self.0.last_doc_id() }
    #[inline] fn at_eof(&self) -> bool { self.0.at_eof() }
    #[inline] fn num_estimated(&self) -> usize { self.0.num_estimated() }

    // The only method we actually override
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self.1.get() {
            MockRevalidateResult::Ok => Ok(RQEValidateStatus::Ok),
            MockRevalidateResult::Moved => {
                match self.0.read()? {
                    Some(r) => Ok(RQEValidateStatus::Moved { current: Some(r) }),
                    None => Ok(RQEValidateStatus::Moved { current: None }),
                }
            }
            MockRevalidateResult::MovedToEof => {
                while self.0.read()?.is_some() {}
                Ok(RQEValidateStatus::Moved { current: None })
            }
            MockRevalidateResult::Aborted => Ok(RQEValidateStatus::Aborted),
        }
    }
}

