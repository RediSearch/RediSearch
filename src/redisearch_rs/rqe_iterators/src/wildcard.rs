/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard iterator implementation

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
#[derive(Default)]
pub struct Wildcard<'index> {
    // Supposed to be the max id in the index
    top_id: t_docId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
}

impl Wildcard<'_> {
    pub const fn new(top_id: t_docId) -> Self {
        Wildcard {
            top_id,
            result: RSIndexResult::virt().frequency(1),
        }
    }
}

impl<'index> RQEIterator<'index> for Wildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        self.result.doc_id += 1;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);

        if doc_id > self.top_id {
            // skip beyond range - set to EOF
            self.result.doc_id = self.top_id;
            return Ok(None);
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.top_id
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }
}
