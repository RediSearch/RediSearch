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

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome};

/// An iterator that yields all ids within a given range, from 1 to max id (inclusive) in an index.
#[derive(Default)]
pub struct Wildcard<'a> {
    // Supposed to be the max id in the index
    top_id: t_docId,

    current_id: t_docId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'a>,
}

impl<'a> Wildcard<'a> {
    pub fn new(top_id: t_docId) -> Self {
        Wildcard {
            top_id,
            current_id: 0,
            result: RSIndexResult::virt().frequency(1),
        }
    }
}

impl<'a> RQEIterator for Wildcard<'a> {
    fn read(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        self.current_id += 1;
        self.result.doc_id = self.current_id;
        Ok(Some(&self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);

        if doc_id > self.top_id {
            // skip beyond range - set to EOF
            self.current_id = self.top_id;
            return Ok(None);
        }

        self.current_id = doc_id;
        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&self.result)))
    }

    fn rewind(&mut self) {
        self.current_id = 0;
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.current_id
    }

    fn at_eof(&self) -> bool {
        self.current_id >= self.top_id
    }
}
