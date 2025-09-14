/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Empty iterator implementation
use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, SkipToOutcome};

/// An iterator that yields all ids within a given range.
#[derive(Default)]
pub struct Wildcard {
    top_id: t_docId,
    current_id: t_docId,
    num_docs: usize,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'static>,
}

impl RQEIterator for Wildcard {
    fn read(&mut self) -> Result<Option<&RSIndexResult<'_>>, RQEIteratorError> {
        self.result.doc_id = self.current_id;
        self.current_id += 1;
        Ok(Some(&(self.result)))
    }

    fn skip_to(
        &mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError> {
        Ok(None)
    }

    fn rewind(&mut self) {}

    fn num_estimated(&self) -> usize {
        (self.top_id - self.current_id) as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.top_id
    }

    fn at_eof(&self) -> bool {
        self.current_id < self.top_id
    }
}
