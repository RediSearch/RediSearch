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

/// An iterator that yields no results.
///
/// The `Empty` iterator is a sentinel iterator that represents an empty result set.
#[derive(Default)]
pub struct Empty;

impl<'iterator, 'index> RQEIterator<'iterator, 'index> for Empty {
    fn read(
        &'iterator mut self,
    ) -> Result<Option<&'iterator mut RSIndexResult<'index>>, RQEIteratorError> {
        Ok(None)
    }

    fn skip_to(
        &'iterator mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'iterator, 'index>>, RQEIteratorError> {
        Ok(None)
    }

    fn rewind(&mut self) {}

    fn num_estimated(&self) -> usize {
        0
    }

    fn last_doc_id(&self) -> t_docId {
        0
    }

    fn at_eof(&self) -> bool {
        true
    }
}
