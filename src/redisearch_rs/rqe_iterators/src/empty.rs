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

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// An iterator that yields no results.
///
/// The `Empty` iterator is a sentinel iterator that represents an empty result set.
#[derive(Default)]
pub struct Empty;

impl<'index> RQEIterator<'index> for Empty {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        None
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        Ok(None)
    }

    #[inline(always)]
    fn rewind(&mut self) {}

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        0
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        0
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        true
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }
}
