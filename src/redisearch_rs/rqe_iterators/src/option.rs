/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

// NOTE: `None` variant is expected to be equal to the Empty implementation,
// but repeated here due not being able to use Empty as a temporary
// value in half of the locations below.

impl<'index, I> RQEIterator<'index> for Option<I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match self {
            Some(it) => it.read(),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match self {
            Some(it) => it.skip_to(doc_id),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self {
            Some(it) => it.revalidate(),
            None => Ok(RQEValidateStatus::Ok),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        if let Some(it) = self {
            it.rewind();
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match self {
            Some(it) => it.num_estimated(),
            None => 0,
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        match self {
            Some(it) => it.last_doc_id(),
            None => 0,
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match self {
            Some(it) => it.at_eof(),
            None => true,
        }
    }
}
