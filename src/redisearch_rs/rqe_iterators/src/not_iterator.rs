/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`NotIterator`].

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, maybe_empty::MaybeEmpty,
};

/// An iterator that negates the results of its child iterator.
///
/// Yields all document IDs from 1 to `max_doc_id` (inclusive) that are **not**
/// present in the child iterator.
pub struct NotIterator<'index, I> {
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID to iterate up to (inclusive).
    max_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,
    // TODO: Timeout
}

impl<'index, I> NotIterator<'index, I>
where
    I: RQEIterator<'index>,
{
    pub const fn new(child: I, max_doc_id: t_docId) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            max_doc_id,
            result: RSIndexResult::virt(),
        }
    }
}

impl<'index, I> RQEIterator<'index> for NotIterator<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // skip all child docs, while not EOF and in sync with child
        while !self.at_eof() {
            self.result.doc_id += 1;

            if self.result.doc_id < self.child.last_doc_id() {
                // no child to NOT results, return our result as-is
                return Ok(Some(&mut self.result));
            }

            if self.child.last_doc_id() == self.result.doc_id {
                // we caught up with child iterator,
                // skip the _real_ result as part of the NOT iterator negation
                continue;
            }

            if let Some(result) = self.child.read()? {
                if result.doc_id > self.result.doc_id {
                    // child skipped ahead already
                    return Ok(Some(&mut self.result));
                }
                debug_assert_eq!(
                    result.doc_id, self.result.doc_id,
                    "child read backwards without rewind"
                );
            } else {
                // child EOF at read
                return Ok(Some(&mut self.result));
            }
        }

        debug_assert!(self.at_eof());
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }

        // Do not skip beyond max_doc_id
        if doc_id > self.max_doc_id {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        // Case 1: Child is ahead or at EOF - docId is not in child
        if self.child.last_doc_id() > doc_id || self.child.at_eof() {
            self.result.doc_id = doc_id;
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            match rc {
                Some(SkipToOutcome::Found(_)) => {
                    // Found value - do not return
                }
                _ => {
                    // Not found - return
                    self.result.doc_id = doc_id;
                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        self.result.doc_id = doc_id;
        match self.read()? {
            Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Get child status
        let child_status = self.child.revalidate()?;
        if matches!(child_status, RQEValidateStatus::Aborted) {
            // Replace aborted child with an empty iterator
            self.child = MaybeEmpty::new_empty();
            // Return here so we can borrow child after mutating the child
            return Ok(RQEValidateStatus::Ok);
        }

        debug_assert!(
            !matches!(child_status, RQEValidateStatus::Moved { .. })
                || self.child.at_eof()
                || self.child.last_doc_id() > self.last_doc_id()
        );
        Ok(RQEValidateStatus::Ok)
    }
}
