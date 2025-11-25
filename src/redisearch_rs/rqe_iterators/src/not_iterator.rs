/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Not iterator implementation

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, maybe_empty::MaybeEmpty};

pub struct NotIterator<'index, I> {
    child: MaybeEmpty<I>,
    max_doc_id: t_docId,
    current_id: t_docId,
    result: RSIndexResult<'index>,
    // Timeout - TBD
}

impl<'index, I> NotIterator<'index, I>
    where
    I: RQEIterator<'index> {

    pub const fn new(child: I, max_doc_id: t_docId) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            // wcii,
            max_doc_id,
            current_id: 0,
            result: RSIndexResult::virt(),
        }
    }
}

impl<'index, I> RQEIterator<'index> for NotIterator<'index, I>
    where
    I: RQEIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        // IdList::at_eof() flips to true as soon as the last child doc is read,
        // so we must still treat that doc as excluded.
        if self.last_doc_id() == self.child.last_doc_id() {
            self.child.read()?;
            // TODO timeout
        }

        while self.current_id < self.max_doc_id {
            self.current_id += 1;

            let child_last = self.child.last_doc_id();
            let child_at_eof = self.child.at_eof();

            // Accept current_id when:
            //  - child.last_doc_id > current_id, or
            //  - child is at EOF and current_id > child.last_doc_id().
            if self.current_id < child_last
                || (child_at_eof && self.current_id > child_last)
            {
                self.result.doc_id = self.current_id;
                return Ok(Some(&mut self.result));
            }

            self.child.read()?;
            // TODO timeout
        }

        Ok(None)
    }

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
            self.current_id = self.max_doc_id;
            return Ok(None);
        }


        // Case 1: Child is ahead or at EOF - docId is not in child
        if self.child.last_doc_id() > doc_id || self.child.at_eof() {
            self.current_id = doc_id;
            self.result.doc_id = doc_id;
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            match rc {
                Some(SkipToOutcome::Found(_)) => {
                    // Found value - do not return
                },
                _ => {
                    // Not found - return
                    self.current_id = doc_id;
                    self.result.doc_id = doc_id;
                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        self.current_id = doc_id;
        let rc = self.read()?;
        match rc {
            Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            None => Ok(None),
        }
    }

    fn rewind(&mut self) {
        self.current_id = 0;
        self.child.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.current_id
    }

    fn at_eof(&self) -> bool {
        self.current_id >= self.max_doc_id
    }

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
