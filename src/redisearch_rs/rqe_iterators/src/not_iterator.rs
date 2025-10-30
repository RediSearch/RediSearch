/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Not iterator implementation

use ffi::{IteratorStatus, RedisModule_RegisterEnumConfig, t_docId};
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, wildcard::Wildcard, empty::Empty};

// typedef struct {
//   QueryIterator base;         // base index iterator
//   QueryIterator *wcii;        // wildcard index iterator
//   QueryIterator *child;       // child index iterator
//   t_docId maxDocId;
//   TimeoutCtx timeoutCtx;
// } NotIterator;

// Base - not needed
// WCII - Wildcard
// Child - Box<dyn RQEIterator>

pub struct NotIterator<'index, I> {
    child: I,
    // wcii: Option<Box<Wildcard<'index>>>,
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
            child,
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
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {

        if self.at_eof() {
            return Ok(None);
        }

        if self.last_doc_id() == self.child.last_doc_id() {
            self.child.read()?;
            // TODO timeout
        }

        /*
            base->lastDocId++;
        if (base->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
        ni->timeoutCtx.counter = 0;
        base->current->docId = base->lastDocId;
        return ITERATOR_OK;
        }
         */
        while self.current_id < self.max_doc_id {
            self.current_id += 1;
            if (self.current_id < self.child.last_doc_id() || self.child.at_eof()) {
                self.result.doc_id = self.current_id;
                return Ok(Some(&mut self.result));
            }
            self.child.read()?;
            // TODO timeout
        }
        // Comment EOF
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


        // Case 1: Child is ahead or at EOF - docId is not in child
        if self.child.last_doc_id() > doc_id || self.child.at_eof() {
            self.current_id = doc_id;
            self.result.doc_id = doc_id;
            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            // ?
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
        let rc: Option<&mut RSIndexResult<'index>> = self.read()?;
        // ?
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
            // Replace aborted child with an empty iterator (drop happens automatically)
            // self.child = Empty::default()
        }
        //TODO - debug assert
        // RS_LOG_ASSERT(child_status != VALIDATE_MOVED || ni->child->atEOF || ni->child->lastDocId > base->lastDocId, "Moved but still not beyond lastDocId");

        Ok(RQEValidateStatus::Ok)
    }
}
