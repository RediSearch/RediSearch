/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSIndexResult

/**
 * An iterator has two successful states: `OK` and `NotFound`.
 * `OK` indicates that the iterator has found a valid result, in either "read" or "skip_to",
 * `NotFound` indicates that a "skip_to(doc_id)" operation has found a result with a doc_id greater than the requested `doc_id`.
 */
pub enum RQEIteratorStatus {
    OK,
    NotFound,
}

pub enum RQEIteratorError {
    EOF,
    TimedOut,
}

pub enum RQEValidateStatus {
    Moved,  // The iterator is still valid but lastDocID changed, and `current` is a new valid result or at EOF. If not at EOF, the `current` result should be used before the next read, or it will be overwritten.
    Aborted,    // The iterator is no longer valid, and should not be used or rewound. Should be freed.
}

pub trait RQEIterator {

    /**
     * Read the next entry from the iterator.
     *  On a successful read, the iterator must:
     *  1. Set its `lastDocId` member to the new current result id
     *  2. Set its `current` index result property to its current result, for the caller to access if desired
     *  @returns Ok(RQEIteratorStatus::OK)
     *  @returns Err(RQEIteratorError) for any error.
     */
    fn read(&mut self) -> Result<RQEIteratorStatus, RQEIteratorError>;

    /**
     * Skip to the next ID of the iterator, which is greater or equal to `docId`.
     *  It is assumed that when `skip_to` is called, `self.lastDocId() < docId`.
     *  On a successful read, the iterator must:
     *  1. Set its `lastDocId` member to the new current result id
     *  2. Set its `current` index result property to its current result, for the caller to access if desired
     *  @returns Ok(RQEIteratorStatus::OK) if the iterator has found `docId`.
     *  @returns Ok(RQEIteratorStatus::NotFound) if the iterator has only found a result greater than `docId`.
     *  @returns Err(RQEIteratorError) for any error.
     */
    fn skip_to(&mut self, doc_id: u64) -> Result<RQEIteratorStatus, RQEIteratorError>;

    /**
     * Called when the iterator is being revalidated after a concurrent index change.
     * The iterator should check if it is still valid.
     *
     * @return Ok(()) if the iterator is still valid
     * @return Err(RQEValidateStatus::Moved) if the iterator is still valid, but the lastDocId has changed (moved forward)
     * @return Err(RQEValidateStatus::Aborted) if the iterator is no longer valid
     */ 

    fn revalidate(&mut self) -> Result<(), RQEValidateStatus>;

    /**
     * Rewind the iterator to the beginning and reset its properties.
     */
    fn rewind(&mut self);


    /**************** properties ****************/

    /**
     * Returns a reference to the current index result.
     */
    fn current(&self) -> &RSIndexResult;

    /**
     * Returns the last doc id that was read or skipped to.
     */
    fn last_doc_id(&self) -> u64;

    /**
     * Returns true if the iterator has more results to read, meaning, not at EOF.
     */
    fn has_next(&self) -> bool;
   
}