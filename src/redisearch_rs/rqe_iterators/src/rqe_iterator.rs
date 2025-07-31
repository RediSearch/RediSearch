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

/// The outcome of [`RQEIterator::skip_to`].
pub enum SkipToOutcome {
    /// The iterator has a valid entry for the requested `doc_id`.
    Found(RSIndexResult),

    /// The iterator doesn't have an entry for the requested `doc_id`, but there are entries with an id greater than the requested one.
    NotFound(RSIndexResult),
}

/// An iterator failure indications
pub enum RQEIteratorError {
    /// The iterator has reached the time limit for execution.
    TimedOut,
}

/// The status of the iterator after a call to `revalidate`
pub enum RQEValidateStatus {
    /// The iterator is still valid and at the same position.
    Ok,
    /// The iterator is still valid but lastDocID changed, and `current` is a new valid result or at EOF. If not at EOF, the `current` result should be used before the next read, or it will be overwritten.
    Moved,
    /// The iterator is no longer valid, and should not be used or rewound. Should be freed.
    Aborted,
}

pub trait RQEIterator {
    /// Read the next entry from the iterator.
    ///
    /// On a successful read, the iterator must:
    /// 1. Set its `lastDocId` member to the new current result id
    /// 2. Set its `current` index result property to its current result, for the caller to access if desired
    /// This function returns Ok with the current result or Err(RQEIteratorError) for any error.
    fn read(&mut self) -> Result<Option<RSIndexResult>, RQEIteratorError>;

    /// Skip to the next record in the iterator with an ID greater or equal to the given `docId`.
    ///
    /// It is assumed that when `skip_to` is called, `self.lastDocId() < docId`.
    ///
    /// On a successful read, the iterator must:
    /// 1. Set its `lastDocId` member to the new current result id
    /// 2. Set its `current` index result property to its current result, for the caller to access if desired
    ///
    /// Return `Ok(RQEIteratorStatus::OK)` if the iterator has found a record with the `docId` and `Ok(RQEIteratorStatus::NotFound)`
    /// if the iterator found a result greater than `docId`. 'None" will be returned if the iterator has reached the end of the index.
    fn skip_to(&mut self, doc_id: t_docId) -> Result<Option<RQEIteratorStatus>, RQEIteratorError>;

    /// Called when the iterator is being revalidated after a concurrent index change.
    ///
    /// The iterator should check if it is still valid.
    /// Return Ok if the iterator is still valid, Moved if the iterator is still valid, but the lastDocId has changed (moved forward) or Aborted if the iterator is no longer valid

    fn revalidate(&mut self) -> RQEValidateStatus;

    ///Rewind the iterator to the beginning and reset its properties.
    fn rewind(&mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&self) -> t_docId;

    /// Returns true if the iterator has more results to read, meaning, not at EOF.
    fn has_next(&self) -> bool;
}
