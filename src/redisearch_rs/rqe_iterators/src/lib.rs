/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod empty;

use ffi::t_docId;
use inverted_index::RSIndexResult;

/// The outcome of [`RQEIterator::skip_to`].
pub enum SkipToOutcome<'index, 'aggregate_children> {
    /// The iterator has a valid entry for the requested `doc_id`.
    Found(RSIndexResult<'index, 'aggregate_children>),

    /// The iterator doesn't have an entry for the requested `doc_id`, but there are entries with an id greater than the requested one.
    NotFound(RSIndexResult<'index, 'aggregate_children>),
}

/// An iterator failure indications
pub enum RQEIteratorError {
    /// The iterator has reached the time limit for execution.
    TimedOut,
}

#[derive(Debug, PartialEq, Eq)]
/// The status of the iterator after a call to `revalidate`
pub enum RQEValidateStatus {
    /// The iterator is still valid and at the same position.
    Ok,
    /// The iterator is still valid but its internal state has changed.
    Moved,
    /// The iterator is no longer valid, and should not be used or rewound. Should be dropped.
    Aborted,
}

pub trait RQEIterator {
    /// Read the next entry from the iterator.
    ///
    /// On a successful read, the iterator must set its `last_doc_id` property to the new current result id
    /// This function returns Ok with the current result for valid results, or None if the iterator is depleted.
    /// The function will return Err(RQEIteratorError) for any error.
    fn read(&mut self) -> Result<Option<RSIndexResult<'_, '_>>, RQEIteratorError>;

    /// Skip to the next record in the iterator with an ID greater or equal to the given `docId`.
    ///
    /// It is assumed that when `skip_to` is called, `self.lastDocId() < docId`.
    ///
    /// On a successful read, the iterator must set its `last_doc_id` property to the new current result id
    ///
    /// Return `Ok(SkipToOutcome::Found)` if the iterator has found a record with the `docId` and `Ok(SkipToOutcome::NotFound)`
    /// if the iterator found a result greater than `docId`. 'None" will be returned if the iterator has reached the end of the index.
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, '_>>, RQEIteratorError>;

    /// Called when the iterator is being revalidated after a concurrent index change.
    ///
    /// The iterator should check if it is still valid.
    fn revalidate(&mut self) -> RQEValidateStatus {
        // Default implementation does nothing.
        RQEValidateStatus::Ok
    }

    ///Rewind the iterator to the beginning and reset its properties.
    fn rewind(&mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&self) -> t_docId;

    /// Returns `false` if the iterator can yield more results.
    /// The iterator implementation must ensure that `at_eof` returns `false` when it is sure that the [`RQEIterator::read`] returns `Ok(None)`.
    fn at_eof(&self) -> bool;
}
