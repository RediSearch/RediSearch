/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod empty;
pub mod id_list;
pub mod wildcard;

use ffi::t_docId;
use inverted_index::RSIndexResult;

#[derive(Debug)]
/// The outcome of [`RQEIterator::skip_to`].
pub enum SkipToOutcome<'iterator, 'index> {
    /// The iterator has a valid entry for the requested `doc_id`.
    Found(&'iterator mut RSIndexResult<'index>),

    /// The iterator doesn't have an entry for the requested `doc_id`, but there are entries with an id greater than the requested one.
    NotFound(&'iterator mut RSIndexResult<'index>),
}

#[derive(Debug)]
/// An iterator failure indications
pub enum RQEIteratorError {
    /// The iterator has reached the time limit for execution.
    TimedOut,
}

#[derive(Debug, PartialEq)]
/// The status of the iterator after a call to `revalidate`
pub enum RQEValidateStatus<'iterator, 'index> {
    /// The iterator is still valid and at the same position.
    Ok,
    /// The iterator is still valid but its internal state has changed.
    Moved {
        /// The new current current document the iterator is at, or `None` if the iterator is at EOF.
        current: Option<&'iterator mut RSIndexResult<'index>>,
    },
    /// The iterator is no longer valid, and should not be used or rewound. Should be dropped.
    Aborted,
}

pub trait RQEIterator<'iterator, 'index> {
    /// Read the next entry from the iterator.
    ///
    /// On a successful read, the iterator must set its `last_doc_id` property to the new current result id
    /// This function returns Ok with the current result for valid results, or None if the iterator is depleted.
    /// The function will return Err(RQEIteratorError) for any error.
    fn read(
        &'iterator mut self,
    ) -> Result<Option<&'iterator mut RSIndexResult<'index>>, RQEIteratorError>;

    /// Skip to the next record in the iterator with an ID greater or equal to the given `docId`.
    ///
    /// It is assumed that when `skip_to` is called, `self.lastDocId() < docId`.
    ///
    /// On a successful read, the iterator must set its `last_doc_id` property to the new current result id
    ///
    /// Return `Ok(SkipToOutcome::Found)` if the iterator has found a record with the `docId` and `Ok(SkipToOutcome::NotFound)`
    /// if the iterator found a result greater than `docId`. 'None" will be returned if the iterator has reached the end of the index.
    fn skip_to(
        &'iterator mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'iterator, 'index>>, RQEIteratorError>;

    /// Called when the iterator is being revalidated after a concurrent index change.
    ///
    /// The iterator should check if it is still valid.
    fn revalidate(
        &'iterator mut self,
    ) -> Result<RQEValidateStatus<'iterator, 'index>, RQEIteratorError> {
        // Default implementation does nothing.
        Ok(RQEValidateStatus::Ok)
    }

    ///Rewind the iterator to the beginning and reset its properties.
    fn rewind(&'iterator mut self);

    /// Returns an upper-bound estimation for the number of results the iterator is going to yield.
    fn num_estimated(&'iterator self) -> usize;

    /**************** properties ****************/

    /// Returns the last doc id that was read or skipped to.
    fn last_doc_id(&'iterator self) -> t_docId;

    /// Returns `false` if the iterator can yield more results.
    /// The iterator implementation must ensure that `at_eof` returns `false` when it is sure that the [`RQEIterator::read`] returns `Ok(None)`.
    fn at_eof(&'iterator self) -> bool;
}
