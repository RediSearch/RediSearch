/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::t_docId;
use inverted_index::{IndexReader, RSIndexResult};

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    expiration_checker::{ExpirationChecker, NoOpChecker},
};

/// A generic iterator over inverted index entries.
///
/// This iterator is used to query an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
/// * `E` - The expiration checker type used to check for expired documents.
pub struct InvIndIterator<'index, R, E = NoOpChecker> {
    /// The reader used to iterate over the inverted index.
    pub(super) reader: R,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,

    /// The expiration checker used to determine if documents are expired.
    expiration_checker: E,

    /// The implementation of the `read` method.
    /// Using dynamic dispatch so we can pick the right version during the
    /// iterator construction saving to re-do the checks each time read() is called.
    read_impl: fn(&mut Self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>,
    /// The implementation of the `skip_to` method.
    skip_to_impl:
        fn(&mut Self, t_docId) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>,
}

impl<'index, R, E> InvIndIterator<'index, R, E>
where
    R: IndexReader<'index>,
    E: ExpirationChecker,
{
    /// Creates a new inverted index iterator with the given expiration checker.
    pub fn new(reader: R, result: RSIndexResult<'static>, expiration_checker: E) -> Self {
        // no need to manually skip duplicates if there is none in the II.
        let skip_multi = reader.has_duplicates();
        // Check if expiration checking is enabled
        let has_expiration = expiration_checker.has_expiration();

        let read_impl = match (skip_multi, has_expiration) {
            (true, true) => Self::read_skip_multi_check_expiration,
            (true, false) => Self::read_skip_multi,
            (false, true) => Self::read_check_expiration,
            (false, false) => Self::read_default,
        };

        let skip_to_impl = if has_expiration {
            Self::skip_to_check_expiration
        } else {
            Self::skip_to_default
        };

        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
            expiration_checker,
            read_impl,
            skip_to_impl,
        }
    }

    /// Default read implementation, without any additional filtering.
    fn read_default(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        if self.reader.next_record(&mut self.result)? {
            self.last_doc_id = self.result.doc_id;
            Ok(Some(&mut self.result))
        } else {
            self.at_eos = true;
            Ok(None)
        }
    }

    /// Read implementation that skips multi-value entries from the same document.
    fn read_skip_multi(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        while self.reader.next_record(&mut self.result)? {
            if self.last_doc_id == self.result.doc_id {
                // Prevent returning the same doc
                continue;
            }

            self.last_doc_id = self.result.doc_id;
            return Ok(Some(&mut self.result));
        }

        // exited the while loop so we reached the end of the index
        self.at_eos = true;
        Ok(None)
    }

    /// Read implementation that skips entries based on field mask expiration.
    fn read_check_expiration(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        while self.reader.next_record(&mut self.result)? {
            if self.is_current_doc_expired() {
                continue;
            }
            self.last_doc_id = self.result.doc_id;
            return Ok(Some(&mut self.result));
        }

        // exited the while loop so we reached the end of the index
        self.at_eos = true;
        Ok(None)
    }

    /// Read implementation that combines skipping multi-value entries and checking field mask expiration.
    fn read_skip_multi_check_expiration(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        while self.reader.next_record(&mut self.result)? {
            if self.last_doc_id == self.result.doc_id {
                // Prevent returning the same doc
                continue;
            }
            if self.is_current_doc_expired() {
                continue;
            }
            self.last_doc_id = self.result.doc_id;
            return Ok(Some(&mut self.result));
        }

        // exited the while loop so we reached the end of the index
        self.at_eos = true;
        Ok(None)
    }

    /// Returns `true` if the current document is expired.
    fn is_current_doc_expired(&self) -> bool {
        self.expiration_checker.is_expired(&self.result)
    }

    // SkipTo implementation that uses a seeker to find the next valid docId, no additional filtering.
    fn skip_to_default(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        if !self.reader.seek_record(doc_id, &mut self.result)? {
            // reached end of iterator
            self.at_eos = true;
            return Ok(None);
        }

        self.last_doc_id = self.result.doc_id;

        if self.result.doc_id == doc_id {
            // found the record
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            // found a record with an id greater than the requested one
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    // SkipTo implementation that uses a seeker and checks for field expiration.
    fn skip_to_check_expiration(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        if !self.reader.seek_record(doc_id, &mut self.result)? {
            // reached end of iterator
            self.at_eos = true;
            return Ok(None);
        }

        if !self.is_current_doc_expired() {
            // The seeker found a doc id that is greater or equal to the requested doc id
            // and the doc id did not expired.
            self.last_doc_id = self.result.doc_id;

            if self.result.doc_id == doc_id {
                // found the record
                return Ok(Some(SkipToOutcome::Found(&mut self.result)));
            } else {
                // found a record with an id greater than the requested one
                return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
            }
        }

        // The seeker found a record but it's expired. Fall back to read to get the next valid record.
        // This matches the C implementation behavior in InvIndIterator_SkipTo_CheckExpiration.
        match self.read()? {
            Some(_) => {
                // Found a valid record, it must be greater than the requested doc_id.
                // It cannot be equal to the requested doc_id because multi-values indices are only
                // possible with JSON indices, which don't have field expiration.
                Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
            }
            None => {
                // No more records
                Ok(None)
            }
        }
    }
}

impl<'index, R, E> RQEIterator<'index> for InvIndIterator<'index, R, E>
where
    R: IndexReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        (self.read_impl)(self)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // cannot be called with an id smaller than the last one returned by the iterator, see
        // [`RQEIterator::skip_to`].
        debug_assert!(self.last_doc_id() < doc_id);
        (self.skip_to_impl)(self, doc_id)
    }

    fn rewind(&mut self) {
        self.at_eos = false;
        self.last_doc_id = 0;
        self.result.doc_id = 0;
        self.reader.reset();
    }

    fn num_estimated(&self) -> usize {
        self.reader.unique_docs() as usize
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eos
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if !self.reader.needs_revalidation() {
            return Ok(RQEValidateStatus::Ok);
        }

        // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
        // anymore. This means that we need to seek the last docId we were at
        let last_doc_id = self.last_doc_id();
        // reset the state of the reader
        self.rewind();

        if last_doc_id == 0 {
            // Cannot skip to 0
            return Ok(RQEValidateStatus::Ok);
        }

        // try restoring the last docId
        let res = match self.skip_to(last_doc_id)? {
            Some(SkipToOutcome::Found(_)) => RQEValidateStatus::Ok,
            Some(SkipToOutcome::NotFound(doc)) => RQEValidateStatus::Moved { current: Some(doc) },
            None => RQEValidateStatus::Moved { current: None },
        };

        Ok(res)
    }
}
