/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Numeric`] and [`Term`].

use std::{f64, ptr::NonNull};

use ffi::{NumericRangeTree, RS_FIELDMASK_ALL, RedisSearchCtx, t_docId};
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReader, IndexReaderCore, NumericReader, RSIndexResult,
    RSOffsetSlice, TermReader, opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;

use crate::expiration_checker::{ExpirationChecker, NoOpChecker};
use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

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
    reader: R,
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

/// An iterator over numeric inverted index entries.
///
/// This iterator can be used to query a numeric inverted index.
///
/// The [`inverted_index::IndexReader`] API can be used to fully scan an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The type of the numeric reader.
/// * `E` - The expiration checker type used to check for expired documents.
pub struct Numeric<'index, R, E = NoOpChecker> {
    it: InvIndIterator<'index, R, E>,
    /// The numeric range tree and its revision ID, used to detect changes during revalidation.
    range_tree_info: Option<RangeTreeInfo>,
    /// Minimum numeric range, only used in debug print.
    range_min: f64,
    /// Maximum numeric range, only used in debug print.
    range_max: f64,
}

/// Information about the numeric range tree backing a [`Numeric`] iterator.
struct RangeTreeInfo {
    /// Pointer to the numeric range tree.
    tree: NonNull<ffi::NumericRangeTree>,
    /// The revision ID at the time the iterator was created.
    /// Used to detect if the tree has been modified.
    revision_id: u32,
}

impl<'index, R, E> Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    /// Create an iterator returning results from a numeric inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`NumericReader`] such as [`inverted_index::FilterNumericReader`]
    /// or [`inverted_index::FilterGeoReader`].
    ///
    /// `expiration_checker` is used to check for expired documents when reading from the inverted index.
    ///
    /// `range_tree` is the underlying range tree backing the iterator.
    /// It is used during revalidation to check if the iterator is still valid.
    ///
    /// `range_min` and `range_max` are the minimum and maximum numeric ranges,
    /// respectively. They are only used in debug print.
    ///
    /// # Safety
    ///
    /// 1. If `range_tree` is Some, it must be a valid pointer to a `NumericRangeTree`.
    /// 2. If `range_tree` is Some, it must stay valid during the iterator's lifetime.
    pub unsafe fn new(
        reader: R,
        expiration_checker: E,
        range_tree: Option<NonNull<NumericRangeTree>>,
        range_min: Option<f64>,
        range_max: Option<f64>,
    ) -> Self {
        let result = RSIndexResult::numeric(0.0);

        let range_tree_info = range_tree.map(|tree| RangeTreeInfo {
            tree,
            // SAFETY: 1.
            revision_id: unsafe { tree.as_ref().revisionId },
        });

        let range_min = range_min.unwrap_or(f64::NEG_INFINITY);
        let range_max = range_max.unwrap_or(f64::INFINITY);
        assert!(range_min <= range_max);

        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            range_tree_info,
            range_min,
            range_max,
        }
    }

    const fn should_abort(&self) -> bool {
        // If there's no range tree, we can't check for changes
        let Some(ref info) = self.range_tree_info else {
            return false;
        };

        // SAFETY: 5. from [`Self::new`]
        let rt = unsafe { info.tree.as_ref() };
        // If the revision id changed the numeric tree was either completely deleted or a node was split or removed.
        // The cursor is invalidated so we cannot revalidate the iterator.
        rt.revisionId != info.revision_id
    }

    pub const fn range_min(&self) -> f64 {
        self.range_min
    }

    pub const fn range_max(&self) -> f64 {
        self.range_max
    }

    /// Get a reference to the underlying reader.
    ///
    /// This is used by FFI code to access the reader.
    pub const fn reader(&self) -> &R {
        &self.it.reader
    }
}

impl<'index, R, E> RQEIterator<'index> for Numeric<'index, R, E>
where
    R: NumericReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate()
    }
}

/// An iterator over term inverted index entries.
///
/// This iterator can be used to query a term inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
/// * `E` - The expiration checker type used to check for expired documents.
pub struct Term<'index, R, E = NoOpChecker> {
    it: InvIndIterator<'index, R, E>,
    #[allow(dead_code)] // will be used by should_abort()
    context: NonNull<RedisSearchCtx>,
}

impl<'index, R, E> Term<'index, R, E>
where
    R: TermReader<'index>,
    E: ExpirationChecker,
{
    /// Create an iterator returning results from a term inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`inverted_index::FilterMaskReader`].
    ///
    /// `term` is the query term that brought up this iterator. It is stored
    /// in the result and persists across all reads.
    ///
    /// `weight` is the scoring weight applied to the result record. It is
    /// typically derived from the query node and used by the scoring function
    /// to scale the relevance of results from this iterator.
    ///
    /// `expiration_checker` is used to check for expired documents when reading from the inverted index.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context` must remain valid for the lifetime of the iterator.
    pub unsafe fn new(
        reader: R,
        context: NonNull<RedisSearchCtx>,
        term: Box<RSQueryTerm>,
        weight: f64,
        expiration_checker: E,
    ) -> Self {
        let result =
            RSIndexResult::with_term(Some(term), RSOffsetSlice::empty(), 0, RS_FIELDMASK_ALL, 1)
                .weight(weight);
        Self {
            it: InvIndIterator::new(reader, result, expiration_checker),
            context,
        }
    }
}

impl<'index, R, E> RQEIterator<'index> for Term<'index, R, E>
where
    R: TermReader<'index>,
    E: ExpirationChecker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.it.revalidate()
    }
}

/// An iterator over all existing documents in an index.
///
/// Used for wildcard queries (`*`), where the goal is to match every document
/// rather than filtering by a specific term or numeric range. The set of
/// existing documents is maintained by the index spec in its `existingDocs`
/// inverted index.
///
/// Unlike [`Term`] and [`Numeric`], this iterator does not support
/// per-field expiration checks — it always uses [`NoOpChecker`].
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
pub struct Wildcard<'index, E: DecodedBy> {
    it: InvIndIterator<'index, IndexReaderCore<'index, E>>,
    context: NonNull<RedisSearchCtx>,
}

impl<'index, E> Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Create an iterator returning all documents from the `existingDocs`
    /// inverted index.
    ///
    /// `weight` is the score weight applied to every returned result.
    ///
    /// # Safety
    ///
    /// 1. `context` must point to a valid [`RedisSearchCtx`].
    /// 2. `context.spec` must be a non-null pointer to a valid `IndexSpec`.
    /// 3. Both 1 and 2 must remain valid for the lifetime of the iterator.
    /// 4. `context.spec.existingDocs`, when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::InvertedIndex) whose encoding
    ///    variant matches `E`.
    pub unsafe fn new(
        reader: IndexReaderCore<'index, E>,
        context: NonNull<RedisSearchCtx>,
        weight: f64,
    ) -> Self {
        use ffi::RS_FIELDMASK_ALL;

        let result = RSIndexResult::virt()
            .weight(weight)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1);

        Self {
            // Wildcard iterator does not support expiration check
            it: InvIndIterator::new(reader, result, NoOpChecker),
            context,
        }
    }

    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may either null out `existingDocs` (after
    /// collecting all documents) or replace it with a new allocation. In
    /// both cases the reader's pointer is stale and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    fn should_abort(&self) -> bool {
        // SAFETY: 1. and 3. guarantee `context` is valid for the iterator's lifetime.
        let sctx_ref = unsafe { self.context.as_ref() };
        // SAFETY: 2. and 3. guarantee `spec` is a valid, non-null pointer for the iterator's lifetime.
        let spec = unsafe { &*sctx_ref.spec };

        let existing_docs = spec
            .existingDocs
            .cast::<inverted_index::opaque::InvertedIndex>();
        if existing_docs.is_null() {
            // the garbage collector may set existing_docs to NULL after garbage collecting all documents
            return true;
        }

        // SAFETY: 4. guarantees `existingDocs` is valid when non-null, and we just checked it's not null.
        let existing_docs = unsafe { &*existing_docs };
        // SAFETY: 4. guarantees the encoding variant matches E.
        let ii = E::from_opaque(existing_docs);

        !self.it.reader.is_index(ii)
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E> RQEIterator<'index> for Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.it.current()
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.it.read()
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.it.skip_to(doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.it.rewind()
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.should_abort() {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate()
    }
}
