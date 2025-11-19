/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Numeric`] and [`Term`].

use std::ptr::NonNull;

use ffi::{IndexSpec, RS_INVALID_FIELD_INDEX, RedisSearchCtx, t_docId};
use field::{FieldFilterContext, FieldMaskOrIndex};
use inverted_index::{IndexReader, NumericReader, RSIndexResult, TermReader};

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// A generic iterator over inverted index entries.
///
/// This iterator is used to query an inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
/// * `R` - The reader type used to read the inverted index.
pub struct InvIndIterator<'index, R> {
    /// The reader used to iterate over the inverted index.
    reader: R,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: t_docId,
    /// A reusable result object to avoid allocations on each `read` call.
    result: RSIndexResult<'index>,

    /// The search context used to revalidate the iterator and to check for expiration.
    sctx: Option<NonNull<RedisSearchCtx>>,
    /// The context for the field/s filter, used to determine if the field/s is/are expired.
    filter_ctx: FieldFilterContext,

    /// The implementation of the `read` method.
    /// Using dynamic dispatch so we can pick the right version during the
    /// iterator construction saving to re-do the checks each time read() is called.
    read_impl: fn(&mut Self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError>,
    /// The implementation of the `skip_to` method.
    skip_to_impl:
        fn(&mut Self, t_docId) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError>,
}

/// Returns `true` if the iterator should check for expired record when reading from the inverted index.
fn has_expiration(spec: &IndexSpec, mask_or_index: FieldMaskOrIndex) -> bool {
    if spec.docs.ttl.is_null() {
        return false;
    }

    if !spec.monitorDocumentExpiration {
        return false;
    }

    // check if the specific field/fieldMask has expiration
    match mask_or_index {
        FieldMaskOrIndex::Mask(_mask) => true,
        FieldMaskOrIndex::Index(index) if index != RS_INVALID_FIELD_INDEX => true,
        _ => false,
    }

    // TODO: better estimation
}

impl<'index, R> InvIndIterator<'index, R>
where
    R: IndexReader<'index>,
{
    /// # Safety
    ///
    /// 1. `context` is either None or a valid pointer to a `RedisSearchCtx`.
    /// 2. if `context` is Some then `context.spec` is a valid pointer to an `IndexSpec`.
    /// 3. 1 and 2 must stay valid during the iterator's lifetime.
    pub fn new(
        reader: R,
        result: RSIndexResult<'static>,
        filter_ctx: FieldFilterContext,
        context: Option<NonNull<RedisSearchCtx>>,
    ) -> Self {
        #[cfg(debug_assertions)]
        if let Some(context) = &context {
            debug_assert!(context.is_aligned());
            // SAFETY: Guaranteed by 1.
            let context = unsafe { context.as_ref() };
            debug_assert!(!context.spec.is_null());
            debug_assert!(context.spec.is_aligned());
        }

        // no need to manually skip duplicates if there is none in the II.
        let skip_multi = reader.has_duplicates();
        let has_expiration = context
            .as_ref()
            .map(|sctx| {
                // SAFETY: Guaranteed by 1.
                let sctx = unsafe { sctx.as_ref() };
                // SAFETY: Guaranteed by 2.
                let spec = unsafe { sctx.spec.as_ref().expect("sctx.spec cannot be NULL") };
                has_expiration(spec, filter_ctx.field)
            })
            .unwrap_or_default();

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
            filter_ctx,
            sctx: context,
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

    /// Returns `false` if the current document is expired.
    fn check_current_expiration(&self) -> bool {
        let Some(sctx) = self.sctx.as_ref() else {
            // no search context so record cannot expire
            return true;
        };
        // SAFETY: 1 + 3
        let sctx = unsafe { sctx.as_ref() };
        // SAFETY: 2 + 3
        let spec = unsafe { *(sctx.spec) };

        if spec.docs.ttl.is_null() {
            // no TTL so record cannot expire
            return true;
        };

        let current_time = &sctx.time.current as *const _;

        match self.filter_ctx.field {
            FieldMaskOrIndex::Index(index) => unsafe {
                ffi::TimeToLiveTable_VerifyDocAndField(
                    spec.docs.ttl,
                    self.result.doc_id,
                    index,
                    self.filter_ctx.predicate.raw(),
                    current_time,
                )
            },
            FieldMaskOrIndex::Mask(_) => todo!(),
        }
    }

    /// Read implementation that skips entries based on field mask expiration.
    fn read_check_expiration(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eos {
            return Ok(None);
        }

        while self.reader.next_record(&mut self.result)? {
            if !self.check_current_expiration() {
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
        todo!()
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

        while self.reader.seek_record(doc_id, &mut self.result)? {
            if !self.check_current_expiration() {
                continue;
            }

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

        // exited the while loop so we reached the end of the index
        self.at_eos = true;
        Ok(None)
    }
}

impl<'index, R> RQEIterator<'index> for InvIndIterator<'index, R>
where
    R: IndexReader<'index>,
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
        // TODO: NumericCheckAbort when implementing queries

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
pub struct Numeric<'index, R> {
    it: InvIndIterator<'index, R>,
}

impl<'index, R> Numeric<'index, R>
where
    R: NumericReader<'index>,
{
    /// Create an iterator returning results from a numeric inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`NumericReader`] such as [`inverted_index::FilterNumericReader`]
    /// or [`inverted_index::FilterGeoReader`].
    pub fn new(reader: R, filter_ctx: FieldFilterContext) -> Self {
        let result = RSIndexResult::numeric(0.0);
        Self {
            it: InvIndIterator::new(reader, result, filter_ctx, None),
        }
    }

    /// Variant of [`Self::new`] that allows passing a search context.
    /// The search context is used to check for expired documents when reading
    /// from the inverted index.
    ///
    /// # Safety
    ///
    /// 1. `context` is a valid pointer to a `RedisSearchCtx`.
    /// 2. `context.spec` is a valid pointer to an `IndexSpec`.
    /// 3. 1 and 2 must stay valid during the iterator's lifetime.
    pub fn with_context(
        reader: R,
        filter_ctx: FieldFilterContext,
        context: NonNull<RedisSearchCtx>,
    ) -> Self {
        let result = RSIndexResult::numeric(0.0);
        Self {
            it: InvIndIterator::new(reader, result, filter_ctx, Some(context)),
        }
    }
}

impl<'index, R> RQEIterator<'index> for Numeric<'index, R>
where
    R: NumericReader<'index>,
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

/// An iterator over term inverted index entries.
///
/// This iterator can be used to query a term inverted index.
///
/// # Type Parameters
///
/// * `'index` - The lifetime of the index being iterated over.
pub struct Term<'index, R> {
    it: InvIndIterator<'index, R>,
}

impl<'index, R> Term<'index, R>
where
    R: TermReader<'index>,
{
    /// Create an iterator returning results from a term inverted index.
    ///
    /// Filtering the results can be achieved by wrapping the reader with
    /// a [`inverted_index::FilterMaskReader`].
    pub fn new(reader: R, field: FieldMaskOrIndex) -> Self {
        let result = RSIndexResult::term();
        let field_ctx = FieldFilterContext {
            field,
            predicate: field::FieldExpirationPredicate::Default,
        };

        Self {
            it: InvIndIterator::new(reader, result, field_ctx, None),
        }
    }
}

impl<'index, R> RQEIterator<'index> for Term<'index, R>
where
    R: TermReader<'index>,
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
