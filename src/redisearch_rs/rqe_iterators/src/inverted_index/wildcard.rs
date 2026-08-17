/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReaderCore, RawIndexReaderCore, opaque::OpaqueEncoding,
};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    expiration_checker::NoOpChecker,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use super::core::{InvIndIterator, RawInvIndIterator, ResumeStatus};
use rqe_core::RS_FIELDMASK_ALL;

/// An iterator over all existing documents in an index, parameterised over
/// a [`Ref`] mode. See [`Wildcard`] for the [`Active`] instantiation that
/// implements [`RQEIterator`].
///
/// Used for wildcard queries (`*`), where the goal is to match every document
/// rather than filtering by a specific term or numeric range. The set of
/// existing documents is maintained by the index spec in its `existingDocs`
/// inverted index.
///
/// Unlike [`super::Term`] and [`super::Numeric`], this iterator does not support
/// per-field expiration checks — it always uses [`NoOpChecker`].
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode (see [`RawInvIndIterator`] for details).
/// * `E` - The encoding type for the inverted index. Its decoder must implement [`DocIdsDecoder`].
#[repr(C)]
pub struct RawWildcard<
    'query,
    Rf: Ref,
    E: DecodedBy,
    // Frozen active reader for the inner iterator's dispatch pointers; hardcoded
    // to the `Active` reader regardless of `Rf` (see `RawInvIndIterator`'s `RA`).
    RA = RawIndexReaderCore<Active<'query>, E>,
> {
    // `pub(crate)` so the top-level `OptimizedWildcard` wrapper can drive the
    // inner iterator's `resume_in_place` on its inline variants.
    pub(crate) it: RawInvIndIterator<'query, Rf, RawIndexReaderCore<Rf, E>, NoOpChecker, RA>,
}

/// Alias for an [`Active`] [`RawWildcard`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Wildcard<'index, E> = RawWildcard<'index, Active<'index>, E>;

impl<'query, Rf: Ref, E, RA> RawWildcard<'query, Rf, E, RA>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may either null out `existingDocs` (after
    /// collecting all documents) or replace it with a new allocation. In
    /// both cases the reader's pointer is stale and the iterator must
    /// [abort](RQEValidateStatus::Aborted).
    ///
    /// # Safety
    ///
    /// 1. `spec.existingDocs`, when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::InvertedIndex) whose encoding
    ///    variant matches `E`.
    ///
    /// `pub(crate)` so the top-level `OptimizedWildcard` wrapper can run the
    /// identity check on its inline inverted-index wildcard variants.
    pub(crate) fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
        // the garbage collector may set existing_docs to NULL after garbage collecting all documents
        let Some(existing_docs) = spec.existing_docs() else {
            return true;
        };

        // SAFETY: The encoding variant matches E (structural invariant).
        let ii = E::from_opaque(existing_docs);

        !self.it.reader.points_to_ii(ii)
    }
}

impl<'index, E> Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Create an iterator returning all documents from the `existingDocs`
    /// inverted index.
    ///
    /// `weight` is the score weight applied to every returned result.
    pub fn new(reader: IndexReaderCore<'index, E>, weight: f64) -> Self {
        let result = RSIndexResult::build_virt()
            .weight(weight)
            .field_mask(RS_FIELDMASK_ALL)
            .frequency(1)
            .build();

        Self {
            // Wildcard iterator does not support expiration check
            it: InvIndIterator::new(reader, result, NoOpChecker),
        }
    }

    /// Get a reference to the underlying reader.
    pub const fn reader(&self) -> &IndexReaderCore<'index, E> {
        &self.it.reader
    }
}

impl<'index, E> RQEIterator<'index> for Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'index,
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
        doc_id: DocId,
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
    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id()
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // The existingDocs encoding match is a structural invariant: the
        // encoding is determined at index creation and cannot change.
        if self.should_abort(spec) {
            return Ok(RQEValidateStatus::Aborted);
        }

        self.it.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxWildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<E: DecodedBy> ProfilePrint for Wildcard<'_, E> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"WILDCARD", map);
    }
}

impl<'index, E> RQEIteratorBoxed<'index> for Wildcard<'index, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    type Suspended = RawWildcard<'index, Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is a `#[repr(C)]` newtype whose only
        // `Rf`-dependent field is the inner `RawInvIndIterator`, layout-identical
        // across modes by invariant 1 on [`RawInvIndIterator`] (const proof
        // there). `Box::from_raw` reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawWildcard<'index, Suspended, E>) }
    }
}

impl<'query, E, RA> RQESuspendedIterator<'query> for RawWildcard<'query, Suspended, E, RA>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    type Resumed<'a>
        = Wildcard<'a, E>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: identity check on the suspended form. On abort we drop the
        // suspended iterator without promoting it to Active — nothing is
        // materialized.
        if self.should_abort(spec) {
            return Ok(ResumeOutcome::Aborted);
        }

        // Step 2: run the shared in-place resume transition on the inner
        // core iterator (refresh pointers, reset stale offsets, promote the
        // result, and re-seek if GC moved us). `spec` witnesses the read lock
        // the refresh requires.
        let status = self.it.resume_in_place(spec)?;

        // Step 3: reinterpret the owning box's type. The heap address is
        // preserved across the cast.
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is a `#[repr(C)]` newtype whose only
        // `Rf`-dependent field is the inner `RawInvIndIterator`, layout-identical
        // across modes by invariant 1 on [`RawInvIndIterator`] (const proof
        // there). `resume_in_place` left the inner iterator as a valid active
        // iterator, so the whole `RawWildcard` is now a valid `Wildcard<'a, E>`.
        let active = unsafe { Box::from_raw(raw as *mut Wildcard<'a, E>) };

        Ok(match status {
            ResumeStatus::Unchanged => ResumeOutcome::Ok(active),
            ResumeStatus::Moved => ResumeOutcome::Moved(active),
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }
}
