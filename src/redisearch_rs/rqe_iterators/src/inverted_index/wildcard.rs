/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK, t_docId,
};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::{
    DecodedBy, DocIdsDecoder, IndexReaderCore, RawIndexReaderCore, RefreshOutcome,
    opaque::OpaqueEncoding,
};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQESuspendedIterator, SkipToOutcome,
    expiration_checker::NoOpChecker,
};

use super::core::{InvIndIterator, RawInvIndIterator};

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
pub struct RawWildcard<Rf: Ref, E: DecodedBy> {
    it: RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>>,
}

/// Alias for an [`Active`] [`RawWildcard`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Wildcard<'index, E> = RawWildcard<Active<'index>, E>;

impl<Rf: Ref, E> RawWildcard<Rf, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Check if the iterator should abort revalidation.
    ///
    /// The garbage collector may either null out `existingDocs` (after
    /// collecting all documents) or replace it with a new allocation. In
    /// both cases the reader's pointer is stale and the iterator must
    /// abort with `VALIDATE_ABORTED`.
    ///
    /// # Why mode-independent
    ///
    /// The body reads only mode-independent state: `spec.existing_docs()`
    /// (a raw pointer lookup) and the reader's `points_to_ii`. No `&'a`
    /// borrow of any reader buffer is materialized, so this is safe to
    /// call on `RawWildcard<Suspended, E>` from
    /// `RQESuspendedIterator::resume`.
    ///
    /// # Safety
    ///
    /// 1. `spec.existingDocs`, when non-null, must point to an opaque
    ///    [`InvertedIndex`](inverted_index::InvertedIndex) whose encoding
    ///    variant matches `E`.
    pub fn should_abort(&self, spec: &IndexSpecReadGuard) -> bool {
        let existing_docs = spec
            .existing_docs()
            .cast::<inverted_index::opaque::InvertedIndex>();
        if existing_docs.is_null() {
            // the garbage collector may set existing_docs to NULL after garbage collecting all documents
            return true;
        }

        // SAFETY: spec.existing_docs() returns a valid pointer when non-null, and we just checked it's not null.
        let existing_docs = unsafe { &*existing_docs };
        // SAFETY: The encoding variant matches E (structural invariant).
        let ii = E::from_opaque(existing_docs);

        !self.it.reader.points_to_ii(ii)
    }
}

impl<'index, E> Wildcard<'index, E>
where
    E: DecodedBy + inverted_index::opaque::OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    /// Forwarding shim: re-seek the inner [`RawInvIndIterator`] after a
    /// GC cycle invalidated the cached block offset. Used by enum-level
    /// `RQESuspendedIterator::resume` implementations in
    /// `iterators_ffi` that need to drive the active-side reseek step
    /// from outside this crate.
    pub fn reseek_after_refresh(&mut self, last_doc_id: t_docId) -> ValidateStatus {
        self.it.reseek_after_refresh(last_doc_id)
    }
}

impl<E: DecodedBy + 'static> RawWildcard<Suspended, E>
where
    for<'a> RawIndexReaderCore<ref_mode::Active<'a>, E>:
        inverted_index::IndexReader<'a>,
{
    /// Forwarding shim: refresh the inner [`RawInvIndIterator`]'s reader
    /// pointers while still in [`Suspended`] mode. Used by enum-level
    /// `RQESuspendedIterator::resume` implementations in
    /// `iterators_ffi` that need to drive the suspended-side refresh
    /// step from outside this crate.
    pub fn refresh_pointers(&mut self) -> inverted_index::RefreshOutcome {
        self.it.refresh_pointers()
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
        use ffi::RS_FIELDMASK_ALL;

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
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    type Suspended = RawWildcard<Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is `#[repr(C)]` containing only a
        // `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>>` field, whose
        // layout is identical across modes (see
        // [`InvIndIterator::suspend`]). Box::from_raw reuses the same heap
        // allocation.
        unsafe { Box::from_raw(raw as *mut RawWildcard<Suspended, E>) }
    }

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
    fn type_(&self) -> IteratorType {
        IteratorType::InvIdxWildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}


impl<E> RQESuspendedIterator for RawWildcard<Suspended, E>
where
    E: DecodedBy + OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>> + 'static,
    <E as DecodedBy>::Decoder: DocIdsDecoder,
{
    type Resumed<'a> = Wildcard<'a, E>;

    fn resume<'a>(
        mut self: Box<Self>,
        spec: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Step 1: should_abort on the suspended form. Reads only
        // mode-independent state (`spec.existing_docs()` and the
        // reader's `points_to_ii`).
        let abort = self.should_abort(spec);

        // Step 2: refresh reader pointers in place while still
        // suspended. Skipped on the abort path.
        let outcome = if abort {
            RefreshOutcome::Ok
        } else {
            self.it.refresh_pointers()
        };

        // Step 3: whole-box cast Suspended → Active.
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is `#[repr(C)]` over an inner
        // `RawInvIndIterator<Rf, RawIndexReaderCore<Rf, E>>`, whose
        // layout is identical across modes (see
        // `RawInvIndIterator::suspend`). The caller's read lock
        // (witnessed by `spec`) plus the `refresh_pointers` step above
        // together ensure every pointer inside the iterator is valid
        // for `'a`.
        let mut active = unsafe { Box::from_raw(raw as *mut Wildcard<'a, E>) };

        if abort {
            return (active, ValidateStatus_VALIDATE_ABORTED);
        }

        // Step 4: if GC ran, re-seek to the previous last_doc_id.
        let status = match outcome {
            RefreshOutcome::Ok => ValidateStatus_VALIDATE_OK,
            RefreshOutcome::NeedsReseek { last_doc_id } => {
                active.it.reseek_after_refresh(last_doc_id)
            }
        };
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated_field()
    }
}