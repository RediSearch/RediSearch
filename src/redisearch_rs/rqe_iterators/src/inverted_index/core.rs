/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    IndexFlags, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED,
    ValidateStatus_VALIDATE_OK,
};
use index_result::{RSIndexResult, RawIndexResult};
use inverted_index::{IndexReader, RefreshOutcome, ResumableReader, SuspendableReader};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    ResumeOutcome, SkipToOutcome, SkipToOutcomeRaw,
    expiration_checker::{ExpirationChecker, NoOpChecker},
};
use index_spec::IndexSpecReadGuard;

/// A generic iterator over inverted index entries, parameterised over a
/// [`Ref`] mode.
///
/// This iterator is used to query an inverted index. See [`InvIndIterator`] for
/// the only instantiation that currently has an [`RQEIterator`] impl.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** For any reader `R` that is itself
///    layout-compatible with its suspended form (invariant 1 on the reader
///    types — e.g. [`RawIndexReaderCore`](inverted_index::RawIndexReaderCore)),
///    the `Active` and `Suspended` instantiations of `RawInvIndIterator` are
///    layout-identical, which is what lets `suspend`/`resume` reinterpret the
///    `Box` in place. This holds because the struct is `#[repr(C)]` and every
///    `Rf`-dependent field is layout-stable across modes: `result` is a
///    [`RawIndexResult`] (proven layout-compatible
///    in `index_result`), `reader: R` is covered by its own invariant, and the
///    `read_impl`/`skip_to_impl` fields are `fn` pointers (pointer-sized in every
///    mode). Enforced, for a representative reader, by the `const _` proof below.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode: [`Active<'a>`] gives the iterator working,
///   readable references into the index; [`Suspended`]
///   produces a passive carrier with no callable surface.
/// * `R` - The reader type used to read the inverted index.
/// * `E` - The expiration checker type used to check for expired documents.
#[repr(C)]
pub struct RawInvIndIterator<'query, Rf: Ref, R, E = NoOpChecker> {
    /// The reader used to iterate over the inverted index.
    pub(super) reader: R,
    /// if we reached the end of the index.
    at_eos: bool,
    /// the last document ID read by the iterator.
    last_doc_id: DocId,
    /// A reusable result object to avoid allocations on each `read` call.
    pub(super) result: RawIndexResult<'query, Rf>,

    /// The expiration checker used to determine if documents are expired.
    expiration_checker: E,

    /// Cached [`IndexFlags`] of the underlying inverted index, snapshotted at
    /// construction time from `reader.flags()`. Kept here so FFI introspection
    /// (e.g. `InvIndIterator_GetReaderFlags` during FT.PROFILE printing) can
    /// read flags regardless of `Rf` — the live reader's `flags()` method is
    /// only available in the [`Active`] instantiation, but the value itself
    /// is immutable for the iterator's lifetime.
    flags: IndexFlags,

    /// Cached count of unique documents in the underlying index, snapshotted at
    /// construction time from `reader.unique_docs()`. Backs
    /// [`RQEIterator::num_estimated`] (active) and
    /// [`RQESuspendedIterator::num_estimated`] (suspended) uniformly, so
    /// FT.PROFILE introspection can read the estimate regardless of `Rf` — the
    /// live reader's `unique_docs()` is only callable in the [`Active`]
    /// instantiation. The value is an estimate; the snapshot is acceptable.
    num_docs: u64,
    /// The implementation of the [`read`](RQEIterator::read) method.
    /// Using dynamic dispatch so we can pick the right version during the
    /// iterator construction saving to re-do the checks each time [`read()`](RQEIterator::read) is called.
    read_impl: fn(&mut Self) -> Result<Option<&mut RawIndexResult<'query, Rf>>, RQEIteratorError>,
    /// The implementation of the [`skip_to`](RQEIterator::skip_to) method.
    #[expect(
        clippy::type_complexity,
        reason = "Specialised fn pointer parameterised by Rf — extracting it to a type alias \
                  would hide the Rf dependency that drives transmute soundness across modes."
    )]
    skip_to_impl:
        fn(&mut Self, DocId) -> Result<Option<SkipToOutcomeRaw<'_, 'query, Rf>>, RQEIteratorError>,
}

/// Alias for an [`Active`] [`RawInvIndIterator`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type InvIndIterator<'index, R, E = NoOpChecker> =
    RawInvIndIterator<'index, Active<'index>, R, E>;

// Compile-time proof of invariant 1 on `RawInvIndIterator`: for a representative
// concrete reader, the `Active` and `Suspended` instantiations are
// layout-identical. The reader's own layout compatibility is invariant 1 on
// `RawIndexReaderCore`; the `result` field's is proven in `index_result`.
const _: () = {
    use inverted_index::{RawIndexReaderCore, doc_ids_only::DocIdsOnly};
    use std::mem::{align_of, offset_of, size_of};
    type A = InvIndIterator<'static, RawIndexReaderCore<Active<'static>, DocIdsOnly>, NoOpChecker>;
    type S =
        RawInvIndIterator<'static, Suspended, RawIndexReaderCore<Suspended, DocIdsOnly>, NoOpChecker>;
    assert!(offset_of!(A, reader) == offset_of!(S, reader));
    assert!(offset_of!(A, at_eos) == offset_of!(S, at_eos));
    assert!(offset_of!(A, last_doc_id) == offset_of!(S, last_doc_id));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, expiration_checker) == offset_of!(S, expiration_checker));
    assert!(offset_of!(A, flags) == offset_of!(S, flags));
    assert!(offset_of!(A, num_docs) == offset_of!(S, num_docs));
    assert!(offset_of!(A, read_impl) == offset_of!(S, read_impl));
    assert!(offset_of!(A, skip_to_impl) == offset_of!(S, skip_to_impl));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, Rf: Ref, R, E> RawInvIndIterator<'query, Rf, R, E> {
    /// Read the cached `last_doc_id` regardless of mode.
    ///
    /// `last_doc_id` is a plain `t_docId` field whose value survives the
    /// suspend/resume transition unchanged. Composite iterators
    /// (`Term`, `Numeric`, …, `Optional`, etc.) need this on the suspended
    /// side to feed [`RQESuspendedIterator::last_doc_id`].
    pub(crate) const fn last_doc_id_field(&self) -> DocId {
        self.last_doc_id
    }

    /// Read the cached [`IndexFlags`] regardless of mode.
    ///
    /// Snapshotted at construction time from the reader's `flags()`. The
    /// value is immutable for the iterator's lifetime; callers reading this
    /// while suspended (e.g. FT.PROFILE introspection after the iterator has
    /// transitioned to `Suspended`) get the same answer they would on
    /// `Active`.
    pub const fn flags(&self) -> IndexFlags {
        self.flags
    }

    /// Read the cached unique-document count regardless of mode.
    ///
    /// Snapshotted at construction from `reader.unique_docs()`. Backs both the
    /// active and suspended [`num_estimated`](RQESuspendedIterator::num_estimated)
    /// impls, so FT.PROFILE introspection works after the iterator has
    /// transitioned to `Suspended`, where the live reader is unavailable.
    pub(crate) const fn num_docs_field(&self) -> u64 {
        self.num_docs
    }
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

        let flags = reader.flags();
        let num_docs = reader.unique_docs();

        Self {
            reader,
            at_eos: false,
            last_doc_id: 0,
            result,
            expiration_checker,
            flags,
            num_docs,
            read_impl,
            skip_to_impl,
        }
    }

    /// Returns the current query term bytes, if available.
    ///
    /// The term is stored in the iterator's result and is set during
    /// construction. Returns [`None`] if the result is not a term result
    /// or the term has no string representation.
    pub fn query_term_bytes(&self) -> Option<&[u8]> {
        self.result.as_term()?.query_term()?.as_bytes()
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
        doc_id: DocId,
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
        doc_id: DocId,
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
        doc_id: DocId,
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
        self.num_docs_field() as usize
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eos
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        unimplemented!(
            "InvIndIterator::type_() should not be called directly; use the specific iterator type"
        )
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}
// ---- Suspend / Resume ------------------------------------------------------

impl<'query, RS, E> RawInvIndIterator<'query, Suspended, RS, E>
where
    RS: ResumableReader,
    E: ExpirationChecker + 'static,
{
    /// Refresh this iterator's reader-side pointers in place while still
    /// in [`Suspended`] mode.
    ///
    /// Delegates to [`ResumableReader::refresh_pointers`] on the inner
    /// reader. Returns whether the iterator's position is still usable as
    /// a [`RefreshOutcome::Ok`], or whether the caller must rewind +
    /// re-seek to `last_doc_id` after the cast to [`Active`]
    /// ([`RefreshOutcome::NeedsReseek`]).
    ///
    /// # Why on the suspended form
    ///
    /// See [`ResumableReader::refresh_pointers`] for the full argument.
    /// In short: doing the refresh on the suspended form avoids
    /// materializing any `&'a` borrow of `self.reader.buf` before the
    /// fresh pointer has been written — the `Box<Suspended>` →
    /// `Box<Active<'a>>` cast happens only after every `Rf`-dependent
    /// field is known to be valid.
    pub(crate) fn refresh_pointers(&mut self, _guard: &IndexSpecReadGuard<'_>) -> RefreshOutcome {
        // SAFETY: the caller proves the spec read lock is held by passing
        // `_guard` — a witness that C holds the read lock (the only way to obtain
        // an `IndexSpecReadGuard`). `ResumableReader::refresh_pointers` requires
        // exactly that lock while it dereferences the reader's raw index pointer.
        unsafe { self.reader.refresh_pointers() }
    }
}

impl<'index, R, E> InvIndIterator<'index, R, E>
where
    R: IndexReader<'index>,
    E: ExpirationChecker,
{
    /// Re-seek the iterator to its previous `last_doc_id` after a GC
    /// cycle invalidated the cached block offset.
    ///
    /// Called from [`RQESuspendedIterator::resume`] on the active side,
    /// after [`RawInvIndIterator::refresh_pointers`] reported
    /// [`RefreshOutcome::NeedsReseek`]. Translates the
    /// [`SkipToOutcome`] returned by [`skip_to`](Self::skip_to) into a
    /// [`ValidateStatus`].
    ///
    /// The `last_doc_id` parameter is the value reported by
    /// `refresh_pointers` (the reader's decoder base) and is ignored —
    /// it can be non-zero on a freshly-constructed reader before any
    /// reads happen, which would cause a spurious reseek. We use
    /// [`Self::last_doc_id_field`] instead (the iterator's tracked
    /// "position of the most recent read", 0 if no reads have happened).
    ///
    /// # Outcome mapping
    ///
    /// - [`SkipToOutcome::Found`] → `VALIDATE_OK` (same logical position).
    /// - [`SkipToOutcome::NotFound`] or EOF → `VALIDATE_MOVED` (the
    ///   previous `last_doc_id` no longer exists; the iterator has
    ///   advanced past it).
    /// - `skip_to` decode/I/O error → `VALIDATE_ABORTED`. A decode error means
    ///   the re-seek hit a corrupted/malformed block, leaving the reader in a
    ///   partially-updated state; rather than hand back a live iterator whose
    ///   `current()` may be bogus, we abort (matching the legacy `revalidate`
    ///   path, which propagated the error so the FFI wrapper aborts).
    pub(crate) fn reseek_after_refresh(&mut self, _last_doc_id: DocId) -> ValidateStatus {
        let target_doc_id = self.last_doc_id;
        self.rewind();

        if target_doc_id == 0 {
            // We hadn't returned anything yet, so there's nothing to
            // re-seek; a fresh `read()` will produce the right next doc.
            return ValidateStatus_VALIDATE_OK;
        }

        match self.skip_to(target_doc_id) {
            Ok(Some(SkipToOutcome::Found(_))) => ValidateStatus_VALIDATE_OK,
            Ok(Some(SkipToOutcome::NotFound(_))) | Ok(None) => ValidateStatus_VALIDATE_MOVED,
            Err(_) => ValidateStatus_VALIDATE_ABORTED,
        }
    }
}

impl<'index, R, E> RQEIteratorBoxed<'index> for InvIndIterator<'index, R, E>
where
    R: IndexReader<'index> + SuspendableReader + 'index,
    R::Suspended: ResumableReader,
    E: ExpirationChecker + 'static,
{
    type Suspended = RawInvIndIterator<'index, Suspended, R::Suspended, E>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Reuse the same allocation so external pointers into the iterator
        // interior (composite aggregate results) stay valid across the cycle.
        let active: *mut Self = Box::into_raw(self);

        // SAFETY: `active` just came from a `Box`, so it is non-null, aligned,
        // initialized, and unaliased (we own it). `&raw mut` forms a field
        // pointer without creating a reference, leaving provenance over the
        // whole allocation intact for the cast below.
        let result_slot = unsafe { &raw mut (*active).result };
        // SAFETY: `result_slot` points at an initialized `RSIndexResult<'index>`
        // and is unaliased. Suspending only loosens validity, so there is no
        // further precondition. Converting the `Rf`-carrying `result` field
        // through the canonical in-place conversion — rather than folding it
        // into the blanket struct reinterpretation below — keeps the
        // borrowed-data transition explicit and auditable.
        unsafe { RawIndexResult::<Active<'index>>::into_suspended_in_place(result_slot) };

        // SAFETY: `result` is now the suspended form; the remaining `Rf`-dependent
        // fields (`reader`, the `fn` pointers) are handled by reinterpreting the
        // whole `#[repr(C)]` struct, which is sound by invariant 1 on
        // `RawInvIndIterator` (const proof above) given invariant 1 on the reader
        // `R`. `Box::from_raw` reuses the same allocation, so the box address is
        // preserved.
        unsafe {
            Box::from_raw(active.cast::<RawInvIndIterator<'index, Suspended, R::Suspended, E>>())
        }
    }
}

impl<'query, RS, E> RQESuspendedIterator<'query> for RawInvIndIterator<'query, Suspended, RS, E>
where
    RS: ResumableReader,
    E: ExpirationChecker + 'static,
{
    type Resumed<'a>
        = InvIndIterator<'a, RS::Resumed<'a>, E>
    where
        'query: 'a;

    /// # Precondition
    ///
    /// This core resume refreshes the reader's pointers (dereferencing the
    /// stored index pointer via [`refresh_pointers`](RawInvIndIterator::refresh_pointers))
    /// without performing an index-identity check of its own. It assumes the
    /// caller — a leaf wrapper's `resume` (`Term`/`Tag`/`Missing`/`Wildcard`/
    /// `Numeric`), which runs its `should_abort` identity check *first* — has
    /// already established that the underlying inverted index is still live
    /// (not freed or replaced by GC while suspended). The bare-core iterator is
    /// not resumed directly in production.
    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Step 1: refresh reader pointers *on the suspended form*. This
        // never materializes a `&'a` borrow of any potentially-dangling
        // field — the `Box<Suspended>` → `Box<Active<'a>>` conversion happens
        // afterwards, only once every `Rf`-dependent field is valid. Passing
        // `guard` proves the spec read lock is held for the pointer refresh.
        let outcome = self.refresh_pointers(guard);

        // Step 2: convert Suspended → Active in place. The heap allocation is
        // preserved, so external pointers into this iterator's interior
        // (composite aggregate results) stay valid across the cycle.
        let suspended: *mut Self = Box::into_raw(self);

        // SAFETY: `suspended` just came from a `Box`, so it is non-null, aligned,
        // initialized, and unaliased (we own it). `&raw mut` forms a field
        // pointer without creating a reference, leaving provenance over the
        // whole allocation intact for the cast below.
        let result_slot = unsafe { &raw mut (*suspended).result };
        // SAFETY: `into_active_in_place`'s preconditions for `'a` hold — the
        // caller's read lock (witnessed by `_guard`) plus the `refresh_pointers`
        // call above ensure every index-backed pointer inside `result` is
        // dereferenceable for `'a`; `result_slot` is initialized and unaliased.
        // Converting the `Rf`-carrying `result` field through the canonical
        // in-place conversion keeps the borrowed-data transition explicit.
        unsafe { RawIndexResult::<'query, Suspended>::into_active_in_place::<'a>(result_slot) };

        // SAFETY: `result` is now the active form; the remaining `Rf`-dependent
        // fields (`reader`, the `fn` pointers) are handled by reinterpreting the
        // whole `#[repr(C)]` struct, which is sound by invariant 1 on
        // `RawInvIndIterator` (const proof above) given invariant 1 on the reader
        // `RS`. `Box::from_raw` reuses the same allocation, so the box address is
        // preserved.
        let mut active = unsafe {
            Box::from_raw(suspended.cast::<InvIndIterator<'a, RS::Resumed<'a>, E>>())
        };

        // Step 3: if GC ran while we were suspended, the cached block
        // offset is stale even though the buffer pointer was refreshed.
        // Rewind and re-seek to the last doc id.
        let status = match outcome {
            RefreshOutcome::Ok => ValidateStatus_VALIDATE_OK,
            RefreshOutcome::NeedsReseek { last_doc_id } => active.reseek_after_refresh(last_doc_id),
        };

        // Map the re-seek status to a resume outcome. A decode error during the
        // re-seek yields `VALIDATE_ABORTED` (see `reseek_after_refresh`): the
        // block is corrupted, so drop the active iterator rather than hand back a
        // bogus `current()`.
        Ok(if status == ValidateStatus_VALIDATE_ABORTED {
            drop(active);
            ResumeOutcome::Aborted
        } else if status == ValidateStatus_VALIDATE_MOVED {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id_field()
    }

    fn num_estimated(&self) -> usize {
        // The live reader's `unique_docs()` is unavailable once weakened, so we
        // return the snapshot cached at construction (see `num_docs`). This
        // matches the active `num_estimated` and keeps FT.PROFILE introspection
        // of a suspended iterator meaningful.
        self.num_docs_field() as usize
    }
}
