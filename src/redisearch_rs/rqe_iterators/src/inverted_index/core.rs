/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::IndexFlags;
use index_result::{RSIndexResult, RawIndexResult, RawOffsetSlice, RawTermRecord};
use index_spec::IndexSpecReadGuard;
use inverted_index::{IndexReader, RefreshOutcome, ResumableReader, SuspendableReader};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    expiration_checker::{ExpirationChecker, NoOpChecker},
};

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
///    [`RawIndexResult`] (proven layout-compatible in `index_result`) and
///    `reader: R` is covered by its own invariant. The `read_impl`/`skip_to_impl`
///    fn-pointer fields are frozen at the mode-independent reader parameter `RA`
///    (see below), so they are the *same type* in every mode — the cast does not
///    reinterpret them at all. Enforced, for a representative reader, by the
///    `const _` proof below.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode: [`Active<'a>`] gives the iterator working,
///   readable references into the index; [`Suspended`]
///   produces a passive carrier with no callable surface.
/// * `R` - The reader type used to read the inverted index. Weakens to
///   `R::Suspended` when the iterator is suspended.
/// * `E` - The expiration checker type used to check for expired documents.
/// * `RA` - The **active** reader type the `read_impl`/`skip_to_impl` dispatch
///   pointers are compiled for. Defaults to `R` (the active instantiation) and,
///   unlike `R`, is **not** remapped on suspend — so those fn-pointer fields keep
///   an identical type across the `Active`/`Suspended` cast. It only appears
///   inside the `fn`-pointer field types (which take `&mut InvIndIterator`), so a
///   suspended carrier cannot invoke them.
#[repr(C)]
pub struct RawInvIndIterator<'query, Rf: Ref, R, E = NoOpChecker, RA = R> {
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
    /// construction time from `reader.unique_docs()`. Kept here so introspection
    /// (e.g. `FT.PROFILE` estimate printing) can read the estimate regardless of
    /// `Rf` — the live reader's `unique_docs()` is only callable in the
    /// [`Active`] instantiation, but the snapshot is a stable estimate that both
    /// modes can report.
    num_docs: u64,

    /// The implementation of the [`read`](RQEIterator::read) method.
    /// Using dynamic dispatch so we can pick the right version during the
    /// iterator construction saving to re-do the checks each time [`read()`](RQEIterator::read) is called.
    ///
    /// Typed over the frozen active reader `RA` (not `Self`/`Rf`), so the field
    /// keeps an identical type across the suspend/resume cast and can only be
    /// invoked on the active form.
    read_impl: ReadImpl<'query, RA, E>,
    /// The implementation of the [`skip_to`](RQEIterator::skip_to) method.
    /// Typed over the frozen active reader `RA` — see [`Self::read_impl`].
    skip_to_impl: SkipToImpl<'query, RA, E>,
}

/// Dispatch pointer backing [`RQEIterator::read`], frozen at the active reader
/// `RA` (see [`RawInvIndIterator`]'s type parameters) so it is layout-stable
/// across the suspend/resume cast.
type ReadImpl<'query, RA, E> =
    for<'s> fn(
        &'s mut InvIndIterator<'query, RA, E>,
    ) -> Result<Option<&'s mut RSIndexResult<'query>>, RQEIteratorError>;

/// Dispatch pointer backing [`RQEIterator::skip_to`], frozen at the active
/// reader `RA` — see [`ReadImpl`].
type SkipToImpl<'query, RA, E> =
    for<'s> fn(
        &'s mut InvIndIterator<'query, RA, E>,
        DocId,
    ) -> Result<Option<SkipToOutcome<'s, 'query>>, RQEIteratorError>;

/// Alias for an [`Active`] [`RawInvIndIterator`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type InvIndIterator<'index, R, E = NoOpChecker> =
    RawInvIndIterator<'index, Active<'index>, R, E, R>;

// Compile-time proof of invariant 1 on `RawInvIndIterator`: for a representative
// concrete reader, the `Active` and `Suspended` instantiations are
// layout-identical. The reader's own layout compatibility is invariant 1 on
// `RawIndexReaderCore`; the `result` field's is proven in `index_result`.
const _: () = {
    use inverted_index::{RawIndexReaderCore, doc_ids_only::DocIdsOnly};
    use std::mem::{align_of, offset_of, size_of};
    type A = InvIndIterator<'static, RawIndexReaderCore<Active<'static>, DocIdsOnly>, NoOpChecker>;
    // `S` mirrors the suspended form: the reader slot weakens to the `Suspended`
    // reader, but the frozen `RA` slot stays the *active* reader (matching `A`),
    // so `read_impl`/`skip_to_impl` are the same type in `A` and `S`.
    type S = RawInvIndIterator<
        'static,
        Suspended,
        RawIndexReaderCore<Suspended, DocIdsOnly>,
        NoOpChecker,
        RawIndexReaderCore<Active<'static>, DocIdsOnly>,
    >;
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

impl<'query, Rf: Ref, R, E, RA> RawInvIndIterator<'query, Rf, R, E, RA> {
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
    /// Snapshotted at construction from `reader.unique_docs()`. Feeds
    /// [`RQESuspendedIterator::num_estimated`] on the suspended side, where the
    /// live reader is unavailable.
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
        self.reader.unique_docs() as usize
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn at_eof(&self) -> bool {
        self.at_eos
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if !self.reader.needs_revalidation() {
            return Ok(RQEValidateStatus::Ok);
        }

        // If there has been a GC cycle on this key while we were suspended, the offset might not be valid
        // anymore. This means that we need to seek the last docId we were at
        let last_doc_id = self.last_doc_id();
        // Reset the state of the reader
        self.rewind();

        if last_doc_id == 0 {
            // No need to skip if we're starting from the very beginning.
            return Ok(RQEValidateStatus::Ok);
        }

        // Try jumping to the last docId
        let res = match self.skip_to(last_doc_id)? {
            Some(SkipToOutcome::Found(_)) => RQEValidateStatus::Ok,
            Some(SkipToOutcome::NotFound(doc)) => RQEValidateStatus::Moved { current: Some(doc) },
            None => RQEValidateStatus::Moved { current: None },
        };

        Ok(res)
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

/// Outcome of an in-place suspended→active resume
/// ([`resume_in_place`](RawInvIndIterator::resume_in_place)).
///
/// Reports whether the iterator's position is unchanged or whether a re-seek
/// after a GC/relocation moved it past the previous document.
pub(crate) enum ResumeStatus {
    /// The iterator resumed at the same position it held before suspending.
    Unchanged,
    /// GC or a block relocation invalidated the cached offset; the re-seek
    /// landed the iterator on a different (later) document, or off the end.
    Moved,
}

impl<'query, RS, E, RA> RawInvIndIterator<'query, Suspended, RS, E, RA>
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

    /// Perform the full suspended→active resume transition **in place**.
    ///
    /// Refreshes the reader's pointers, resets any stale borrowed offsets,
    /// promotes the `Rf`-carrying `result` field to its active form, and — if
    /// GC or a block relocation invalidated the cached offset — rewinds and
    /// re-seeks to the last document the iterator returned. On return, `self`'s
    /// storage is a valid `InvIndIterator<'a, RS::Resumed<'a>, E>` at the same
    /// address; the caller only has to reinterpret the owning `Box`'s type,
    /// which is a layout-identical no-op cast (invariant 1, const proof above).
    ///
    /// This is the single shared resume path for the bare-core iterator and for
    /// every leaf wrapper (`Term`/`Tag`/`Missing`/`Wildcard`/`Numeric`), so the
    /// borrowed-data transition lives in exactly one place.
    ///
    /// # Precondition
    ///
    /// Like [`RQESuspendedIterator::resume`], this refreshes the reader pointers
    /// without an index-identity check of its own. It assumes the caller has
    /// already established that the underlying inverted index is still live
    /// (leaf wrappers run their `should_abort` check first).
    pub(crate) fn resume_in_place<'a>(
        &mut self,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeStatus, RQEIteratorError>
    where
        'query: 'a,
        RS::Resumed<'a>: IndexReader<'a>,
    {
        // We use the last doc id yielded by the iterator, rather than the reader state,
        // since the reader initializes its own field to the first block's first_doc_id
        // before any result has been returned, so if GC or a moved block buffer occurs
        // while a freshly-created iterator is suspended, resume would reseek to that first
        // doc and report Ok, causing the next read() to skip the first result.
        let last_doc_id = self.last_doc_id;

        // Step 1: refresh reader pointers *on the suspended form*. This never
        // materializes a `&'a` borrow of any potentially-dangling field — the
        // Suspended → Active reinterpretation happens afterwards, only once every
        // `Rf`-dependent field is valid. Passing `guard` proves the spec read
        // lock is held for the pointer refresh.
        let outcome = self.refresh_pointers(guard);

        // Whatever offsets we borrowed from the index may be stale. To make
        // promotion from suspended to active safe, we unset them while still
        // suspended, before any active reference to `result` can be formed.
        if outcome == RefreshOutcome::NeedsReseek
            && let Some(term) = self.result.as_term_mut()
        {
            match term {
                RawTermRecord::Borrowed { offsets, .. } => {
                    *offsets = RawOffsetSlice::empty();
                }
                RawTermRecord::Owned { .. } | RawTermRecord::FullyOwned { .. } => {
                    // These variants own their offsets, so no issues.
                }
            }
        }

        // Step 2: convert the `Rf`-carrying `result` field Suspended → Active in
        // place, then reinterpret the rest of the struct by reborrowing `self`'s
        // storage as the active iterator. The heap allocation is untouched, so
        // external pointers into this iterator's interior stay valid.
        //
        // `&raw mut` forms a field pointer without creating a reference, leaving
        // provenance over the whole allocation intact for the reborrow below.
        let result_slot = &raw mut self.result;
        // SAFETY: `into_active_in_place`'s preconditions for `'a` hold — the
        // caller's read lock (witnessed by `guard`) plus the `refresh_pointers`
        // call above ensure every index-backed pointer inside `result` is
        // dereferenceable for `'a`; `result_slot` is initialized and unaliased.
        unsafe { RawIndexResult::<'query, Suspended>::into_active_in_place::<'a>(result_slot) };
        // SAFETY: `result` is now the active form; the remaining `Rf`-dependent
        // fields (`reader`, the `fn` pointers) are layout-identical across modes
        // by invariant 1 on `RawInvIndIterator` (const proof above) given
        // invariant 1 on the reader `RS`. Reborrowing the same storage as the
        // active iterator is therefore sound, and no `&mut self` borrow outlives
        // this reborrow (we never touch `self` again).
        let active: &mut InvIndIterator<'a, RS::Resumed<'a>, E> =
            unsafe { &mut *(self as *mut Self).cast() };

        // Step 3: if GC ran while we were suspended, the cached block offset is
        // stale even though the buffer pointer was refreshed. Rewind and re-seek
        // to the last doc id.
        match outcome {
            RefreshOutcome::Ok => Ok(ResumeStatus::Unchanged),
            RefreshOutcome::NeedsReseek => {
                active.rewind();

                if last_doc_id == 0 {
                    // We hadn't returned anything yet, so there's nothing to
                    // re-seek; a fresh `read()` will produce the right next doc.
                    return Ok(ResumeStatus::Unchanged);
                }

                Ok(match active.skip_to(last_doc_id)? {
                    Some(SkipToOutcome::Found(_)) => ResumeStatus::Unchanged,
                    Some(SkipToOutcome::NotFound(_)) | None => ResumeStatus::Moved,
                })
            }
        }
    }
}

impl<'index, R, E> RQEIteratorBoxed<'index> for InvIndIterator<'index, R, E>
where
    R: IndexReader<'index> + SuspendableReader + 'index,
    R::Suspended: ResumableReader,
    E: ExpirationChecker + 'static,
{
    // Reader weakens `R -> R::Suspended`, but the frozen `RA` slot stays `R` (the
    // active reader), so `read_impl`/`skip_to_impl` keep an identical type.
    type Suspended = RawInvIndIterator<'index, Suspended, R::Suspended, E, R>;

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

        // SAFETY: `result` is now the suspended form; the only other `Rf`-dependent
        // field, `reader`, is reinterpreted `R -> R::Suspended` — sound by
        // invariant 1 on `RawInvIndIterator` (const proof above) given invariant 1
        // on the reader `R`. The `read_impl`/`skip_to_impl` fn-pointer fields are
        // frozen at `RA = R` and so are *not* reinterpreted by this cast at all.
        // `Box::from_raw` reuses the same allocation, so the box address is
        // preserved.
        unsafe {
            Box::from_raw(active.cast::<RawInvIndIterator<'index, Suspended, R::Suspended, E, R>>())
        }
    }
}

impl<'query, RS, E, RA> RQESuspendedIterator<'query>
    for RawInvIndIterator<'query, Suspended, RS, E, RA>
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
    /// This core resume refreshes the reader's pointers (via
    /// [`resume_in_place`](RawInvIndIterator::resume_in_place)) without
    /// performing an index-identity check of its own. It assumes the caller — a
    /// leaf wrapper's `resume` (`Term`/`Tag`/`Missing`/`Wildcard`/`Numeric`),
    /// which runs its `should_abort` identity check *first* — has already
    /// established that the underlying inverted index is still live (not freed
    /// or replaced by GC while suspended).
    ///
    /// The liveness check is deliberately **not** performed here: it is
    /// leaf-specific (each leaf locates its index differently — `Term` via
    /// `keysDict`, `Tag` via the tag `TrieMap`, `Missing` via `missingFieldDict`,
    /// `Wildcard` via `existingDocs`), so it cannot be expressed generically at
    /// this layer. Consequently, resuming the *bare* core iterator directly —
    /// i.e. an [`InvIndIterator`] not wrapped by one of those leaves — after the
    /// index was fully GC'd or replaced while suspended would let
    /// `resume_in_place` dereference the stale saved index pointer. That path is
    /// never exercised in production: every core iterator is owned by a leaf
    /// wrapper whose `resume` gates on `should_abort` before delegating here. If
    /// a future caller resumes the bare core directly, it must first prove the
    /// index identity itself.
    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Do the whole borrowed-data transition in place on the boxed value.
        let status = self.resume_in_place(guard)?;

        // Reinterpret the owning box's type. The heap allocation is preserved,
        // so external pointers into this iterator's interior (composite
        // aggregate results) stay valid across the cycle.
        //
        // SAFETY: `resume_in_place` left `self`'s storage as a valid
        // `InvIndIterator<'a, RS::Resumed<'a>, E>` (layout-identical to
        // `Self` by invariant 1 on `RawInvIndIterator`, const proof above).
        // `Box::from_raw` reuses the same allocation, so the box address is
        // preserved.
        let active = unsafe {
            Box::from_raw(Box::into_raw(self).cast::<InvIndIterator<'a, RS::Resumed<'a>, E>>())
        };

        Ok(match status {
            ResumeStatus::Unchanged => ResumeOutcome::Ok(active),
            ResumeStatus::Moved => ResumeOutcome::Moved(active),
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
