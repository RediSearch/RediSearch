/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator over the document IDs returned by a geometry (GEOSHAPE) query.
//!
//! A geometry query yields a set of candidate document IDs in spatial (R-tree)
//! order. This iterator materializes those IDs into a sorted buffer and walks
//! them in ascending order, the order the rest of the query engine expects.
//!
//! On top of the sorted-id-list behavior it provides three extra features:
//!
//! * **Field expiration** — skips documents whose queried field has expired,
//!   using an [`ExpirationChecker`]. Applied on both the [`read`](RQEIterator::read)
//!   and [`skip_to`](RQEIterator::skip_to) paths, so an expired document never
//!   leaks even when the iterator is driven via [`skip_to`](RQEIterator::skip_to)
//!   (e.g. as a non-leading intersection child). The checker is consulted on
//!   every candidate; it self-gates the no-expiration case (e.g. a missing TTL
//!   table) cheaply, so no cached flag is kept.
//! * **Timeout** — aborts a long scan via an amortized [`TimeoutContext`].
//! * **Memory tracking** — reports the bytes held by the materialized id
//!   buffer through a [`MemTracker`] (the geometry index's allocation tracker),
//!   adding on construction and subtracting on drop, so in-flight query
//!   iterators are reflected in the index's reported memory. Callers that want
//!   no accounting use [`NoTracker`]. The FFI crates provide their own
//!   [`MemTracker`] implementation, backed by the index's externally-owned
//!   counter shared across the C boundary.

use ffi::{ValidateStatus, ValidateStatus_VALIDATE_OK};
use index_result::RSIndexResult;
use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, SkipToOutcome,
    expiration_checker::ExpirationChecker,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::{OwnedSlice, TimeoutContext},
};

/// Outcome of examining a single candidate document during [`read`](GeoShape::read_single).
enum SingleRead {
    /// The candidate is valid and now sits in `result`.
    Ok,
    /// The candidate was skipped (expired); keep scanning.
    NotFound,
    /// No more candidates.
    Eof,
}

/// Sink for a [`GeoShape`] iterator's memory accounting.
///
/// The iterator reports the bytes it holds by calling [`add`](MemTracker::add)
/// once at construction and [`sub`](MemTracker::sub) once on drop, keeping an
/// external memory counter in sync with its live allocation. Implementations
/// decide where that accounting goes: [`NoTracker`] discards it, while the FFI
/// crates provide an implementation that applies it to an externally-owned
/// `usize` counter shared across the C boundary.
pub trait MemTracker {
    /// Records that `bytes` additional bytes are now held.
    fn add(&self, bytes: usize);
    /// Records that `bytes` bytes are no longer held.
    fn sub(&self, bytes: usize);
}

/// A [`MemTracker`] that records nothing.
///
/// Used when no external memory accounting is wanted, e.g. in tests or for
/// iterators not owned by a memory-tracked index.
pub struct NoTracker;

impl MemTracker for NoTracker {
    #[inline(always)]
    fn add(&self, _bytes: usize) {}
    #[inline(always)]
    fn sub(&self, _bytes: usize) {}
}

/// An iterator over a geometry query's matching document IDs.
///
/// # Type parameters
///
/// * `'index` — lifetime of the index being queried.
/// * `T` — the [`TimeoutContext`] used to detect query timeouts.
/// * `E` — the [`ExpirationChecker`] used to skip expired documents.
/// * `M` — the [`MemTracker`] kept in sync with the iterator's allocation.
pub struct GeoShape<'index, T, E, M: MemTracker> {
    /// The matching document IDs, sorted in ascending order. May contain
    /// duplicates (the geometry index does not guarantee uniqueness); they are
    /// yielded as-is.
    ids: OwnedSlice<DocId>,
    /// Position of the next document ID to return. `offset == ids.len()` means EOF.
    offset: usize,
    /// The last document ID returned by [`read`](Self::read) or
    /// [`skip_to`](Self::skip_to). Reset to 0 on [`rewind`](Self::rewind).
    last_doc_id: DocId,
    /// Reusable result object, avoiding an allocation on every `read`.
    result: RSIndexResult<'index>,
    /// Timeout source consulted while scanning.
    timeout_ctx: T,
    /// Field-expiration strategy.
    expiration_checker: E,
    /// Sink for this iterator's memory accounting (the geometry index's
    /// allocation tracker). Notified on construction and on drop.
    mem_tracker: M,
    /// The number of bytes this iterator reports to `mem_tracker`.
    /// Added on construction, subtracted on drop.
    tracked_bytes: usize,
}

impl<'index, T, E, M> GeoShape<'index, T, E, M>
where
    E: ExpirationChecker,
    M: MemTracker,
{
    /// Creates a new geometry-query iterator.
    ///
    /// `ids` is sorted in place on construction (the geometry index returns
    /// matches in spatial order).
    ///
    /// The byte size of this iterator is reported to `mem_tracker` immediately
    /// (via [`MemTracker::add`]) and reversed when the iterator is dropped (via
    /// [`MemTracker::sub`]). Pass [`NoTracker`] to opt out of accounting.
    pub fn new(
        ids: impl Into<OwnedSlice<DocId>>,
        timeout_ctx: T,
        expiration_checker: E,
        mem_tracker: M,
    ) -> Self {
        let result = RSIndexResult::build_virt()
            .field_mask(RS_FIELDMASK_ALL)
            .build();
        let mut ids = ids.into();
        // The geometry index yields matches in R-tree order; the query engine
        // expects ascending document IDs.
        ids.sort_unstable();

        let tracked_bytes = Self::mem_bytes(ids.len());

        mem_tracker.add(tracked_bytes);

        Self {
            ids,
            offset: 0,
            last_doc_id: 0,
            result,
            timeout_ctx,
            expiration_checker,
            mem_tracker,
            tracked_bytes,
        }
    }

    /// The total number of bytes accounted for an iterator holding `len`
    /// document IDs: the iterator struct itself plus the materialized id buffer.
    #[inline]
    const fn mem_bytes(len: usize) -> usize {
        std::mem::size_of::<Self>() + len * std::mem::size_of::<DocId>()
    }

    /// Returns the total memory, in bytes, held by this iterator.
    ///
    /// This is the same amount added to (and later subtracted from) the
    /// external memory tracker supplied at construction.
    #[inline]
    pub const fn mem_usage(&self) -> usize {
        self.tracked_bytes
    }

    /// Whether there are document IDs left to yield.
    #[inline(always)]
    fn has_next(&self) -> bool {
        self.offset < self.ids.len()
    }

    /// Examine the next candidate document, applying the field-expiration filter.
    #[inline]
    fn read_single(&mut self) -> SingleRead {
        let Some(&doc_id) = self.ids.get(self.offset) else {
            // No candidate to commit: keep `result` on the last valid doc so an
            // expired candidate probed on a previous iteration cannot leak via
            // `current` once the scan settles on EOF.
            self.result.doc_id = self.last_doc_id;
            return SingleRead::Eof;
        };
        self.offset += 1;

        // `is_expired` reads the candidate out of `result`, so it must be set
        // before the check.
        self.result.doc_id = doc_id;
        if self.expiration_checker.is_expired(&self.result) {
            // The candidate is expired and must never surface.
            return SingleRead::NotFound;
        }

        self.last_doc_id = doc_id;
        SingleRead::Ok
    }
}

/// Returns the index of the first ID in `ids` that is `>= doc_id`.
///
/// `ids` must be a non-empty, ascending slice whose last element is `>= doc_id`,
/// so a match is guaranteed to exist.
///
/// The search has two phases: an exponential ("galloping") probe that brackets
/// `doc_id` in a window growing as `1, 2, 4, …`, followed by a binary search
/// within that window. Two deliberate choices make this fast on the large id
/// buffers a geometry query produces:
///
/// * **Galloping, not a single bounded guess.** A sorted-id list with unique IDs
///   can bound the match to `doc_id - last_doc_id` entries ahead, but the
///   geometry index may yield *duplicate* IDs, so consecutive entries can repeat
///   and that bound is unsound. Galloping makes no uniqueness assumption: it
///   discovers a valid window by probing, and stays cache-friendly when the
///   match is near the cursor — the common case for forward skips.
/// * **A hand-rolled branchy search, not [`slice::partition_point`].** The
///   standard search is branchless; benchmarks show it consistently slower here,
///   both over the full (cold, multi-megabyte) tail — where its data-dependent
///   probes serialize cache misses the branchy form lets the CPU speculate past
///   — and even within the small galloped window, where its general setup
///   dominates.
///
/// Lower-bound semantics are preserved: when `doc_id` is duplicated, the index
/// of its *first* occurrence is returned.
#[inline]
fn lower_bound(ids: &[DocId], doc_id: DocId) -> usize {
    debug_assert!(!ids.is_empty(), "tail must be non-empty");
    debug_assert!(
        *ids.last().expect("non-empty tail") >= doc_id,
        "a match must exist within the tail"
    );

    let len = ids.len();

    // Exponential phase: grow `hi` until `ids[hi] >= doc_id` (or `hi` runs off
    // the end). Every doubling is guarded by a confirmed `ids[hi] < doc_id`, so
    // afterwards `ids[hi >> 1] < doc_id` whenever `hi >= 2`.
    let mut hi = 1;
    // SAFETY: the `hi < len` guard short-circuits before the index, so `hi` is
    // always in bounds when dereferenced.
    while hi < len && unsafe { *ids.get_unchecked(hi) } < doc_id {
        hi <<= 1;
    }

    // Binary search the bracketed, inclusive range `[bottom, top]`. `ids[top]`
    // is `>= doc_id` (the loop either stopped on it, or it is the clamped last
    // element, which the precondition guarantees is `>= doc_id`), so the answer
    // lies in the range.
    let mut bottom = hi >> 1;
    let mut top = hi.min(len - 1);
    while bottom < top {
        let mid = (bottom + top) >> 1;
        // SAFETY: `bottom <= mid < top <= len - 1`, so `mid` is in bounds.
        if unsafe { *ids.get_unchecked(mid) } < doc_id {
            bottom = mid + 1;
        } else {
            top = mid;
        }
    }
    bottom
}

impl<'index, T, E, M> RQEIterator<'index> for GeoShape<'index, T, E, M>
where
    T: TimeoutContext,
    E: ExpirationChecker,
    M: MemTracker,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // The timeout counter is amortized per `read` call: a fresh scan of
        // candidates is allowed before the next real clock probe.
        self.timeout_ctx.reset_counter();
        loop {
            self.timeout_ctx.check_timeout()?;
            match self.read_single() {
                SingleRead::Ok => break,
                SingleRead::NotFound => continue,
                SingleRead::Eof => return Ok(None),
            }
        }
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id < doc_id, "We're trying to skip backwards!");

        if !self.has_next() {
            return Ok(None);
        }

        // `has_next()` is true, so the list is non-empty.
        let last = *self.ids.last().expect("non-empty list");
        if doc_id > last {
            // Past the end: mark EOF without touching `last_doc_id`.
            self.offset = self.ids.len();
            return Ok(None);
        }

        // Find the first ID >= `doc_id` within the unread tail. The tail is
        // non-empty and its last element is >= `doc_id` (both guaranteed above),
        // so a match always exists.
        let tail = &self.ids[self.offset..];
        let idx = self.offset + lower_bound(tail, doc_id);
        let found = self.ids[idx];
        self.offset = idx + 1;

        self.result.doc_id = found;

        if self.expiration_checker.is_expired(&self.result) {
            // The matched entry's field has expired. Fall back to `read`, which
            // scans from `offset` — the entry right after this expired match —
            // past any run of expired entries to the next valid one, updating
            // `result`/`last_doc_id`, so the iterator never settles on an expired
            // doc. This mirrors the inverted-index iterators' `skip_to` contract.
            return match self.read()? {
                // A valid entry beyond `doc_id` was found; the target itself did
                // not match, so report it as `NotFound`.
                Some(result) => Ok(Some(SkipToOutcome::NotFound(result))),
                // No valid entry left: EOF.
                None => Ok(None),
            };
        }

        self.last_doc_id = found;

        if found == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    fn rewind(&mut self) {
        self.offset = 0;
        self.last_doc_id = 0;
        self.result.doc_id = 0;
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.ids.len()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        !self.has_next()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::GeoShape
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<T, E, M: MemTracker> ProfilePrint for GeoShape<'_, T, E, M> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"GEO-SHAPE", map);
    }
}

impl<T, E, M: MemTracker> Drop for GeoShape<'_, T, E, M> {
    fn drop(&mut self) {
        self.mem_tracker.sub(self.tracked_bytes);
    }
}

impl<'index, T, E, M> crate::RQEIteratorBoxed<'index> for GeoShape<'index, T, E, M>
where
    T: TimeoutContext + 'static,
    E: ExpirationChecker + 'static,
    M: MemTracker + 'static,
{
    /// `GeoShape` owns its result data (the doc-id list is materialized
    /// up front), so it has no `Rf`-dependent state to drop on suspend;
    /// the Suspended counterpart is the same type.
    type Suspended = GeoShape<'static, T, E, M>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // SAFETY: `GeoShape<'index, T, E, M>` and
        // `GeoShape<'static, T, E, M>` are layout-identical (`'index`
        // is phantom: the only borrow it constrains is `result`, an
        // `RSIndexResult<'index>`, whose `'index`-dependent state lives
        // entirely behind raw pointers — see [`RSIndexResult`]). The
        // round-trip identity is established by [`RQESuspendedIterator::resume`].
        let raw = Box::into_raw(self);
        unsafe { Box::from_raw(raw as *mut GeoShape<'static, T, E, M>) }
    }
}

impl<T, E, M> crate::RQESuspendedIterator for GeoShape<'static, T, E, M>
where
    T: TimeoutContext + 'static,
    E: ExpirationChecker + 'static,
    M: MemTracker + 'static,
{
    type Resumed<'a> = GeoShape<'a, T, E, M>;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &'a index_spec::IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // SAFETY: see `suspend` — `'static` ↔ `'a` is purely phantom.
        let raw = Box::into_raw(self);
        let active = unsafe { Box::from_raw(raw as *mut GeoShape<'a, T, E, M>) };
        (active, ValidateStatus_VALIDATE_OK)
    }

    fn last_doc_id(&self) -> ffi::t_docId {
        self.last_doc_id
    }
}
