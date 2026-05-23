/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`NotOptimized`].

use std::time::Duration;

use ffi::{
    RS_FIELDMASK_ALL, ValidateStatus, ValidateStatus_VALIDATE_ABORTED,
    ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK, t_docId,
};
use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    SkipToOutcome, WildcardIterator, maybe_empty::MaybeEmpty, not::NotIterator,
    utils::TimeoutContext,
};
use index_spec::IndexSpecReadGuard;

/// Check the clock every this many loop iterations to amortize syscall cost.
const TIMEOUT_CHECK_GRANULARITY: u32 = 5_000;

/// An optimized NOT iterator that uses a wildcard inverted index iterator.
///
/// Parameterised over a [`Ref`] mode — see [`NotOptimized`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Unlike [`Not`](super::not::Not) which iterates sequentially from 1 to
/// `max_doc_id`, this variant uses a
/// [wildcard iterator](crate::wildcard) that reads from the existing-documents inverted
/// index. It yields all documents present in the wildcard iterator that
/// are **not** present in the child iterator.
///
/// This is applicable when the index has an `existingDocs` inverted index
/// (i.e. `index_all` is enabled), providing better performance by only
/// visiting documents that actually exist.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode.
/// * `W` - The wildcard iterator type, must implement [`WildcardIterator`].
/// * `I` - The child iterator type whose results are negated.
#[repr(C)]
pub struct RawNotOptimized<Rf: Ref, W, I> {
    /// The wildcard iterator over all existing documents.
    wcii: W,
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID (used as upper bound guard).
    max_doc_id: t_docId,
    /// Sticky EOF flag, set when iteration completes.
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RawIndexResult<Rf>,
    /// Tracks the execution deadline for this iterator.
    timeout_ctx: Option<TimeoutContext>,
}

/// Alias for an [`Active`] [`RawNotOptimized`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type NotOptimized<'index, W, I> = RawNotOptimized<Active<'index>, W, I>;

impl<Rf: Ref, W, I> RawNotOptimized<Rf, W, I> {
    /// Get a shared reference to the _child_ iterator. Mode-independent.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }
}

impl<'index, W, I> NotOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Create a new optimized NOT iterator.
    ///
    /// `wcii` is the wildcard iterator over all existing documents.
    /// `child` is the iterator whose documents will be excluded.
    /// `max_doc_id` is the upper bound for document IDs.
    /// `weight` is the score weight applied to every returned result.
    /// `timeout` controls the amortized timeout. Pass [`None`] to skip timeout checks.
    pub fn new(
        wcii: W,
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            wcii,
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            timeout_ctx: timeout.map(|t| TimeoutContext::new(t, TIMEOUT_CHECK_GRANULARITY, false)),
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`].
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        if let Some(ctx) = self.timeout_ctx.as_mut() {
            ctx.check_timeout()
        } else {
            Ok(())
        }
    }

    /// Advance the wildcard iterator and set [`forced_eof`](Self::forced_eof)
    /// if it is exhausted.
    ///
    /// Returns `Ok(true)` if the wildcard iterator produced a new document,
    /// `Ok(false)` if it reached EOF.
    #[inline(always)]
    fn advance_wcii_or_eof(&mut self) -> Result<bool, RQEIteratorError> {
        if self.wcii.read()?.is_none() {
            self.forced_eof = true;
            return Ok(false);
        }
        Ok(true)
    }

    /// Check whether the child iterator is positionally past `doc_id`
    /// (already advanced beyond it) or fully exhausted, meaning `doc_id`
    /// cannot be in the child without performing additional reads.
    #[inline(always)]
    fn child_is_ahead_or_depleted(&self, doc_id: t_docId) -> bool {
        doc_id < self.child.last_doc_id()
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
    }

    /// Internal read logic shared by [`read`](RQEIterator::read) and
    /// [`skip_to`](RQEIterator::skip_to).
    ///
    /// Returns `Ok(true)` if a valid result was found (stored in
    /// `self.result.doc_id`), `Ok(false)` if EOF was reached.
    fn read_inner(&mut self) -> Result<bool, RQEIteratorError> {
        if self.at_eof() {
            self.forced_eof = true;
            return Ok(false);
        }

        // Advance the wildcard iterator to the next document.
        // We check the return value (not `at_eof`) because iterators
        // may report `at_eof() == true` immediately after returning the last
        // element, while the returned value is still valid.
        if !self.advance_wcii_or_eof()? {
            return Ok(false);
        }

        loop {
            let wcii_last = self.wcii.last_doc_id();

            if self.child_is_ahead_or_depleted(wcii_last) {
                // Case 1: The wildcard document is not in the child.
                self.result.doc_id = wcii_last;
                return Ok(true);
            } else if wcii_last == self.child.last_doc_id() {
                // Case 2: Both iterators at the same position, advance both.
                self.child.read()?;
                if !self.advance_wcii_or_eof()? {
                    return Ok(false);
                }
            } else {
                // Case 3: Child is behind, read it forward to catch up.
                //
                // We use a read loop rather than `skip_to` because the
                // child almost always needs only a single read to reach
                // or pass `wcii_last`. The only scenario where the child
                // lags behind is when GC has removed a doc ID from the
                // wildcard inverted index but not yet from the child's
                // index — and even then the gap is typically tiny.
                // `read` is cheaper than `skip_to`, so the loop is
                // faster in the common case.
                while !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
                    self.child.read()?;
                }
            }
            self.check_timeout()?;
        }
    }
}

impl<'index, W, I> RQEIterator<'index> for NotOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.read_inner()? {
            Ok(Some(&mut self.result))
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }
        if doc_id > self.max_doc_id {
            self.forced_eof = true;
            return Ok(None);
        }

        // Skip wcii to docId.
        if self.wcii.skip_to(doc_id)?.is_none() {
            self.forced_eof = true;
            return Ok(None);
        }

        let wcii_last = self.wcii.last_doc_id();

        // If child is behind wcii, advance it to catch up.
        if !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
            self.child.skip_to(wcii_last)?;
        }

        // If child landed at the same position, the document is in the
        // child. Advance to find the next valid NOT result.
        if self.child.last_doc_id() == wcii_last {
            if self.read_inner()? {
                return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
            } else {
                return Ok(None);
            }
        }

        // Child is ahead or depleted: wcii_last is a valid result.
        self.result.doc_id = wcii_last;
        if self.result.doc_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.result.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.forced_eof || self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::NotOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W, I> RQEIteratorBoxed<'index> for NotOptimized<'index, W, I>
where
    W: WildcardIterator<'index> + RQEIteratorBoxed<'index>,
    for<'a> <W::Suspended as RQESuspendedIterator>::Resumed<'a>:
        WildcardIterator<'a> + RQEIteratorBoxed<'a, Suspended = W::Suspended>,
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawNotOptimized<Suspended, W::Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawNotOptimized` is `#[repr(C)]`. The only `Rf`-dependent
        // field is `result: RawIndexResult<Rf>` (layout-compatible via
        // `SharedPtr` transparency). `W`/`I` are layout-compatible with
        // `W::Suspended`/`I::Suspended` by the [`RQEIteratorBoxed`]
        // contract; `MaybeEmpty<I>` likewise (see [`MaybeEmpty::suspend`]).
        // Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawNotOptimized<Suspended, W::Suspended, I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        self.wcii.cascade_suspend();
        self.child.cascade_suspend();
    }
}

impl<WS, IS> RQESuspendedIterator for RawNotOptimized<Suspended, WS, IS>
where
    WS: RQESuspendedIterator,
    for<'a> WS::Resumed<'a>: WildcardIterator<'a> + RQEIteratorBoxed<'a, Suspended = WS>,
    IS: RQESuspendedIterator,
{
    type Resumed<'a> = NotOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let RawNotOptimized {
            wcii,
            child,
            max_doc_id,
            forced_eof,
            result,
            timeout_ctx,
        } = *self;

        let (wcii, wcii_status) = Box::new(wcii).resume(guard);
        let (child, child_status) = Box::new(child).resume(guard);

        let child = if child_status == ValidateStatus_VALIDATE_ABORTED {
            MaybeEmpty::new_empty()
        } else {
            *child
        };

        if wcii_status == ValidateStatus_VALIDATE_ABORTED {
            // SAFETY: `NotOptimized`'s `result` is a virtual sentinel built
            // via `build_virt()` — no aliased pointers to validate. The
            // `Active<'a>` re-typing is unconditionally sound.
            let result = unsafe { result.into_active::<'a>() };
            let active = Box::new(NotOptimized {
                wcii: *wcii,
                child,
                max_doc_id,
                forced_eof,
                result,
                timeout_ctx,
            });
            return (active, ValidateStatus_VALIDATE_ABORTED);
        }

        let _ = child_status;

        // SAFETY: see above.
        let result = unsafe { result.into_active::<'a>() };

        let mut active = Box::new(NotOptimized {
            wcii: *wcii,
            child,
            max_doc_id,
            forced_eof,
            result,
            timeout_ctx,
        });

        #[expect(non_upper_case_globals, reason = "bindgen-generated constants")]
        let status = match wcii_status {
            ValidateStatus_VALIDATE_MOVED => {
                active.forced_eof = active.wcii.at_eof();
                let mut have_valid_pos = !active.forced_eof;
                if have_valid_pos {
                    active.result.doc_id = active.wcii.last_doc_id();
                    if active.child.last_doc_id() < active.result.doc_id {
                        let _ = active.child.skip_to(active.result.doc_id);
                    }
                    if active.child.last_doc_id() == active.result.doc_id {
                        if let Ok(found) = active.read_inner() {
                            have_valid_pos = found;
                        } else {
                            active.forced_eof = false;
                            have_valid_pos = false;
                        }
                    }
                }
                let _ = have_valid_pos;
                ValidateStatus_VALIDATE_MOVED
            }
            _ => ValidateStatus_VALIDATE_OK,
        };

        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }
}

impl<'index, W> NotIterator<'index>
    for NotOptimized<'index, W, Box<dyn RQEIterator<'index> + 'index>>
where
    W: crate::WildcardIterator<'index>,
{
    fn child(&self) -> Option<&dyn RQEIterator<'index>> {
        NotOptimized::child(self).map(|c| &**c as &dyn RQEIterator<'index>)
    }
}

impl<'index, W> crate::interop::ProfileChildren<'index>
    for NotOptimized<'index, W, crate::c2rust::CRQEIterator>
where
    W: crate::WildcardIterator<'index> + crate::RQEIteratorBoxed<'index> + 'index,
    for<'a> <W::Suspended as RQESuspendedIterator>::Resumed<'a>:
        crate::WildcardIterator<'a> + crate::RQEIteratorBoxed<'a, Suspended = W::Suspended>,
{
    fn profile_children(self) -> Self {
        NotOptimized {
            wcii: self.wcii,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            max_doc_id: self.max_doc_id,
            forced_eof: self.forced_eof,
            result: self.result,
            timeout_ctx: self.timeout_ctx,
        }
    }
}
