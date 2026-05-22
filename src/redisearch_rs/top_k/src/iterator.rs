/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TopKIterator`] — the generic top-k state machine.

use std::marker::PhantomData;
use std::num::NonZeroUsize;

use ffi::{ValidateStatus, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK, t_docId};
use index_result::{RSIndexResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_iterator_type::IteratorType;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQESuspendedIterator, SkipToOutcome};

use crate::traits::{ScoreBatch, ScoreSource};

/// Determines which collection algorithm [`TopKIterator`] uses.
///
/// Selected at construction based on whether a child filter is present,
/// and may be switched mid-execution when the source decides a different
/// strategy is more efficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TopKMode {
    /// No child filter — stream directly from the source's single batch.
    /// The heap is bypassed entirely.
    ///
    /// # Invariants
    ///
    /// The [`ScoreSource`] used with this mode **must** produce at most one
    /// batch (i.e. the first [`ScoreSource::next_batch`] call returns the
    /// complete result set, and a second call would return `Ok(None)`).
    /// Any additional batches are not consumed and their results are silently
    /// lost.  In debug builds, [`TopKIterator`] asserts this invariant.
    Unfiltered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Phase {
    /// Collection has not started; the first call to [`read`](TopKIterator::read)
    /// will trigger it.
    NotStarted,
    /// Actively collecting results (transient; only visible inside collection methods).
    Collecting,
    /// Collection is done; yielding results from `results` in order.
    #[expect(
        dead_code,
        reason = "TopKMode::Unfiltered is without child so no yielding necessary"
    )]
    Yielding,
    /// Unfiltered path: yielding directly from `direct_batch` without a heap.
    YieldingDirect,
}

/// A generic top-k iterator parameterized over a [`ScoreSource`] and a child
/// [`RQEIterator`].
///
/// Parameterised over a [`Ref`] mode — see [`TopKIterator`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// # Layout
///
/// The struct is `#[repr(C)]` so that [`Active`] and [`Suspended`]
/// instantiations are layout-compatible: `result: RawIndexResult<Rf>` differs
/// only in the validity of internal [`SharedPtr`](ref_mode::SharedPtr) fields
/// (transparent over `Rf`), and the child field varies via `I` vs
/// `I::Suspended` (layout-compatible by the [`RQEIterator`] contract).
/// `S` is identical across modes because [`ScoreSource`] is unparameterized.
#[repr(C)]
pub struct RawTopK<'index, Rf: Ref, S, I>
where
    S: ScoreSource,
{
    source: S,
    child: Option<I>,
    mode: TopKMode,
    /// Preserved so [`rewind`](RQEIterator::rewind) can restore the original mode.
    initial_mode: TopKMode,
    /// Holds the in-progress batch for the Unfiltered path.
    ///
    /// Dropped on [`suspend`](RQEIterator::suspend) and re-acquired on the
    /// next [`read`](RQEIterator::read) after [`resume`](RQESuspendedIterator::resume),
    /// since the batch cursor may carry references that are only valid while
    /// the spec read lock is held.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    phase: Phase,
    /// Currently-held result. Valid iff `has_current` is `true`.
    result: RawIndexResult<Rf>,
    /// Discriminant for `result`: matches the legacy `current: Option<…>`
    /// shape — `false` before the first read and after EOF.
    has_current: bool,
    last_doc_id: t_docId,
    at_eof: bool,
    _marker: PhantomData<&'index ()>,
}

/// Alias for an [`Active`] [`RawTopK`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type TopKIterator<'index, S, I> = RawTopK<'index, Active<'index>, S, I>;

impl<'index, S, I> TopKIterator<'index, S, I>
where
    S: ScoreSource + 'index,
    I: RQEIterator<'index>,
{
    /// Create a new [`TopKIterator`].
    ///
    /// The execution mode is always Unfiltered.
    ///
    /// For now, only [`TopKMode::Unfiltered`] exists.
    pub fn new(source: S, child: Option<I>, k: NonZeroUsize) -> Self {
        let mode = TopKMode::Unfiltered;
        Self::new_with_mode(source, child, k, mode)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(source: S, child: Option<I>, k: NonZeroUsize, mode: TopKMode) -> Self {
        Self {
            source,
            child,
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            phase: Phase::NotStarted,
            result: RawIndexResult::build_virt().build(),
            has_current: false,
            last_doc_id: 0,
            at_eof: false,
            _marker: PhantomData,
        }
    }

    /// Drive collection based on the current mode.
    fn collect(&mut self) -> Result<(), RQEIteratorError> {
        self.phase = Phase::Collecting;
        let result = match self.mode {
            TopKMode::Unfiltered => self.prepare_unfiltered_direct(),
        };
        if result.is_err() {
            // Reset so a retry via read() works: Phase::Collecting has no handler there.
            // TODO: MOD-14209: bubble up errors
            self.phase = Phase::NotStarted;
        }
        result
    }

    /// Set up the unfiltered direct-yield path.
    ///
    /// Calls [`ScoreSource::next_batch`] exactly once.  Results are streamed
    /// directly from the batch cursor — no heap is involved.
    ///
    /// # Invariants
    ///
    /// [`TopKMode::Unfiltered`] requires the source to produce at most one
    /// batch.  In debug builds this method calls [`ScoreSource::next_batch`] a
    /// second time and panics if another batch is returned, catching
    /// misbehaving implementations early.
    fn prepare_unfiltered_direct(&mut self) -> Result<(), RQEIteratorError> {
        self.direct_batch = self.source.next_batch()?;
        if self.direct_batch.is_none() {
            self.at_eof = true;
        }
        debug_assert!(
            matches!(self.source.next_batch(), Ok(None)),
            "ScoreSource did not return Ok(None) in TopKMode::Unfiltered \
             (extra batch or error); use a batched mode instead"
        );
        self.phase = Phase::YieldingDirect;
        Ok(())
    }

    /// Yield the next result from the unfiltered direct batch.
    fn advance_unfiltered_direct(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let item = self.direct_batch.as_mut().and_then(S::Batch::next);

        match item {
            Some((doc_id, score)) => {
                self.result = self.source.build_result(doc_id, score);
                self.has_current = true;
                self.last_doc_id = doc_id;
                Ok(Some(&mut self.result))
            }
            None => {
                self.at_eof = true;
                self.has_current = false;
                Ok(None)
            }
        }
    }
}

impl<'index, S, I> RQEIterator<'index> for TopKIterator<'index, S, I>
where
    S: ScoreSource + 'static,
    I: RQEIterator<'index>,
{
    type Suspended = RawTopK<'static, Suspended, S, I::Suspended>;

    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.has_current {
            Some(&mut self.result)
        } else {
            None
        }
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof {
            return Ok(None);
        }

        if self.phase == Phase::NotStarted {
            self.collect()?;
        }

        match self.phase {
            Phase::YieldingDirect => self.advance_unfiltered_direct(),
            Phase::Yielding => {
                unreachable!("`Phase::Yielding` is dead code (planned for batches - MOD-14203).")
            }
            Phase::NotStarted | Phase::Collecting => {
                unreachable!("collect() must set phase to YieldingDirect or Yielding")
            }
        }
    }

    fn skip_to(
        &mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // TopKIterator is a root-only iterator that yields results sorted by
        // score, not by doc_id.  It cannot be used as a child in a larger
        // iterator tree, so skip_to is unsupported.
        unimplemented!("TopKIterator is a root-only iterator; skip_to is not supported")
    }

    fn suspend(mut self: Box<Self>) -> Box<Self::Suspended> {
        // Drop the in-flight batch and reset phase BEFORE the type cast.
        // A `ScoreSource::Batch` may borrow from the source's live state; any
        // such borrow is invalid once the spec read lock is released. We pay
        // for a fresh `collect()` on resume — see [`prepare_unfiltered_direct`].
        self.direct_batch = None;
        self.phase = Phase::NotStarted;

        let raw = Box::into_raw(self);
        // SAFETY: `RawTopK` is `#[repr(C)]`. The only `Rf`-dependent field is
        // `result: RawIndexResult<Rf>`, layout-compatible across `Rf` via
        // [`SharedPtr`](ref_mode::SharedPtr) transparency. `Option<I>` and
        // `Option<I::Suspended>` are layout-compatible by the
        // [`RQEIterator`] contract. `S` is unchanged because
        // [`ScoreSource`] is unparameterized over `Rf`. `Box::from_raw` reuses
        // the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawTopK<'static, Suspended, S, I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        if let Some(child) = &mut self.child {
            child.cascade_suspend();
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.source.rewind();
        if let Some(child) = &mut self.child {
            child.rewind();
        }
        self.mode = self.initial_mode;
        self.direct_batch = None;
        self.has_current = false;
        self.last_doc_id = 0;
        self.at_eof = false;
        self.phase = Phase::NotStarted;
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.k.get().min(self.source.num_estimated())
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.at_eof
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.source.iterator_type()
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

impl<S, IS> RQESuspendedIterator for RawTopK<'static, Suspended, S, IS>
where
    S: ScoreSource + 'static,
    IS: RQESuspendedIterator,
{
    type Resumed<'a> = RawTopK<'a, Active<'a>, S, IS::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let RawTopK {
            source,
            child,
            mode,
            initial_mode,
            direct_batch,
            k,
            phase,
            result,
            has_current,
            last_doc_id,
            at_eof,
            _marker,
        } = *self;

        // `suspend` always resets these — assert as an internal invariant.
        debug_assert!(direct_batch.is_none(), "direct_batch must be dropped on suspend");
        debug_assert!(matches!(phase, Phase::NotStarted), "phase must be NotStarted on suspend");

        // Resume the child first (if present) so we never construct the
        // active iterator with a still-suspended `child` field.
        let (child, child_status) = match child {
            None => (None, ValidateStatus_VALIDATE_OK),
            Some(c) => {
                let (active_child, status) = Box::new(c).resume(guard);
                (Some(*active_child), status)
            }
        };

        // SAFETY: `result` was last populated via `ScoreSource::build_result`
        // (owned/virtual), which carries no `'index`-bound borrows. Re-typing
        // the held result as `Active<'a>` is therefore unconditionally sound;
        // the lifetime is purely advisory at this point.
        let result = unsafe { result.into_active::<'a>() };

        let active = Box::new(RawTopK {
            source,
            child,
            mode,
            initial_mode,
            direct_batch: None,
            k,
            phase: Phase::NotStarted,
            result,
            has_current,
            last_doc_id,
            at_eof,
            _marker: PhantomData,
        });

        // If we had a yielded result before suspend, the iterator's position
        // can only "move" because the batch is regenerated on the next read.
        // Surface that as MOVED so callers re-query `current()` / `read()`.
        let status = if has_current || last_doc_id > 0 {
            child_status.max(ValidateStatus_VALIDATE_MOVED)
        } else {
            child_status
        };
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        self.k.get().min(self.source.num_estimated())
    }
}
