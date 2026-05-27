/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TopKIterator`] — the generic top-k state machine.

use std::num::NonZeroUsize;

use ffi::t_docId;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

use crate::traits::{ScoreBatch, ScoreSource};

/// Determines which collection algorithm [`TopKIterator`] uses.
///
/// Selected at construction based on whether a child filter is present,
/// and may be switched mid-execution when the source decides a different
/// strategy is more efficient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// A generic top-k iterator parameterized over a [`ScoreSource`].
///
/// Implements [`Unfiltered`](TopKMode::Unfiltered).
pub struct TopKIterator<'index, S: ScoreSource> {
    source: S,
    child: Option<Box<dyn RQEIterator<'index> + 'index>>,
    mode: TopKMode,
    /// Preserved so [`rewind`](Self::rewind) can restore the original mode.
    initial_mode: TopKMode,
    /// Holds the in-progress batch for the Unfiltered path.
    direct_batch: Option<S::Batch>,
    k: NonZeroUsize,
    phase: Phase,
    current: Option<RSIndexResult<'index>>,
    last_doc_id: t_docId,
    at_eof: bool,
}

impl<'index, S: ScoreSource + 'index> TopKIterator<'index, S> {
    /// Create a new [`TopKIterator`].
    ///
    /// The execution mode is always Unfiltered.
    ///
    /// For now, only [`TopKMode::Unfiltered`] exists.
    pub fn new(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
    ) -> Self {
        let mode = TopKMode::Unfiltered;
        Self::new_with_mode(source, child, k, mode)
    }

    /// Create a new [`TopKIterator`] with an explicit initial mode.
    pub fn new_with_mode(
        source: S,
        child: Option<Box<dyn RQEIterator<'index> + 'index>>,
        k: NonZeroUsize,
        mode: TopKMode,
    ) -> Self {
        Self {
            source,
            child,
            mode,
            initial_mode: mode,
            direct_batch: None,
            k,
            phase: Phase::NotStarted,
            current: None,
            last_doc_id: 0,
            at_eof: false,
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
                let result = self.source.build_result(doc_id, score);
                self.current = Some(result);
                self.last_doc_id = doc_id;
                Ok(self.current.as_mut())
            }
            None => {
                self.at_eof = true;
                self.current = None;
                Ok(None)
            }
        }
    }
}

impl<'index, S: ScoreSource + 'index> RQEIterator<'index> for TopKIterator<'index, S> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.current.as_mut()
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

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if let Some(child) = &mut self.child {
            return child.revalidate(spec);
        }
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.source.rewind();
        if let Some(child) = &mut self.child {
            child.rewind();
        }
        self.mode = self.initial_mode;
        self.direct_batch = None;
        self.current = None;
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
