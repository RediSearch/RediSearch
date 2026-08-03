/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`OptionalOptimized`].
//!
//! This is the optimized variant of the optional iterator. Instead of scanning
//! all doc IDs from 1 to `maxDocId`, it uses a [wildcard iterator](crate::wildcard) over
//! `spec.existingDocs` to visit only real document IDs, yielding real or virtual
//! results accordingly.

use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref};

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    maybe_empty::MaybeEmpty,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    wildcard::WildcardIterator,
};

use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
/// An iterator that emits results for all document IDs present in the index,
/// driven by a [wildcard iterator](crate::wildcard) over the existing-documents inverted index.
///
/// Parameterised over a [`Ref`] mode — see [`OptionalOptimized`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
///
/// For each doc ID that `wcii` yields:
/// - If the query child also has a hit at that doc ID, a **real** result is
///   returned with [`OptionalOptimized::weight`] applied.
/// - Otherwise a **virtual** result is returned with zero weight.
///
/// This avoids scanning doc IDs 1..=maxDocId sequentially. When the index is
/// sparse (few documents relative to `maxDocId`), the optimized variant is
/// significantly faster.
#[repr(C)]
pub struct RawOptionalOptimized<'query, Rf: Ref, W, I> {
    /// Wildcard iterator over `spec.existingDocs` — the authoritative source of doc IDs.
    wcii: W,
    /// Query child — provides real hits at positions where it has a match.
    /// Wrapped in [`MaybeEmpty`] so it can be replaced with an empty iterator
    /// when it is aborted during [`RQEIterator::revalidate`].
    child: MaybeEmpty<I>,
    /// Virtual result returned when `wcii` has a doc but `child` does not.
    virt: RawIndexResult<'query, Rf>,
    /// Inclusive upper bound (matches C `maxDocId`).
    max_doc_id: DocId,
    /// Weight applied to real results from `child`.
    weight: f64,
    /// Tracks the doc ID of the last result yielded.
    ///
    /// `0` in the initial state and after [`rewind`](RQEIterator::rewind),
    /// which is treated as virtual. Doc IDs start from 1, so 0 is a safe sentinel.
    last_doc_id: DocId,
    /// Whether the iterator has run *past* its last result, i.e. a
    /// `read`/`skip_to` found nothing, or a revalidation landed past the end.
    ///
    /// This is the state behind both [`current`](RQEIterator::current), which
    /// reports no current once it is set, and [`at_eof`](RQEIterator::at_eof),
    /// its negation.
    ///
    /// Distinct from [`Self::reached_max`], the look-ahead: on reaching
    /// `max_doc_id` the next read will find nothing, but that final result is
    /// still in hand, so this flag stays clear until the iterator has moved past
    /// it. Only [`rewind`](RQEIterator::rewind) clears it.
    past_end: bool,
}

/// Alias for an [`Active`] [`RawOptionalOptimized`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type OptionalOptimized<'index, W, I> = RawOptionalOptimized<'index, Active<'index>, W, I>;

impl<'index, W, I> OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Returns a reference to the child iterator, if any.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Sets the child iterator.
    pub fn set_child(&mut self, child: I) {
        self.child = MaybeEmpty::new(child);
    }

    /// Creates a new [`OptionalOptimized`] iterator.
    ///
    /// * `wcii` — wildcard iterator over `spec.existingDocs`; drives which doc IDs
    ///   are visited.
    /// * `child` — query child iterator that provides real hits.
    /// * `max_doc_id` — inclusive upper bound on doc IDs.
    /// * `weight` — applied to results produced by `child`.
    pub fn new(wcii: W, child: I, max_doc_id: DocId, weight: f64) -> Self {
        Self {
            wcii,
            child: MaybeEmpty::new(child),
            virt: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            max_doc_id,
            weight,
            last_doc_id: 0,
            past_end: false,
        }
    }

    /// Whether the iterator has yielded `max_doc_id`, so the next `read` will
    /// find nothing.
    ///
    /// The look-ahead, used to terminate `read`. It is true one step before
    /// [`Self::past_end`] — while `max_doc_id` is still the current result — so
    /// it must not be confused with [`at_eof`](RQEIterator::at_eof).
    #[inline(always)]
    const fn reached_max(&self) -> bool {
        self.last_doc_id >= self.max_doc_id
    }
}

impl<'index, W, I> RQEIterator<'index> for OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.past_end {
            return None;
        }

        if self.last_doc_id != 0
            && self.child.last_doc_id() == self.last_doc_id
            && let Some(result) = self.child.current()
        {
            return Some(result);
        }

        Some(&mut self.virt)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // The look-ahead, not `at_eof()`: having yielded `max_doc_id` means the
        // next read finds nothing, and this is the read that records it.
        if self.past_end || self.reached_max() {
            self.past_end = true;
            return Ok(None);
        }

        // Advance wcii to the next existing document.
        let wcii_doc_id = match self.wcii.read()? {
            None => {
                self.past_end = true;
                return Ok(None);
            }
            Some(r) => r.doc_id,
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if wcii_doc_id > self.max_doc_id {
            self.past_end = true;
            return Ok(None);
        }

        // Advance child to catch up with wcii.
        if wcii_doc_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(wcii_doc_id)?;
        }

        self.last_doc_id = wcii_doc_id;

        let weight = self.weight;
        if self.child.last_doc_id() == wcii_doc_id {
            // Real hit: child has a result at this position.
            let result = self
                .child
                .current()
                .expect("child has a result at wcii_doc_id");
            result.weight = weight;
            Ok(Some(result))
        } else {
            // Virtual hit: wcii has a doc ID but child does not.
            self.virt.doc_id = wcii_doc_id;
            Ok(Some(&mut self.virt))
        }
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.last_doc_id);

        // `doc_id > self.last_doc_id` is asserted above, so a target beyond
        // `max_doc_id` also covers the `reached_max()` case.
        if doc_id > self.max_doc_id || self.past_end {
            self.past_end = true;
            return Ok(None);
        }

        // Promote wcii to doc_id. It may land on a different doc if doc_id is not
        // present in the existing-documents index.
        let (found, effective_id) = match self.wcii.skip_to(doc_id)? {
            None => {
                self.past_end = true;
                return Ok(None);
            }
            Some(SkipToOutcome::Found(r)) => (true, r.doc_id),
            Some(SkipToOutcome::NotFound(r)) => (false, r.doc_id),
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if effective_id > self.max_doc_id {
            self.past_end = true;
            return Ok(None);
        }

        // Advance child to effective_id if needed.
        if effective_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(effective_id)?;
        }

        self.last_doc_id = effective_id;

        let weight = self.weight;
        if self.child.last_doc_id() == effective_id {
            // Real hit — outcome (Found/NotFound) mirrors wcii.
            let result = self
                .child
                .current()
                .expect("child has a result at effective_id");
            result.weight = weight;
            if found {
                Ok(Some(SkipToOutcome::Found(result)))
            } else {
                Ok(Some(SkipToOutcome::NotFound(result)))
            }
        } else {
            // Virtual hit — outcome (Found/NotFound) mirrors wcii.
            self.virt.doc_id = effective_id;
            if found {
                Ok(Some(SkipToOutcome::Found(&mut self.virt)))
            } else {
                Ok(Some(SkipToOutcome::NotFound(&mut self.virt)))
            }
        }
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Simple enum to avoid holding a borrow through the match.
        enum ValidateOutcome {
            Ok,
            Moved,
        }

        // Step 1: Revalidate wcii. If it aborts or is at EOF, we can return immediately.
        let wcii_outcome = match self.wcii.revalidate(spec)? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { current: Some(_) } => ValidateOutcome::Moved,
            RQEValidateStatus::Moved { current: None } => {
                self.past_end = true;
                return Ok(RQEValidateStatus::Moved { current: None });
            }
            RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
        };
        // A wildcard that has run past its end means we have too. Monotonic on
        // purpose: an iterator that already returned `None` must not be revived by
        // a wildcard that still has documents beyond `max_doc_id` — `rewind` is
        // the way to restart one.
        self.past_end |= self.wcii.at_eof();

        // `last_doc_id` is `None` in the initial/rewound state, which is always
        // virtual.
        let current_was_virtual =
            self.last_doc_id == 0 || self.child.last_doc_id() != self.last_doc_id;

        // Step 2: Revalidate child. If it aborts, replace with an empty iterator.
        // Abort is treated as Moved: child's state changed, so we must re-evaluate.
        let child_outcome = match self.child.revalidate(spec)? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { .. } => ValidateOutcome::Moved,
            RQEValidateStatus::Aborted => {
                let _ = self.child.take_iterator(); // replace with Empty
                ValidateOutcome::Moved
            }
        };

        // Step 3: Determine the outcome based on wcii's and child's status.
        match wcii_outcome {
            ValidateOutcome::Ok => {
                if matches!(child_outcome, ValidateOutcome::Ok) || current_was_virtual {
                    // Child is still valid, or the current result was virtual — no change.
                    return Ok(RQEValidateStatus::Ok);
                }
                // Child moved or aborted while current was a real result.
                // Advance to the next valid state.
                let current = self.read()?;
                Ok(RQEValidateStatus::Moved { current })
            }
            ValidateOutcome::Moved => {
                // wcii moved to a new valid position; update child accordingly.
                let wcii_doc_id = self.wcii.last_doc_id();

                // wcii may have moved past max_doc_id.
                if wcii_doc_id > self.max_doc_id {
                    self.past_end = true;
                    return Ok(RQEValidateStatus::Moved { current: None });
                }

                if wcii_doc_id > self.child.last_doc_id() {
                    let _ = self.child.skip_to(wcii_doc_id)?;
                }

                // Landing *on* `max_doc_id` is a live position handed back below as
                // `Moved { current: Some }`, so nothing is recorded here.
                self.last_doc_id = wcii_doc_id;

                let weight = self.weight;
                if self.child.last_doc_id() == wcii_doc_id {
                    // Real hit at the new wcii position.
                    let result = self
                        .child
                        .current()
                        .expect("child has a result at wcii_doc_id");
                    result.weight = weight;
                    Ok(RQEValidateStatus::Moved {
                        current: Some(result),
                    })
                } else {
                    // Virtual hit at the new wcii position.
                    self.virt.doc_id = wcii_doc_id;
                    Ok(RQEValidateStatus::Moved {
                        current: Some(&mut self.virt),
                    })
                }
            }
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.past_end = false;
        self.virt.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.past_end
    }

    fn type_(&self) -> crate::IteratorType {
        crate::IteratorType::OptionalOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W: WildcardIterator<'index> + 'index> crate::interop::ProfileChildren<'index>
    for OptionalOptimized<'index, W, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        OptionalOptimized {
            max_doc_id: self.max_doc_id,
            weight: self.weight,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            wcii: self.wcii,
            virt: self.virt,
            last_doc_id: self.last_doc_id,
            past_end: self.past_end,
        }
    }
}

impl<'index, W, I> ProfilePrint for OptionalOptimized<'index, W, I>
where
    W: crate::WildcardIterator<'index>,
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"OPTIONAL", self.child(), map);
    }
}
