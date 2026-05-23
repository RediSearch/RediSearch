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
use ref_mode::{Active, Ref, Suspended};

use crate::{
    RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    SkipToOutcome,
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
    /// when it is aborted during the legacy `revalidate` method.
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
    /// Whether the iterator has reached EOF.
    at_eof: bool,
}

/// Alias for an [`Active`] [`RawOptionalOptimized`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type OptionalOptimized<'index, W, I> = RawOptionalOptimized<'index, Active<'index>, W, I>;

impl<'query, Rf: Ref, W, I> RawOptionalOptimized<'query, Rf, W, I> {
    /// Returns a reference to the child iterator, if any. Mode-independent.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }
}

impl<'index, W, I> OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Takes the child iterator out, replacing it with an [`Empty`](crate::Empty) iterator.
    pub fn take_child(&mut self) -> Option<I> {
        self.child.take_iterator()
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
            at_eof: false,
        }
    }
}

impl<'index, W, I> RQEIterator<'index> for OptionalOptimized<'index, W, I>
where
    // Only generic `RQEIterator` methods are called on the wildcard base here;
    // the `WildcardIterator` marker is enforced at construction (see `new`).
    W: RQEIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.last_doc_id != 0
            && self.child.last_doc_id() == self.last_doc_id
            && let Some(result) = self.child.current()
        {
            return Some(result);
        }

        Some(&mut self.virt)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof {
            return Ok(None);
        }

        // Advance wcii to the next existing document.
        let wcii_doc_id = match self.wcii.read()? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(r) => r.doc_id,
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if wcii_doc_id > self.max_doc_id {
            self.at_eof = true;
            return Ok(None);
        }

        // Advance child to catch up with wcii.
        if wcii_doc_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(wcii_doc_id)?;
        }

        self.last_doc_id = wcii_doc_id;
        // Keep at_eof consistent so callers see true immediately after the last result.
        self.at_eof = wcii_doc_id >= self.max_doc_id;

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

        if doc_id > self.max_doc_id || self.at_eof {
            self.at_eof = true;
            return Ok(None);
        }

        // Promote wcii to doc_id. It may land on a different doc if doc_id is not
        // present in the existing-documents index.
        let (found, effective_id) = match self.wcii.skip_to(doc_id)? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(SkipToOutcome::Found(r)) => (true, r.doc_id),
            Some(SkipToOutcome::NotFound(r)) => (false, r.doc_id),
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if effective_id > self.max_doc_id {
            self.at_eof = true;
            return Ok(None);
        }

        // Advance child to effective_id if needed.
        if effective_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(effective_id)?;
        }

        self.last_doc_id = effective_id;
        // Keep at_eof consistent so callers see true immediately after the last result.
        self.at_eof = effective_id >= self.max_doc_id;

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

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.at_eof = false;
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
        self.at_eof
    }

    fn type_(&self) -> ffi::IteratorType {
        ffi::IteratorType::OptionalOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W, I> RQEIteratorBoxed<'index> for OptionalOptimized<'index, W, I>
where
    // Marker enforced at construction (`new`), not on the suspend/resume path —
    // the suspend/resume impls only move the wildcard child through the box
    // cast, so `W: RQEIteratorBoxed` suffices. This drops the recursive
    // `for<'a> …: WildcardIterator + RQEIteratorBoxed<Suspended = …>` HRTB,
    // which is otherwise unsatisfiable once `'query` narrows on resume.
    W: RQEIteratorBoxed<'index>,
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawOptionalOptimized<'index, Suspended, W::Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawOptionalOptimized` is `#[repr(C)]`. The only
        // `Rf`-dependent field is `virt: RawIndexResult<Rf>`, layout-
        // compatible across `Rf` via `SharedPtr` transparency. `W`/`I` are
        // layout-compatible with `W::Suspended`/`I::Suspended` by the
        // [`RQEIteratorBoxed`] contract, and `MaybeEmpty<I>` likewise
        // (see [`MaybeEmpty::suspend`]). Box::from_raw reuses the same
        // heap allocation.
        unsafe {
            Box::from_raw(
                raw as *mut RawOptionalOptimized<'index, Suspended, W::Suspended, I::Suspended>,
            )
        }
    }
}

impl<'query, WS, IS> RQESuspendedIterator<'query> for RawOptionalOptimized<'query, Suspended, WS, IS>
where
    WS: RQESuspendedIterator<'query>,
    IS: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = OptionalOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let RawOptionalOptimized {
            wcii,
            child,
            virt,
            max_doc_id,
            weight,
            last_doc_id,
            at_eof,
        } = *self;

        // Resume both children unconditionally, mirroring the legacy path
        // (which drove `resume` on both before inspecting either outcome).
        // The `wcii` (base) child is genuinely a wildcard; its concrete
        // resumed type `WS::Resumed<'a>` is statically a `WildcardIterator`.
        let pre_wcii_last_doc_id = RQESuspendedIterator::last_doc_id(&wcii);
        let wcii_outcome = Box::new(wcii).resume(guard)?;
        let pre_child_last_doc_id = RQESuspendedIterator::last_doc_id(&child);
        let child_outcome = Box::new(child).resume(guard)?;

        // If `wcii` aborted we cannot rebuild — there is no base to enumerate
        // doc ids — so the whole iterator aborts, dropping the just-resumed
        // child.
        let (wcii, wcii_moved) = match wcii_outcome {
            ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
            ResumeOutcome::Ok(w) => (*w, false),
            ResumeOutcome::Moved(w) => (*w, true),
        };

        // An aborted query child is replaced by `Empty` (mirroring
        // `revalidate`'s `take_iterator`) and treated as "moved": its state
        // changed, so downstream must re-evaluate.
        let (child, child_moved_or_aborted): (MaybeEmpty<IS::Resumed<'a>>, bool) =
            match child_outcome {
                ResumeOutcome::Aborted => (MaybeEmpty::new_empty(), true),
                ResumeOutcome::Ok(c) => (*c, false),
                ResumeOutcome::Moved(c) => (*c, true),
            };

        // SAFETY: `OptionalOptimized`'s `virt` is a virtual sentinel built
        // via `build_virt()` — no aliased pointers to validate. The
        // `Active<'a>` re-typing is unconditionally sound. See the same
        // SAFETY note in `Not::resume`.
        let virt = unsafe { virt.into_active::<'a>() };

        let mut active = Box::new(OptionalOptimized {
            wcii,
            child,
            virt,
            max_doc_id,
            weight,
            last_doc_id,
            at_eof,
        });

        // Distinguish "wcii moved to a new valid position" from "wcii moved
        // past all docs (no new current)". `ResumeOutcome::Moved` doesn't
        // carry the {Some, None} distinction that legacy `revalidate` had, so
        // we recover it by comparing wcii's pre/post `last_doc_id`: a true
        // "Moved to EOF without a new doc" leaves the cached doc_id unchanged
        // (the wcii has nothing new to surface). A "Moved to a new valid doc"
        // advances the cached doc_id even if `at_eof` is now true (because
        // that new doc is the LAST valid one).
        if wcii_moved && active.wcii.at_eof() && active.wcii.last_doc_id() == pre_wcii_last_doc_id {
            active.at_eof = true;
            return Ok(ResumeOutcome::Moved(active));
        }
        active.at_eof = active.wcii.at_eof();

        let current_was_virtual =
            active.last_doc_id == 0 || pre_child_last_doc_id != active.last_doc_id;

        if !wcii_moved {
            // wcii stayed at the same position.
            if !child_moved_or_aborted || current_was_virtual {
                // Child is still valid, or the current result was virtual — no change.
                return Ok(ResumeOutcome::Ok(active));
            }
            // Child moved or aborted while the current was a real result:
            // advance to the next valid state.
            active.read()?;
            return Ok(ResumeOutcome::Moved(active));
        }

        // wcii moved to a new valid position; update the child accordingly.
        let wcii_doc_id = active.wcii.last_doc_id();
        if wcii_doc_id > active.max_doc_id {
            active.at_eof = true;
            return Ok(ResumeOutcome::Moved(active));
        }
        if wcii_doc_id > active.child.last_doc_id() {
            active.child.skip_to(wcii_doc_id)?;
        }
        active.last_doc_id = wcii_doc_id;
        active.at_eof |= wcii_doc_id >= active.max_doc_id;
        if active.child.last_doc_id() != wcii_doc_id {
            active.virt.doc_id = wcii_doc_id;
        }
        Ok(ResumeOutcome::Moved(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }
}

impl<'index, W> crate::interop::ProfileChildren<'index>
    for OptionalOptimized<'index, W, crate::c2rust::CRQEIterator>
where
    W: crate::RQEIteratorBoxed<'index> + 'index,
{
    fn profile_children(self) -> Self {
        OptionalOptimized {
            max_doc_id: self.max_doc_id,
            weight: self.weight,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            wcii: self.wcii,
            virt: self.virt,
            last_doc_id: self.last_doc_id,
            at_eof: self.at_eof,
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
