/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Optional`].

use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use std::cmp;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};
use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};

/// An iterator that emits a sequence of results with no gaps, up to a given document id.
/// Results are pulled from an underlying [`RQEIterator`] instance. If there is no entry
/// for a given document id, a virtual result is yielded in its place.
///
/// Parameterised over a [`Ref`] mode — see [`Optional`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
#[repr(C)]
pub struct RawOptional<'query, Rf: Ref, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the [`Optional::child`] beyond this bound are ignored.
    /// If the [`Optional::child`] ends before this bound, this [`Optional`] iterator yields virtual
    /// results with no [`Optional::weight`] applied until [`Optional::max_doc_id`] is reached.
    max_doc_id: DocId,

    /// Weight applied to results produced by the inner [`Optional::child`] iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// Virtual result which will always contain the last doc id,
    /// even if that doc id came from the [`Optional::child`] iterator.
    ///
    /// Only for actual virtual results do we return a reference to it in
    /// functions such as Read/SkipTo.
    result: RawIndexResult<'query, Rf>,

    /// The child [`RQEIterator`] provided at construction time.
    /// It is used while it can still produce results. Once exhausted,
    /// the iterator yields virtual results until [`Optional::max_doc_id`] is reached.
    ///
    /// In case child aborts during [`RQEIterator::revalidate`],
    /// this child is turned into [`OptionalChild::Gone`], changed from the
    /// [`OptionalChild::Present`] state it starts at when creating using
    /// [`Optional::new`]. From that point onward all results will be virtual
    /// until `max_doc_id` is reached.
    child: OptionalChild<I>,
}

/// Child slot for [`RawOptional`].
///
/// `#[repr(C)]` so that `OptionalChild<I>` is layout-compatible with
/// `OptionalChild<I::Suspended>` — a plain `Option<I>` is niche-dependent and
/// not transmute-stable across the `I` → `I::Suspended` swap that the
/// suspend/resume machinery relies on. Mirrors [`crate::maybe_empty`]'s
/// `MaybeEmptyOption` for the same reason.
#[repr(C)]
enum OptionalChild<I> {
    /// Child aborted during [`RQEIterator::revalidate`] (or otherwise gone):
    /// only virtual results from here on.
    Gone,
    /// Child still producing results.
    Present(I),
}

impl<I> OptionalChild<I> {
    /// Shared reference to the child, if it is still present.
    #[inline(always)]
    const fn as_ref(&self) -> Option<&I> {
        match self {
            Self::Gone => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Mutable reference to the child, if it is still present.
    #[inline(always)]
    const fn as_mut(&mut self) -> Option<&mut I> {
        match self {
            Self::Gone => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Map the child (if present) into a new type, preserving the [`Gone`] state.
    ///
    /// [`Gone`]: OptionalChild::Gone
    #[inline(always)]
    fn map<J>(self, f: impl FnOnce(I) -> J) -> OptionalChild<J> {
        match self {
            Self::Gone => OptionalChild::Gone,
            Self::Present(i) => OptionalChild::Present(f(i)),
        }
    }
}

/// Alias for an [`Active`] [`RawOptional`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Optional<'index, I> = RawOptional<'index, Active<'index>, I>;

impl<'query, Rf: Ref, I> RawOptional<'query, Rf, I> {
    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Optional`] iterator. Mode-independent.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }
}

impl<'index, I> Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the inclusive upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`RQEIterator`]. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `child` [`RQEIterator`] used and wrapped around by this [`Optional`] iterator
    pub fn new(max_id: DocId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            child: OptionalChild::Present(child),
        }
    }

    /// Set the child of this [`Optional`] iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = OptionalChild::Present(new_child);
    }

    /// Unset the child of this [`Optional`] iterator (make it behave as if
    /// it had no child — i.e. [`Gone`](OptionalChild::Gone)).
    pub fn unset_child(&mut self) {
        self.child = OptionalChild::Gone;
    }

    /// Take the child of this [`Optional`] iterator if it had one.
    /// After this the child iterator of this [`Optional`] will behave
    /// as if it was the [`Empty`](crate::Empty) iterator.
    pub fn take_child(&mut self) -> Option<I> {
        match std::mem::replace(&mut self.child, OptionalChild::Gone) {
            OptionalChild::Present(child) => Some(child),
            OptionalChild::Gone => None,
        }
    }
}

impl<'index, I> RQEIterator<'index> for Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if let Some(child) = self.child.as_mut()
            && child.last_doc_id() == self.result.doc_id
            && let Some(child_result) = child.current()
        {
            Some(child_result)
        } else {
            Some(&mut self.result)
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof() {
            return Ok(None);
        }

        let maybe_real = self
            .child
            .as_mut()
            .map(|child| {
                let child_last_doc_id = child.last_doc_id();
                match child_last_doc_id.cmp(&(self.result.doc_id + 1)) {
                    cmp::Ordering::Less => child.read(),
                    cmp::Ordering::Equal => Ok(child.current()),
                    cmp::Ordering::Greater => Ok(None),
                }
            })
            .transpose()?
            .flatten();

        self.result.doc_id += 1;

        if let Some(real) = maybe_real {
            debug_assert!(
                real.doc_id >= self.result.doc_id,
                "no backwards reads should be possible"
            );

            if real.doc_id == self.result.doc_id {
                real.weight = self.weight;
                return Ok(Some(real));
            }
        }

        Ok(Some(&mut self.result))
    }

    /// Skip to a specific docId. If the child has a hit on this docId, return it.
    /// Otherwise, return a virtual hit.
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.result.doc_id);

        if doc_id > self.max_doc_id || self.at_eof() {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        if let Some(child) = self.child.as_mut() {
            if doc_id > child.last_doc_id() {
                // use current() here to work around
                // borrowing rules to be able to handle
                // both of `doc_id >= child.last_doc_id` cases...
                let _ = child.skip_to(doc_id)?;
            }

            if let Some(real) = child.current()
                && real.doc_id == doc_id
            {
                real.weight = self.weight;
                self.result.doc_id = real.doc_id;
                return Ok(Some(SkipToOutcome::Found(real)));
            }
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(RQEValidateStatus::Ok);
        };
        let last_child_doc_id = child.last_doc_id();

        // Revalidate the child iterator
        match child.revalidate(spec)? {
            // Abort: Handle child validation results (but continue processing)
            status @ (RQEValidateStatus::Aborted | RQEValidateStatus::Moved { .. }) => {
                if matches!(status, RQEValidateStatus::Aborted) {
                    self.child = OptionalChild::Gone; // Drop it so we become fully virtual until max is reached
                }

                Ok(if last_child_doc_id != self.result.doc_id {
                    // virtual
                    RQEValidateStatus::Ok
                } else {
                    // was real before abort, re-read to
                    // prevent returning stale data.
                    RQEValidateStatus::Moved {
                        current: self.read()?,
                    }
                })
            }
            // If the current result is virtual,
            // or if the child was not moved, we can return VALIDATE_OK
            RQEValidateStatus::Ok => Ok(RQEValidateStatus::Ok),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.result.doc_id = 0;
        if let Some(child) = self.child.as_mut() {
            child.rewind();
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Optional
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Optional<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawOptional<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawOptional` is `#[repr(C)]`. The only `Rf`-dependent
        // field is `result: RawIndexResult<Rf>`, layout-compatible across
        // `Rf` via `SharedPtr` transparency. `Option<I>` ↔ `Option<I::Suspended>`
        // are layout-compatible by the [`RQEIteratorBoxed`] contract.
        // Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawOptional<'index, Suspended, I::Suspended>) }
    }
}

impl<'query, S> RQESuspendedIterator<'query> for RawOptional<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = Optional<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let RawOptional {
            max_doc_id,
            weight,
            result,
            child,
        } = *self;

        // Resume the child first (if present) so we never construct the
        // active `Optional<'a, …>` with a still-suspended `child` field.
        // Track whether the child aborted or moved to mirror `revalidate`;
        // an aborted child is dropped (`Gone`), leaving us fully virtual.
        let (child, child_disturbed, last_child_doc_id) = match child {
            OptionalChild::Gone => (OptionalChild::Gone, false, 0),
            OptionalChild::Present(c) => {
                let last = S::last_doc_id(&c);
                match Box::new(c).resume(guard)? {
                    ResumeOutcome::Aborted => (OptionalChild::Gone, true, last),
                    ResumeOutcome::Ok(active_child) => {
                        (OptionalChild::Present(*active_child), false, last)
                    }
                    ResumeOutcome::Moved(active_child) => {
                        (OptionalChild::Present(*active_child), true, last)
                    }
                }
            }
        };

        // SAFETY: `Optional`'s `result` is a virtual sentinel built via
        // `build_virt()` — no aliased pointers to validate. The
        // `Active<'a>` re-typing is unconditionally sound. See the same
        // SAFETY note in `Not::resume`.
        let result = unsafe { result.into_active::<'a>() };

        let mut active = Box::new(Optional {
            max_doc_id,
            weight,
            result,
            child,
        });

        // Mirror `revalidate`: a child that aborted or moved forces a re-read
        // iff the previous result came from the child (its doc id matches the
        // aggregate's current doc id) rather than being a virtual sentinel.
        let moved = if child_disturbed && last_child_doc_id == active.result.doc_id {
            active.read()?;
            true
        } else {
            false
        };

        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mode-independent — mirrors the active `num_estimated`.
        self.max_doc_id as usize
    }
}
impl<'index> crate::interop::ProfileChildren<'index>
    for Optional<'index, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        Optional {
            max_doc_id: self.max_doc_id,
            weight: self.weight,
            result: self.result,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
        }
    }
}

impl<'index, I> ProfilePrint for Optional<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"OPTIONAL", self.child(), map);
    }
}
