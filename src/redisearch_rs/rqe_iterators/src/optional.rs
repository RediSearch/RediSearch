/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Optional`].

use ffi::{RS_FIELDMASK_ALL, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK, t_docId};
use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use std::cmp;

use crate::{
    BoxedRQEIterator, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, SkipToOutcome,
};
use index_spec::IndexSpecReadGuard;

/// Trait implemented by all optional iterator variants.
///
/// Both [`Optional`] and [`crate::optional_optimized::OptionalOptimized`] implement this,
/// with the child stored as a [`BoxedRQEIterator`].
pub trait OptionalIterator<'index>: RQEIterator<'index> {
    /// Returns a shared reference to the child iterator, if any.
    fn child(&self) -> Option<&(dyn RQEIterator<'index> + 'index)>;

    /// Takes ownership of the child iterator, replacing it with an empty state.
    ///
    /// Returns `None` if there is no child.
    fn take_child(&mut self) -> Option<BoxedRQEIterator<'index>>;

    /// Sets (or overwrites) the child iterator.
    fn set_child(&mut self, child: BoxedRQEIterator<'index>);

    /// Unsets the child iterator (makes it `None`).
    ///
    /// # Panics
    ///
    /// Panics for iterator variants that do not support an absent child
    /// (e.g. [`crate::optional_optimized::OptionalOptimized`]).
    fn unset_child(&mut self);
}

impl<'index> OptionalIterator<'index> for Optional<'index, BoxedRQEIterator<'index>> {
    fn child(&self) -> Option<&(dyn RQEIterator<'index> + 'index)> {
        Optional::child(self).map(|c| c as &dyn RQEIterator<'index>)
    }

    fn take_child(&mut self) -> Option<BoxedRQEIterator<'index>> {
        Optional::take_child(self)
    }

    fn set_child(&mut self, child: BoxedRQEIterator<'index>) {
        Optional::set_child(self, child);
    }

    fn unset_child(&mut self) {
        Optional::unset_child(self);
    }
}

/// An iterator that emits a sequence of results with no gaps, up to a given document id.
/// Results are pulled from an underlying [`RQEIterator`] instance. If there is no entry
/// for a given document id, a virtual result is yielded in its place.
///
/// Parameterised over a [`Ref`] mode — see [`Optional`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
#[repr(C)]
pub struct RawOptional<Rf: Ref, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the [`Optional::child`] beyond this bound are ignored.
    /// If the [`Optional::child`] ends before this bound, this [`Optional`] iterator yields virtual
    /// results with no [`Optional::weight`] applied until [`Optional::max_doc_id`] is reached.
    max_doc_id: t_docId,

    /// Weight applied to results produced by the inner [`Optional::child`] iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// Virtual result which will always contain the last doc id,
    /// even if that doc id came from the [`Optional::child`] iterator.
    ///
    /// Only for actual virtual results do we return a reference to it in
    /// functions such as Read/SkipTo.
    result: RawIndexResult<Rf>,

    /// The child [`RQEIterator`] provided at construction time.
    /// It is used while it can still produce results. Once exhausted,
    /// the iterator yields virtual results until [`Optional::max_doc_id`] is reached.
    ///
    /// In case child aborts during `RQEIterator::revalidate` (removed),
    /// this child is turned into [`None`], changed from the [`Some`] state it starts
    /// at when creating using [`Optional::new`]. From that point onward all results
    /// will be virtual until `max_doc_id` is reached.
    child: Option<I>,
}

/// Alias for an [`Active`] [`RawOptional`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Optional<'index, I> = RawOptional<Active<'index>, I>;

impl<Rf: Ref, I> RawOptional<Rf, I> {
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
    pub fn new(max_id: t_docId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            child: Some(child),
        }
    }

    /// Set the child of this [`Optional`] iterator.
    pub fn set_child(&mut self, new_child: I) {
        self.child = Some(new_child);
    }

    /// Unset the child of this [`Optional`] iterator (make it `None`).
    pub fn unset_child(&mut self) {
        self.child = None;
    }

    /// Take the child of this [`Optional`] iterator if it had one.
    /// After this the child iterator of this [`Optional`] will behave
    /// as if it was the [`Empty`](crate::Empty) iterator.
    pub const fn take_child(&mut self) -> Option<I> {
        self.child.take()
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
        doc_id: t_docId,
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
    fn last_doc_id(&self) -> t_docId {
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
    type Suspended = RawOptional<Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Walk the single child (`Option<I>`). When `Some(I)`, dispatch the
        // child's `suspend` via the trait (vtable for dyn-erased `I`); when
        // `None`, no-op. See [`crate::boxed::suspend_child_slot_in_place`].
        //
        // SAFETY: `raw` came from `Box::into_raw`, exclusively owned.
        unsafe {
            if let Some(child) = (*raw).child.as_mut() {
                crate::boxed::suspend_child_slot_in_place(child as *mut I);
            }
        }
        // SAFETY: `RawOptional` is `#[repr(C)]` over `child: Option<I>`
        // (now byte-rewritten when `Some`), `result: RawIndexResult<Rf>`
        // (layout-compatible via `SharedPtr`), and plain fields.
        unsafe { Box::from_raw(raw as *mut RawOptional<Suspended, I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        if let Some(child) = self.child.as_mut() {
            child.cascade_suspend();
        }
    }
}

impl<S> RQESuspendedIterator for RawOptional<Suspended, S>
where
    S: RQESuspendedIterator,
{
    type Resumed<'a> = Optional<'a, S::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let RawOptional {
            max_doc_id,
            weight,
            result,
            child,
        } = *self;

        // Resume the child first (if present) so we never construct the
        // active `Optional<'a, …>` with a still-suspended `child` field.
        let (child, child_status, last_child_doc_id) = match child {
            None => (None, ValidateStatus_VALIDATE_OK, 0),
            Some(c) => {
                let last = S::last_doc_id(&c);
                let (active_child, status) = Box::new(c).resume(guard);
                (Some(*active_child), status, last)
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

        // Mirror the existing `revalidate` semantics: abort drops the child
        // (we go fully virtual), move forces a re-read iff the previous
        // result came from the child rather than being a virtual sentinel.
        #[expect(non_upper_case_globals, reason = "bindgen-generated constants")]
        let status = match child_status {
            ValidateStatus_VALIDATE_ABORTED => {
                active.child = None;
                if last_child_doc_id == active.result.doc_id {
                    let _ = active.read();
                    ValidateStatus_VALIDATE_MOVED
                } else {
                    ValidateStatus_VALIDATE_OK
                }
            }
            ValidateStatus_VALIDATE_MOVED => {
                if last_child_doc_id == active.result.doc_id {
                    let _ = active.read();
                    ValidateStatus_VALIDATE_MOVED
                } else {
                    ValidateStatus_VALIDATE_OK
                }
            }
            _ => ValidateStatus_VALIDATE_OK,
        };

        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
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
