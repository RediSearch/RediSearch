/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Helper wrapping either [`Empty`] or the provided [`RQEIterator`].

use ffi::{ValidateStatus, ValidateStatus_VALIDATE_OK, t_docId};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, SkipToOutcome, empty::Empty,
};

/// An iterator that is either [`Empty`] or the provided [`RQEIterator`].
///
/// `#[repr(C)]` so that `MaybeEmpty<I>` is layout-compatible with
/// `MaybeEmpty<I::Suspended>` once the iterator's `Suspended` counterpart
/// (added in the suspend/resume phase) lines up with `I`.
#[repr(C)]
pub struct MaybeEmpty<I>(MaybeEmptyOption<I>);

impl<I> MaybeEmpty<I> {
    /// Get a ref to child iterator, if any. Mode-independent —
    /// pattern-matches on the inner storage, no iterator surface needed.
    #[inline(always)]
    pub const fn as_ref(&self) -> Option<&I> {
        match &self.0 {
            MaybeEmptyOption::None(_) => None,
            MaybeEmptyOption::Some(it) => Some(it),
        }
    }
}

impl<'index, I> MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    /// Create a new [`MaybeEmpty`] with the given iterator as the underlying [`RQEIterator`].
    #[inline(always)]
    pub const fn new(iterator: I) -> Self {
        Self(MaybeEmptyOption::Some(iterator))
    }

    /// Create a new [`MaybeEmpty`] with [`Empty`] as the underlying [`RQEIterator`].
    #[inline(always)]
    pub const fn new_empty() -> Self {
        Self(MaybeEmptyOption::None(Empty))
    }

    /// Transform the inner iterator (if present) into a new type.
    pub fn map<'b, J>(self, f: impl FnOnce(I) -> J) -> MaybeEmpty<J>
    where
        J: RQEIterator<'b>,
    {
        match self.0 {
            MaybeEmptyOption::None(_) => MaybeEmpty(MaybeEmptyOption::None(Empty)),
            MaybeEmptyOption::Some(it) => MaybeEmpty(MaybeEmptyOption::Some(f(it))),
        }
    }

    /// Consume the iterator, if there is any, and return if so.
    pub fn take_iterator(&mut self) -> Option<I> {
        if let MaybeEmptyOption::Some(iterator) = std::mem::take(&mut self.0) {
            return Some(iterator);
        }
        None
    }
}

impl<'index, I> Default for MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn default() -> Self {
        Self::new_empty()
    }
}

#[repr(C)]
enum MaybeEmptyOption<I> {
    None(Empty),
    Some(I),
}

impl<I> Default for MaybeEmptyOption<I> {
    fn default() -> Self {
        MaybeEmptyOption::None(Empty)
    }
}

impl<'index, I> RQEIterator<'index> for MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.current(),
            MaybeEmptyOption::Some(it) => it.current(),
        }
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.read(),
            MaybeEmptyOption::Some(it) => it.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.skip_to(doc_id),
            MaybeEmptyOption::Some(it) => it.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.revalidate(spec),
            MaybeEmptyOption::Some(it) => it.revalidate(spec),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.rewind(),
            MaybeEmptyOption::Some(it) => it.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match &self.0 {
            MaybeEmptyOption::None(empty) => empty.num_estimated(),
            MaybeEmptyOption::Some(it) => it.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        match &self.0 {
            // Disambiguated against `RQESuspendedIterator::last_doc_id`
            // (Empty's suspended counterpart is itself).
            MaybeEmptyOption::None(empty) => RQEIterator::last_doc_id(empty),
            MaybeEmptyOption::Some(it) => it.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match &self.0 {
            MaybeEmptyOption::None(empty) => empty.at_eof(),
            MaybeEmptyOption::Some(it) => it.at_eof(),
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        match &self.0 {
            MaybeEmptyOption::None(empty) => empty.type_(),
            MaybeEmptyOption::Some(it) => it.type_(),
        }
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        match &self.0 {
            MaybeEmptyOption::None(empty) => {
                empty.intersection_sort_weight(prioritize_union_children)
            }
            MaybeEmptyOption::Some(it) => it.intersection_sort_weight(prioritize_union_children),
        }
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for MaybeEmpty<I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = MaybeEmpty<I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `MaybeEmpty<I>` is `#[repr(C)]` and contains a
        // `#[repr(C)]` enum `MaybeEmptyOption<I>` whose layout is
        // tag + (max(size_of::<Empty>(), size_of::<I>())) =
        // tag + size_of::<I>(). `I` and `I::Suspended` are layout-
        // compatible by the [`RQEIteratorBoxed`] contract, so the two
        // instantiations share the same byte layout. Box::from_raw reuses
        // the same heap allocation.
        unsafe { Box::from_raw(raw as *mut MaybeEmpty<I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        match &mut self.0 {
            MaybeEmptyOption::Some(it) => it.cascade_suspend(),
            MaybeEmptyOption::None(_) => {} // Empty iterator — nothing to cascade
        }
    }
}

impl<S> RQESuspendedIterator for MaybeEmpty<S>
where
    S: RQESuspendedIterator,
{
    type Resumed<'a> = MaybeEmpty<S::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        match (*self).0 {
            MaybeEmptyOption::None(empty) => (
                Box::new(MaybeEmpty(MaybeEmptyOption::None(empty))),
                ValidateStatus_VALIDATE_OK,
            ),
            MaybeEmptyOption::Some(child) => {
                let (active_child, status) = Box::new(child).resume(guard);
                (
                    Box::new(MaybeEmpty(MaybeEmptyOption::Some(*active_child))),
                    status,
                )
            }
        }
    }

    fn last_doc_id(&self) -> t_docId {
        match &self.0 {
            MaybeEmptyOption::None(_) => 0,
            MaybeEmptyOption::Some(child) => S::last_doc_id(child),
        }
    }
}