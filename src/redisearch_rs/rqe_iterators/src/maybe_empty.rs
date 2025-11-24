/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, empty::Empty};

/// An iterator that is either [`Empty`] or the provided [`RQEIterator`].
pub struct MaybeEmpty<I>(MaybeEmptyOption<I>);

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
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.revalidate(),
            MaybeEmptyOption::Some(it) => it.revalidate(),
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
            MaybeEmptyOption::None(empty) => empty.last_doc_id(),
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
}
