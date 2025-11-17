/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// TODO: dependency for MOD-10080;
//       unused until then;
//       this lint overwrite should be removed at that point.
#![allow(unused)]

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, empty::Empty};

/// An iterator that is either [`Empty`] or the provided [`RQEIterator`].
pub(crate) struct MaybeEmpty<I>(MaybeEmptyOption<I>);

impl<'index, I> MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    /// Create a new [`MaybeEmpty`] with the given iterator as the underlying [`RQEIterator`].
    #[inline(always)]
    pub(crate) const fn new(iterator: I) -> Self {
        Self(MaybeEmptyOption::Some(iterator))
    }

    /// Create a new [`MaybeEmpty`] with [`Empty`] as the underlying [`RQEIterator`].
    #[inline(always)]
    pub(crate) const fn new_empty() -> Self {
        Self(MaybeEmptyOption::None(Empty))
    }

    /// Consume the iterator, if there is any, and return if so.
    pub(crate) fn take_iterator(&mut self) -> Option<I> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Infinite<'index>(inverted_index::RSIndexResult<'index>);

    impl<'index> RQEIterator<'index> for Infinite<'index> {
        fn read(
            &mut self,
        ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, crate::RQEIteratorError>
        {
            self.0.doc_id += 1;
            Ok(Some(&mut self.0))
        }

        fn skip_to(
            &mut self,
            doc_id: ffi::t_docId,
        ) -> Result<Option<SkipToOutcome<'_, 'index>>, crate::RQEIteratorError> {
            self.0.doc_id = doc_id;
            Ok(Some(SkipToOutcome::Found(&mut self.0)))
        }

        fn rewind(&mut self) {
            self.0.doc_id = 0;
        }

        fn num_estimated(&self) -> usize {
            usize::MAX
        }

        fn last_doc_id(&self) -> ffi::t_docId {
            self.0.doc_id
        }

        fn at_eof(&self) -> bool {
            false
        }

        fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, crate::RQEIteratorError> {
            Ok(RQEValidateStatus::Ok)
        }
    }

    #[test]
    fn initial_state_empty() {
        let it = MaybeEmpty::<Infinite>::new_empty();

        assert_eq!(it.last_doc_id(), 0);
        assert!(it.at_eof());
        assert_eq!(it.num_estimated(), 0);
    }

    #[test]
    fn initial_state_not_empty() {
        let it = MaybeEmpty::new(Infinite::default());

        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());
        assert_eq!(it.num_estimated(), usize::MAX);
    }

    #[test]
    fn read_empty() {
        let mut it = MaybeEmpty::<Infinite>::new_empty();

        assert_eq!(it.num_estimated(), 0);
        assert!(it.at_eof());

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        assert!(matches!(it.read(), Ok(None)));
    }

    #[test]
    fn read_not_empty() {
        let mut it = MaybeEmpty::new(Infinite::default());
        for expected_id in 1..=5 {
            let result = it.read();
            let result = result.unwrap();
            let doc = result.unwrap();
            assert_eq!(doc.doc_id, expected_id);
            assert_eq!(it.last_doc_id(), expected_id);
            assert!(!it.at_eof());
        }
    }

    #[test]
    fn skip_to_empty() {
        let mut it = MaybeEmpty::<Infinite>::new_empty();

        assert!(matches!(it.skip_to(1), Ok(None)));
        assert!(it.at_eof());

        assert!(matches!(it.skip_to(42), Ok(None)));
        assert!(matches!(it.skip_to(1000), Ok(None)));
    }

    #[test]
    fn skip_to_not_empty() {
        let mut it = MaybeEmpty::new(Infinite::default());

        for i in 1..=5 {
            let id = (i * 5) as ffi::t_docId;
            let outcome = it.skip_to(id).unwrap();
            assert_eq!(
                outcome,
                Some(SkipToOutcome::Found(
                    &mut inverted_index::RSIndexResult::virt().doc_id(id)
                ))
            );
            assert_eq!(it.last_doc_id(), id);
            assert!(!it.at_eof());
        }
    }

    #[test]
    fn rewind_empty() {
        let mut it = MaybeEmpty::<Infinite>::new_empty();

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());

        it.rewind();
        assert!(it.at_eof());

        assert!(matches!(it.read(), Ok(None)));
        assert!(it.at_eof());
    }

    #[test]
    fn rewind_not_empty() {
        let mut it = MaybeEmpty::new(Infinite::default());

        // Read some documents
        for _i in 1..=3 {
            let result = it.read().unwrap();
            assert!(result.is_some());
        }

        assert_eq!(it.last_doc_id(), 3);

        // Rewind
        it.rewind();

        // Check state after rewind
        assert_eq!(it.last_doc_id(), 0);
        assert!(!it.at_eof());

        // Should be able to read from beginning again
        let result = it.read().unwrap();
        let doc = result.unwrap();

        assert_eq!(doc.doc_id, 1);
        assert_eq!(it.last_doc_id(), 1);
    }

    #[test]
    fn revalidate_empty() {
        let mut it = MaybeEmpty::<Infinite>::new_empty();
        assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
    }

    #[test]
    fn revalidate_not_empty() {
        let mut it = MaybeEmpty::new(Infinite::default());
        assert_eq!(it.revalidate().unwrap(), RQEValidateStatus::Ok);
    }
}
