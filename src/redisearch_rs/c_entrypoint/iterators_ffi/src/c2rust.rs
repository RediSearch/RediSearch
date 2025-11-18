/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
// TODO: remove once we have compound iterators written in Rust that leverage
//   this shim.
#![allow(unused)]
use ffi::{
    IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND, IteratorStatus_ITERATOR_OK,
    IteratorStatus_ITERATOR_TIMEOUT, QueryIterator, ValidateStatus_VALIDATE_ABORTED,
    ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK, t_docId,
};
use inverted_index::RSIndexResult;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};
use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

/// A Rust shim over a query iterator that satisfies the C iterator API.
///
/// # Rationale
///
/// [`CRQEIterator`] allows Rust code to treat query iterators written in C
/// as if they implemented the [`RQEIterator`] trait.
///
/// This is particularly useful when composing iterators—e.g.
/// in the union or intersection iterators. They can work seamslessly with both C
/// and Rust iterators, since they both implement the [`RQEIterator`] trait: Rust
/// iterators do it "directly", C iterators do it via this shim.
///
/// # Implementation details
///
/// It is not a given that the underlying iterator is written in C!
/// It might be a Rust iterator, wrapped to obey the C API, being passed into
/// a Rust composite iterator.
#[repr(transparent)]
pub(crate) struct CRQEIterator {
    header: NonNull<QueryIterator>,
}

impl AsRef<QueryIterator> for CRQEIterator {
    fn as_ref(&self) -> &QueryIterator {
        unsafe { self.header.as_ref() }
    }
}

impl AsMut<QueryIterator> for CRQEIterator {
    fn as_mut(&mut self) -> &mut QueryIterator {
        unsafe { self.header.as_mut() }
    }
}

impl Deref for CRQEIterator {
    type Target = QueryIterator;

    fn deref(&self) -> &Self::Target {
        unsafe { self.header.as_ref() }
    }
}

impl DerefMut for CRQEIterator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.header.as_mut() }
    }
}

impl Drop for CRQEIterator {
    fn drop(&mut self) {
        let header = std::mem::replace(&mut self.header, NonNull::dangling());
        let free = unsafe { header.as_ref().Free };
        if let Some(free) = free {
            unsafe { free(header.as_ptr()) }
        }
    }
}

impl CRQEIterator {
    /// Transmute a non-null pointer to a C query iterator into a mutable reference
    /// to its Rust wrapper.
    ///
    /// # Safety
    pub(crate) const unsafe fn new<'a>(header: NonNull<QueryIterator>) -> Self {
        // SAFETY:
        Self { header }
    }

    pub(crate) fn into_raw(self) -> NonNull<QueryIterator> {
        let self_ = ManuallyDrop::new(self);
        self_.header
    }
}

impl<'index> RQEIterator<'index> for CRQEIterator {
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let callback = self
            .Read
            .expect("The `Read` callback is a NULL function pointer");
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_mut()) };
        #[allow(non_upper_case_globals)]
        match status {
            IteratorStatus_ITERATOR_EOF => {
                self.atEOF = true;
                Ok(None)
            }
            IteratorStatus_ITERATOR_TIMEOUT => Err(RQEIteratorError::TimedOut),
            IteratorStatus_ITERATOR_OK => {
                let data = self.current as *mut RSIndexResult;
                // SAFETY:
                // - We have a unique handle over this iterator.
                let data = unsafe {
                    data.as_mut()
                        .expect("`current` is a NULL pointer after `Read` returned OK")
                };
                Ok(Some(data))
            }
            IteratorStatus_ITERATOR_NOTFOUND => {
                unreachable!(
                    "The `Read` callback returned `NOTFOUND`, which should only be used for `SkipTo`"
                )
            }
            _ => {
                unreachable!("`Read` returned an unexpected iterator status, {status}")
            }
        }
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let callback = self
            .SkipTo
            .expect("The `SkipTo` callback is a NULL function pointer");
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_mut(), doc_id) };
        #[allow(non_upper_case_globals)]
        match status {
            IteratorStatus_ITERATOR_EOF => {
                self.atEOF = true;
                Ok(None)
            }
            IteratorStatus_ITERATOR_TIMEOUT => Err(RQEIteratorError::TimedOut),
            IteratorStatus_ITERATOR_OK => {
                let data = self.current as *mut RSIndexResult;
                // SAFETY:
                // - We have a unique handle over this iterator.
                let data = unsafe {
                    data.as_mut()
                        .expect("`current` is a NULL pointer after `SkipTo` returned OK")
                };
                Ok(Some(SkipToOutcome::Found(data)))
            }
            IteratorStatus_ITERATOR_NOTFOUND => {
                let data = self.current as *mut RSIndexResult;
                // SAFETY:
                // - We have a unique handle over this iterator.
                let data = unsafe {
                    data.as_mut()
                        .expect("`current` is a NULL pointer after `SkipTo` returned NOT_FOUND")
                };
                Ok(Some(SkipToOutcome::NotFound(data)))
            }
            _ => {
                unreachable!("`SkipTo` returned an unexpected iterator status, {status}")
            }
        }
    }

    fn rewind(&mut self) {
        let callback = self
            .Rewind
            .expect("The `Rewind` callback is a NULL function pointer");
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        unsafe { callback(self.header.as_mut()) };
    }

    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let callback = self
            .Revalidate
            .expect("The `Revalidate` callback is a NULL function pointer");
        let status = unsafe { callback(self.header.as_mut()) };
        #[allow(non_upper_case_globals)]
        let status = match status {
            ValidateStatus_VALIDATE_ABORTED => RQEValidateStatus::Aborted,
            ValidateStatus_VALIDATE_MOVED => RQEValidateStatus::Moved {
                current: self.current(),
            },
            ValidateStatus_VALIDATE_OK => RQEValidateStatus::Ok,
            _ => {
                unreachable!("`Validate` returned an unexpected status, {status}")
            }
        };
        Ok(status)
    }

    fn num_estimated(&self) -> usize {
        let callback = self
            .NumEstimated
            .expect("The `NumEstimated` callback is a NULL function pointer");
        // SAFETY:
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        unsafe { callback(&self.header as *const _ as *mut _) }
    }

    fn last_doc_id(&self) -> t_docId {
        self.lastDocId
    }

    fn at_eof(&self) -> bool {
        self.atEOF
    }

    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.atEOF {
            None
        } else {
            unsafe { self.current.cast::<RSIndexResult<'index>>().as_mut() }
        }
    }
}
