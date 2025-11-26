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
/// If you squint a bit, this is a C-flavored version of a `Box<dyn RQEIterator>`,
/// using the C iterator interface rather than the Rust trait.
/// It can be used to pass around different iterator kinds, it is heap-allocated
/// and it has ownership (and must free) the underlying iterator
/// when it goes out of scope.
///
/// # Why do we need this?
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
#[allow(unused)]
pub(crate) struct CRQEIterator {
    /// # Safety invariants
    ///
    /// 1. [`Self::header`] is a valid pointer to a [`QueryIterator`] instance,
    ///    and can be converted to a reference.
    /// 2. [`Self::header`] is an owning pointer, in the same way `Box` owns the
    ///    allocated heap data.
    /// 3. All callbacks are defined (i.e. the function pointers are not NULL)
    /// 4. All callbacks can be safely called, when the right aliasing conditions are
    ///    in place
    header: NonNull<QueryIterator>,
}

impl AsRef<QueryIterator> for CRQEIterator {
    fn as_ref(&self) -> &QueryIterator {
        // SAFETY: We can convert to a reference thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a shared reference
        // since [`CRQEIteator::header`] owns the iterator (invariant 2.) and
        // this methods takes a shared reference to `self`, thus ensuring that
        // no mutable reference is live at the same time.
        unsafe { self.header.as_ref() }
    }
}

impl AsMut<QueryIterator> for CRQEIterator {
    fn as_mut(&mut self) -> &mut QueryIterator {
        // SAFETY: We can convert to a reference thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a mutable reference
        // since [`CRQEIteator::header`] owns the iterator (invariant 2.) and
        // this methods takes a mutable reference to `self`, thus ensuring that
        // no other reference (either shared or mutable) is live at the same time.
        unsafe { self.header.as_mut() }
    }
}

impl Deref for CRQEIterator {
    type Target = QueryIterator;

    fn deref(&self) -> &Self::Target {
        // SAFETY: We can dereference safety thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a shared reference
        // to the underlying iterator since [`CRQEIteator::header`] owns the iterator
        // (invariant 2.) and this methods takes a shared reference to `self`,
        // thus ensuring that no other mutable reference is live
        // at the same time.
        unsafe { self.header.as_ref() }
    }
}

impl DerefMut for CRQEIterator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: We can dereference safety thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a mutable reference
        // to the underlying iterator since [`CRQEIteator::header`] owns the iterator
        // (invariant 2.) and this methods takes a mutable reference to `self`,
        // thus ensuring that no other reference (either shared or mutable) is live
        // at the same time.
        unsafe { self.header.as_mut() }
    }
}

impl Drop for CRQEIterator {
    fn drop(&mut self) {
        let free = self
            .Free
            .expect("The `Free` callback is a NULL function pointer");
        // SAFETY: Safe thanks to invariant 2. for [`CRQEIterator::header`]
        unsafe {
            free(self.header.as_ptr());
        }
    }
}

impl CRQEIterator {
    /// Convert a C-style iterator into an instance of [`CRQEIterator`], in order to
    /// interoperate with Rust iterators via the [`RQEIterator`] trait.
    ///
    /// # Safety
    ///
    /// 1. `header` is a valid pointer to a [`QueryIterator`] instance,
    ///    and can be converted to a reference.
    /// 2. `header` is an owning pointer, in the same way `Box` owns the
    ///    allocated heap data.
    /// 3. All callbacks are defined (i.e. the function pointers are not NULL)
    /// 4. All callbacks can be safely called, when the right aliasing conditions are
    ///    in place
    #[allow(unused)]
    pub(crate) unsafe fn new(header: NonNull<QueryIterator>) -> Self {
        // SAFETY: the caller is required to uphold `Self::header` field invariants.
        let self_ = Self { header };
        debug_assert!(
            self_.Free.is_some(),
            "The `Free` callback is a NULL function pointer"
        );
        debug_assert!(
            self_.Read.is_some(),
            "The `Read` callback is a NULL function pointer"
        );
        debug_assert!(
            self_.SkipTo.is_some(),
            "The `SkipTo` callback is a NULL function pointer"
        );
        debug_assert!(
            self_.Revalidate.is_some(),
            "The `Revalidate` callback is a NULL function pointer"
        );
        debug_assert!(
            self_.NumEstimated.is_some(),
            "The `NumEstimated` callback is a NULL function pointer"
        );
        debug_assert!(
            self_.Rewind.is_some(),
            "The `Rewind` callback is a NULL function pointer"
        );
        self_
    }

    /// Return a raw pointer to the underlying [`QueryIterator`].
    ///
    /// The caller is taking ownership of the [`QueryIterator`] instance and is
    /// therefore responsible to free its contents.
    #[allow(unused)]
    pub(crate) fn into_raw(self) -> NonNull<QueryIterator> {
        let self_ = ManuallyDrop::new(self);
        self_.header
    }
}

impl<'index> RQEIterator<'index> for CRQEIterator {
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.Read.unwrap_unchecked() };
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_ptr()) };
        #[allow(non_upper_case_globals)]
        match status {
            IteratorStatus_ITERATOR_EOF => Ok(None),
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
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.SkipTo.unwrap_unchecked() };
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_ptr(), doc_id) };
        #[allow(non_upper_case_globals)]
        match status {
            IteratorStatus_ITERATOR_EOF => Ok(None),
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
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.Rewind.unwrap_unchecked() };
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        unsafe { callback(self.header.as_ptr()) };
    }

    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.Revalidate.unwrap_unchecked() };
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_ptr()) };
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
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.NumEstimated.unwrap_unchecked() };
        // SAFETY:
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        unsafe { callback(self.header.as_ptr()) }
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
            // SAFETY:
            // - We have a unique handle over this iterator.
            // - The C code must guarantee, by constructor, that
            //   its `current` field is either NULL or pointer to a
            //   valid instance of an `RSIndexResult`.
            unsafe { self.current.cast::<RSIndexResult<'index>>().as_mut() }
        }
    }
}
