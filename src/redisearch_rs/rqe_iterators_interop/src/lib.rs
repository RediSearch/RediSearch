/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    IteratorStatus, IteratorStatus_ITERATOR_EOF, IteratorStatus_ITERATOR_NOTFOUND,
    IteratorStatus_ITERATOR_OK, IteratorStatus_ITERATOR_TIMEOUT, QueryIterator, ValidateStatus,
    ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_MOVED, ValidateStatus_VALIDATE_OK,
    t_docId,
};
use inverted_index::RSIndexResult;
use rqe_iterators::{RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

#[repr(C)]
/// A wrapper around a Rust iterator—i.e. an implementer of the [`RQEIterator`] trait.
///
/// It allows existing C code to invoke the Rust iterator
/// as if it were a C iterator, conforming to the existing C iterator conventions.
///
/// # Invariants
///
/// 1. It is always safe to cast a raw [`QueryIterator`] pointer returned by [`RQEIteratorWrapper::boxed_new`]
///    to an [`RQEIteratorWrapper`] pointer when invoking one of the callbacks stored in the header.
pub struct RQEIteratorWrapper<E> {
    // The iterator header.
    // It *must* appear first for C-Rust interoperability to work as expected.
    header: QueryIterator,
    pub inner: E,
}

impl<'index, I> RQEIteratorWrapper<I>
where
    I: RQEIterator<'index> + 'index,
{
    /// Create a new C-compatible wrapper around a Rust iterator.
    ///
    /// The wrapper is placed on the heap.
    pub fn boxed_new(type_: ffi::IteratorType, inner: I) -> *mut QueryIterator {
        let mut wrapper = Box::new(Self {
            header: QueryIterator {
                type_,
                atEOF: inner.at_eof(),
                lastDocId: inner.last_doc_id(),
                current: std::ptr::null_mut(),
                NumEstimated: Some(num_estimated::<I>),
                Read: Some(read::<I>),
                SkipTo: Some(skip_to::<I>),
                Revalidate: Some(revalidate::<I>),
                Free: Some(free_iterator::<I>),
                Rewind: Some(rewind::<I>),
            },
            inner,
        });
        if let Some(current) = wrapper
            .inner
            .current()
            .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
        {
            wrapper.header.current = current;
        }
        Box::into_raw(wrapper) as *mut QueryIterator
    }

    /// Convert a type-erased iterator "header" into a wrapper around a specific Rust iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via [`RQEIteratorWrapper::boxed_new`].
    /// 2. The caller must ensure that the provided header matches the expected Rust iterator type.
    /// 3. The caller must ensure that it has a unique handle over the provided header.
    pub const unsafe fn mut_ref_from_header_ptr(
        base: *mut QueryIterator,
    ) -> &'index mut RQEIteratorWrapper<I> {
        debug_assert!(!base.is_null());

        // SAFETY: Guaranteed by 1 + 2.
        let wrapper = unsafe { base.cast::<RQEIteratorWrapper<I>>().as_mut() };

        if cfg!(debug_assertions) {
            wrapper.expect("Unexpected null pointer!")
        } else {
            // SAFETY: Guaranteed by 1.
            unsafe { wrapper.unwrap_unchecked() }
        }
    }

    /// Convert a type-erased iterator "header into a wrapper around a specific Rust iterator type.
    ///
    /// # Safety
    ///
    /// 1. The caller must ensure that the provided header was produced via [`RQEIteratorWrapper::boxed_new`].
    /// 2. The caller must ensure that the provided header matches the expected Rust iterator type.
    pub const unsafe fn ref_from_header_ptr(
        base: *const QueryIterator,
    ) -> &'index RQEIteratorWrapper<I> {
        debug_assert!(!base.is_null());
        // SAFETY: Guaranteed by 1 + 2.
        unsafe {
            base.cast::<RQEIteratorWrapper<I>>()
                .as_ref()
                .expect("Null pointer!")
        }
    }
}

extern "C" fn read<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
) -> IteratorStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    match wrapper.inner.read() {
        Ok(Some(result)) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_OK
        }
        Err(RQEIteratorError::TimedOut) => IteratorStatus_ITERATOR_TIMEOUT,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            wrapper.header.atEOF = true;
            IteratorStatus_ITERATOR_EOF
        }
    }
}

extern "C" fn skip_to<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
    doc_id: t_docId,
) -> IteratorStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    match wrapper.inner.skip_to(doc_id) {
        Ok(Some(SkipToOutcome::Found(result))) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_OK
        }
        Ok(Some(SkipToOutcome::NotFound(result))) => {
            wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
            wrapper.header.lastDocId = result.doc_id;
            IteratorStatus_ITERATOR_NOTFOUND
        }
        Err(RQEIteratorError::TimedOut) => IteratorStatus_ITERATOR_TIMEOUT,
        Err(RQEIteratorError::IoError(_)) => {
            unreachable!(
                "None of the current iterators can fail due to an I/O error, since everything is read from memory"
            )
        }
        Ok(None) => {
            wrapper.header.atEOF = true;
            IteratorStatus_ITERATOR_EOF
        }
    }
}

extern "C" fn revalidate<'index, I: RQEIterator<'index> + 'index>(
    base: *mut QueryIterator,
) -> ValidateStatus {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    match wrapper.inner.revalidate() {
        Ok(RQEValidateStatus::Ok) => ValidateStatus_VALIDATE_OK,
        Ok(RQEValidateStatus::Moved { current }) => {
            if let Some(result) = current {
                wrapper.header.current = result as *mut RSIndexResult as *mut ffi::RSIndexResult;
                wrapper.header.lastDocId = result.doc_id;
            } else {
                wrapper.header.atEOF = true;
            }
            ValidateStatus_VALIDATE_MOVED
        }
        Ok(RQEValidateStatus::Aborted) => ValidateStatus_VALIDATE_ABORTED,
        Err(_) => ValidateStatus_VALIDATE_ABORTED,
    }
}

extern "C" fn rewind<'index, I: RQEIterator<'index> + 'index>(base: *mut QueryIterator) {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::mut_ref_from_header_ptr(base) };
    wrapper.inner.rewind();
    wrapper.header.lastDocId = wrapper.inner.last_doc_id();
    wrapper.header.atEOF = wrapper.inner.at_eof();
    wrapper.header.current = wrapper
        .inner
        .current()
        .map(|c| c as *mut RSIndexResult as *mut ffi::RSIndexResult)
        .unwrap_or(std::ptr::null_mut());
}

extern "C" fn num_estimated<'index, I: RQEIterator<'index> + 'index>(
    base: *const QueryIterator,
) -> usize {
    debug_assert!(!base.is_null());
    debug_assert!(base.is_aligned());
    // SAFETY: Guaranteed by invariant 1. in [`RQEIteratorWrapper`].
    let wrapper = unsafe { RQEIteratorWrapper::<I>::ref_from_header_ptr(base) };
    wrapper.inner.num_estimated()
}

extern "C" fn free_iterator<'index, I: RQEIterator<'index> + 'index>(base: *mut QueryIterator) {
    if !base.is_null() {
        debug_assert!(base.is_aligned());
        // SAFETY: Callbacks are guaranteed to get a header pointer created by
        //  [`RQEIteratorWrapper::new`], which (internally) uses `Box::into_raw` to
        //  return a raw header pointer.
        let _ = unsafe { Box::from_raw(base as *mut RQEIteratorWrapper<I>) };
    }
}
