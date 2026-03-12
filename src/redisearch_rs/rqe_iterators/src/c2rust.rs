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

use crate::IteratorType;
use inverted_index::RSIndexResult;
use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use crate::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, interop::RQEIteratorWrapper,
    not::Not, optional::Optional, profile::Profilable,
};

/// A Rust shim over a query iterator that satisfies the C iterator API.
///
/// If you squint a bit, this is a C-flavored version of a `Box<dyn [`RQEIterator`]>`,
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
pub struct CRQEIterator {
    /// # Safety invariants
    ///
    /// 1. [`Self::header`] is a valid pointer to a [`QueryIterator`] instance,
    ///    and can be converted to a reference.
    /// 2. [`Self::header`] is an owning pointer, in the same way `Box` owns the
    ///    allocated heap data.
    /// 3. All callbacks are defined (i.e. the function pointers are not NULL),
    ///    with the exception of `SkipTo`, which is optional.
    /// 4. All callbacks can be safely called, when the right aliasing conditions are
    ///    in place
    header: NonNull<QueryIterator>,
}

impl AsRef<QueryIterator> for CRQEIterator {
    fn as_ref(&self) -> &QueryIterator {
        // SAFETY: We can convert to a reference thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a shared reference
        // since [`CRQEIterator::header`] owns the iterator (invariant 2.) and
        // this methods takes a shared reference to `self`, thus ensuring that
        // no mutable reference is live at the same time.
        unsafe { self.header.as_ref() }
    }
}

impl AsMut<QueryIterator> for CRQEIterator {
    fn as_mut(&mut self) -> &mut QueryIterator {
        // SAFETY: We can convert to a reference thanks to invariant 1. of
        // [`CRQEIterator::header`]. It is safe to create a mutable reference
        // since [`CRQEIterator::header`] owns the iterator (invariant 2.) and
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
        // to the underlying iterator since [`CRQEIterator::header`] owns the iterator
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
        // to the underlying iterator since [`CRQEIterator::header`] owns the iterator
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
    /// 3. All callbacks are defined (i.e. the function pointers are not NULL),
    ///    with the exception of `SkipTo`, which is optional.
    /// 4. All callbacks can be safely called, when the right aliasing conditions are
    ///    in place
    pub unsafe fn new(header: NonNull<QueryIterator>) -> Self {
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
    pub fn into_raw(self) -> NonNull<QueryIterator> {
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
        #[expect(non_upper_case_globals)]
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
        let callback = self
            .SkipTo
            .expect("The `SkipTo` callback is a NULL function pointer");
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_ptr(), doc_id) };
        #[expect(non_upper_case_globals)]
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

    fn revalidate(&mut self) -> Result<crate::RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Safe thanks to invariant 3. of [`CRQEIterator::header`].
        let callback = unsafe { self.Revalidate.unwrap_unchecked() };
        // SAFETY:
        // - We have a unique handle over this iterator.
        // - The C code must guarantee, by constructor, that callbacks
        //   can be called on types that implement its C iterator API.
        let status = unsafe { callback(self.header.as_ptr()) };
        #[expect(non_upper_case_globals)]
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

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        self.type_
    }

    fn as_c_iterator(&self) -> Option<&CRQEIterator> {
        Some(self)
    }
}

/// Profile-wrap a child [`CRQEIterator`] by calling [`Profilable::into_profiled`]
/// and boxing the result back into a [`CRQEIterator`].
fn profile_child(child: CRQEIterator) -> CRQEIterator {
    let profiled = child.into_profiled();
    let ptr = RQEIteratorWrapper::boxed_new(IteratorType::Profile, profiled);
    // SAFETY: `boxed_new` returns a valid, owning, non-null pointer.
    let ptr = unsafe { NonNull::new_unchecked(ptr) };
    // SAFETY: `ptr` is valid and owning per above.
    unsafe { CRQEIterator::new(ptr) }
}

impl Profilable<'_> for CRQEIterator {
    type Profiled = Self;

    /// Profile the subtree rooted at this iterator — wrapping every
    /// child node — **without** wrapping `self`.
    ///
    /// Dispatches on the iterator type tag to handle each variant:
    /// - Rust composite iterators ([`Not`], [`Optional`]): children
    ///   are extracted, recursively profiled via `profile_child`, and set back.
    /// - C-native composite iterators (optimized Not/Optional, Hybrid,
    ///   Optimus, Union, C Intersection): children are accessed through
    ///   partial `repr(C)` struct layouts, recursively profiled, and written
    ///   back.
    /// - Leaf iterators: returned unchanged (no children to recurse into).
    fn profile_children(self) -> Self {
        let type_ = self.type_;

        match type_ {
            IteratorType::Not => {
                // SAFETY:
                // - Type tag guarantees this is a Not<CRQEIterator> wrapper.
                // - `into_raw()` consumed `self`, so no other reference exists;
                //   exclusive access is guaranteed.
                let wrapper = unsafe {
                    RQEIteratorWrapper::<Not<CRQEIterator>>::mut_ref_from_header_ptr(
                        self.into_raw().as_ptr(),
                    )
                };

                if let Some(child) = wrapper.inner.take_child() {
                    wrapper.inner.set_child(profile_child(child));
                }

                // SAFETY: RQEIteratorWrapper is #[repr(C)] with QueryIterator as
                // its first field, so the pointer cast is valid. We still own
                // the allocation; re-wrap as CRQEIterator.
                let ptr =
                    // SAFETY: wrapper came from `boxed_new`, so non-null.
                    unsafe { NonNull::new_unchecked(wrapper as *mut _ as *mut QueryIterator) };
                // SAFETY: ptr is valid and owning per above.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::NotOptimized => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is a NotIteratorOptimized.
                let ni = unsafe { &mut *(ptr as *mut ffi::NotIteratorOptimized) };
                if !ni.child.is_null() {
                    // SAFETY: child pointer is non-null (just checked).
                    let child_ptr = unsafe { NonNull::new_unchecked(ni.child) };
                    // SAFETY: child is a valid, owning pointer.
                    let child = unsafe { CRQEIterator::new(child_ptr) };
                    ni.child = std::ptr::null_mut();
                    ni.child = profile_child(child).into_raw().as_ptr();
                }
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::Optional => {
                // SAFETY:
                // - Type tag guarantees this is an Optional<CRQEIterator> wrapper.
                // - `into_raw()` consumed `self`, so no other reference exists;
                //   exclusive access is guaranteed.
                let wrapper = unsafe {
                    RQEIteratorWrapper::<Optional<CRQEIterator>>::mut_ref_from_header_ptr(
                        self.into_raw().as_ptr(),
                    )
                };

                if let Some(child) = wrapper.inner.take_child() {
                    wrapper.inner.set_child(profile_child(child));
                }

                // SAFETY: RQEIteratorWrapper is #[repr(C)] with QueryIterator as
                // its first field, so the pointer cast is valid. We still own
                // the allocation; re-wrap as CRQEIterator.
                let ptr =
                    // SAFETY: wrapper came from `boxed_new`, so non-null.
                    unsafe { NonNull::new_unchecked(wrapper as *mut _ as *mut QueryIterator) };
                // SAFETY: ptr is valid and owning per above.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::OptionalOptimized => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is an OptionalOptimizedIterator.
                let oi = unsafe { &mut *(ptr as *mut ffi::OptionalOptimizedIterator) };
                if !oi.child.is_null() {
                    // SAFETY: child pointer is non-null (just checked).
                    let child_ptr = unsafe { NonNull::new_unchecked(oi.child) };
                    // SAFETY: child is a valid, owning pointer.
                    let child = unsafe { CRQEIterator::new(child_ptr) };
                    oi.child = std::ptr::null_mut();
                    oi.child = profile_child(child).into_raw().as_ptr();
                }
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::Intersect => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is a C IntersectionIterator.
                let ii = unsafe { &mut *(ptr as *mut ffi::IntersectionIterator) };
                for i in 0..ii.num_its as usize {
                    // SAFETY: `its` is a valid array of `num_its` pointers.
                    let slot = unsafe { ii.its.add(i) };
                    // SAFETY: slot is within bounds per above.
                    let child_ptr = unsafe { *slot };
                    if !child_ptr.is_null() {
                        // SAFETY: child pointer is non-null (just checked).
                        let child_ptr = unsafe { NonNull::new_unchecked(child_ptr) };
                        // SAFETY: child is a valid, owning pointer.
                        let child = unsafe { CRQEIterator::new(child_ptr) };
                        // SAFETY: writing back to the same slot.
                        unsafe { *slot = profile_child(child).into_raw().as_ptr() };
                    }
                }
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::Union => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is a UnionIterator.
                let ui = unsafe { &mut *(ptr as *mut ffi::UnionIterator) };
                for i in 0..ui.num_orig as usize {
                    // SAFETY: `its_orig` is a valid array of `num_orig` pointers.
                    let slot = unsafe { ui.its_orig.add(i) };
                    // SAFETY: slot is within bounds per above.
                    let child_ptr = unsafe { *slot };
                    if !child_ptr.is_null() {
                        // SAFETY: child pointer is non-null (just checked).
                        let child_ptr = unsafe { NonNull::new_unchecked(child_ptr) };
                        // SAFETY: child is a valid, owning pointer.
                        let child = unsafe { CRQEIterator::new(child_ptr) };
                        // SAFETY: writing back to the same slot.
                        unsafe { *slot = profile_child(child).into_raw().as_ptr() };
                    }
                }
                // SAFETY: `ptr` is a valid UnionIterator allocated by C.
                // We only modified `its_orig` entries (in-place); `its`,
                // `heap_min_id`, and `num` are untouched and remain valid.
                unsafe { ffi::UI_SyncIterList(ptr.cast()) };
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::Hybrid => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is a HybridIterator.
                let hi = unsafe { &mut *(ptr as *mut ffi::HybridIterator) };
                if !hi.child.is_null() {
                    // SAFETY: child pointer is non-null (just checked).
                    let child_ptr = unsafe { NonNull::new_unchecked(hi.child) };
                    // SAFETY: child is a valid, owning pointer.
                    let child = unsafe { CRQEIterator::new(child_ptr) };
                    hi.child = std::ptr::null_mut();
                    hi.child = profile_child(child).into_raw().as_ptr();
                }
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            IteratorType::Optimus => {
                let ptr = self.into_raw().as_ptr();
                // SAFETY: type tag guarantees this is an OptimizerIterator.
                let oi = unsafe { &mut *(ptr as *mut ffi::OptimizerIterator) };
                if !oi.child.is_null() {
                    // SAFETY: child pointer is non-null (just checked).
                    let child_ptr = unsafe { NonNull::new_unchecked(oi.child) };
                    // SAFETY: child is a valid, owning pointer.
                    let child = unsafe { CRQEIterator::new(child_ptr) };
                    oi.child = std::ptr::null_mut();
                    oi.child = profile_child(child).into_raw().as_ptr();
                }
                // SAFETY: ptr is non-null (came from `into_raw`).
                let ptr = unsafe { NonNull::new_unchecked(ptr) };
                // SAFETY: we still own the pointer.
                unsafe { CRQEIterator::new(ptr) }
            }
            // Rust leaf iterators — no children to recurse into.
            IteratorType::Wildcard
            | IteratorType::InvIdxNumeric
            | IteratorType::InvIdxTerm
            | IteratorType::InvIdxWildcard
            | IteratorType::InvIdxMissing
            | IteratorType::InvIdxTag
            | IteratorType::Empty
            | IteratorType::IdListSorted
            | IteratorType::IdListUnsorted
            | IteratorType::MetricSortedById
            | IteratorType::MetricSortedByScore => self,
            IteratorType::Profile => {
                unreachable!("profile_children called on an already-profiled iterator")
            }
            IteratorType::Max => unreachable!("Unexpected iterator type: {type_}"),
        }
    }
}
