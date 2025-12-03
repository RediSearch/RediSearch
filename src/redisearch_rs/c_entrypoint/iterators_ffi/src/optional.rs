use std::ptr::NonNull;

use ffi::{IteratorType_OPTIONAL_ITERATOR, QueryIterator, t_docId};
use rqe_iterators::{RQEIterator, optional::Optional};

use crate::{c2rust::CRQEIterator, wrapper::RQEIteratorWrapper};

#[unsafe(no_mangle)]
/// Create a new non-optimized optional iterator.
///
/// # Safety
///
/// 1. `child_it` must be a valid pointer to an implementation of the C query iterator API.
/// 2. `child_it` is not null.
/// 3. `child_it` must not be aliased.
pub unsafe extern "C" fn NewNonOptimizedOptionalIterator(
    child: *mut QueryIterator,
    max_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    let child = NonNull::new(child).expect(
        "Trying to create a non-optimized optional iterator using a NULL child iterator pointer",
    );
    let child = unsafe { CRQEIterator::new(child) };
    RQEIteratorWrapper::boxed_new(
        IteratorType_OPTIONAL_ITERATOR,
        OptionalWrapper {
            is_optimized: false,
            it: Optional::new(max_id, weight, child),
        },
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetNonOptimizedOptionalIteratorChild(
    header: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_ITERATOR,
        "Expected an optional iterator"
    );
    // TODO: check `is_optimized`
    let wrapper =
        unsafe { RQEIteratorWrapper::<OptionalWrapper<CRQEIterator>>::ref_from_header_ptr(header) };
    wrapper
        .inner
        .it
        .child()
        .map(|p| p.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn TakeNonOptimizedOptionalIteratorChild(
    header: *mut QueryIterator,
) -> *mut QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_ITERATOR,
        "Expected an optional iterator"
    );
    // TODO: check `is_optimized`
    let wrapper = unsafe {
        RQEIteratorWrapper::<OptionalWrapper<CRQEIterator>>::mut_ref_from_header_ptr(header)
    };
    wrapper
        .inner
        .it
        .take_child()
        .map(|p| p.into_raw().as_ptr())
        .unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetNonOptimizedOptionalIteratorChild(
    header: *mut QueryIterator,
    child: *mut QueryIterator,
) {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        unsafe { *header }.type_,
        IteratorType_OPTIONAL_ITERATOR,
        "Expected an optional iterator"
    );
    // TODO: check `is_optimized`
    let wrapper = unsafe {
        RQEIteratorWrapper::<OptionalWrapper<CRQEIterator>>::mut_ref_from_header_ptr(header)
    };
    let child = NonNull::new(child)
        .expect("Trying to set a NULL child for a non-optimized optional iterator");
    let child = unsafe { CRQEIterator::new(child) };
    wrapper.inner.it.set_child(child);
}

#[repr(C)]
pub struct OptionalWrapper<'index, I> {
    /// A flag to distinguish the Rust- and C- based impls from each other.
    pub is_optimized: bool,
    pub it: Optional<'index, I>,
}

impl<'index, I> RQEIterator<'index> for OptionalWrapper<'index, I>
where
    I: RQEIterator<'index>,
{
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        self.it.read()
    }

    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        self.it.skip_to(doc_id)
    }

    fn rewind(&mut self) {
        self.it.rewind()
    }

    fn num_estimated(&self) -> usize {
        self.it.num_estimated()
    }

    fn last_doc_id(&self) -> t_docId {
        self.it.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.it.at_eof()
    }

    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        self.it.current()
    }

    fn revalidate(
        &mut self,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        self.it.revalidate()
    }
}
