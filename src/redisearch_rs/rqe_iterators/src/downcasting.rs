use std::ffi::c_void;

use crate::RQEIterator;
use rqe_iterator_type::IteratorType;

/// A marker trait to associate an [`RQEIterator`] implementation
/// to a type tag, at compile-time.
///
/// # Downcasting
///
/// This trait exists to support [`downcast_iterator_as_ref`].
///
/// For each variant in [`IteratorType`], there should be at most one
/// implementation of [`IteratorTypeKnownAtCompileTime`] using it as
/// its [`Self::TYPE`] value.
pub trait IteratorTypeKnownAtCompileTime {
    /// The type of this iterator, known at compile-time.
    const TYPE: IteratorType;
}

/// Downcast a reference to a type-erased [`RQEIterator`] into a reference to
/// a concrete type.
///
/// It returns `None` if [`T::TYPE`] doesn't match the iterator type of
/// `&dyn RQEIterator`, or if the input doesn't support downcasting.
pub fn downcast_iterator_as_ref<'a, 'b, T>(i: &'a dyn RQEIterator<'b>) -> Option<&'a T>
where
    T: IteratorTypeKnownAtCompileTime + RQEIterator<'b>,
{
    let raw = i.downcast_as_ref_raw(T::TYPE);
    if raw.is_null() {
        None
    } else {
        // SAFETY: implementor's contract guarantees that a non-None return
        // is a valid pointer to T, and the borrow on `iter` keeps it alive.
        Some(unsafe { &*(raw as *const T) })
    }
}

/// An implementation of [`RQEIterator::downcast_as_ref_raw`] that's suitable for
/// all non-wrapping implementors of the trait, as long as they don't have
/// unassigned type parameters.
pub fn downcast_as_ref_raw<'a, I>(self_: &I, type_: IteratorType) -> *const c_void
where
    I: IteratorTypeKnownAtCompileTime + RQEIterator<'a>,
{
    (I::TYPE == type_)
        .then(|| std::ptr::from_ref(self_).cast())
        .unwrap_or(std::ptr::null())
}
