/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    fmt,
    ops::{Index, IndexMut},
    ptr::NonNull,
    slice::{Iter, IterMut},
};

use icu_casemap::CaseMapper;
use thin_vec::{SmallThinVec, VecCapacity, header::Header};
use value::RSValueFFI;

/// IndexOutOfBounds error can be returned by [`RSSortingVector::try_insert_num`] and the other `try_insert_*` methods.
///
/// In case for debug builds, it contains the index and the length of the vector for better debugging but has zero size in release builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexOutOfBounds(());

impl fmt::Display for IndexOutOfBounds {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Index out of bounds")
    }
}

impl std::error::Error for IndexOutOfBounds {}

/// [`RSSortingVector`] acts as a cache for sortable fields in a document.
///
/// It has a constant length, determined upfront on creation. It can't be resized.
/// The [`RSSortingVector`] may contain values of different types, such as numbers, strings, or references to other values.
/// This depends on the fields in the source document.
///
/// The fields in the sorting vector occur in the same order as they appeared in the document. Fields that are not sortable,
/// are not added at all to the sorting vector, i.e. the sorting vector does not contain null values for non-sortable fields.
///
/// # Memory layout
///
/// `RSSortingVector` is backed by a [`SmallThinVec`] which stores `{len(u16), cap(u16), [RSValueFFI; N]}`
/// in a single heap allocation behind one pointer. When exposed to C via FFI, the raw header pointer
/// is passed directly — no outer `Box` wrapper — giving one allocation and one dereference.
#[derive(Debug, Clone)]
pub struct RSSortingVector {
    values: SmallThinVec<RSValueFFI>,
}

impl RSSortingVector {
    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        let mut values = SmallThinVec::new();
        values.resize(len, RSValueFFI::null_static());
        Self { values }
    }

    /// Returns an immutable reference to the value at index `index`,
    /// or `None` if the index is out-of-bounds for this sorting vector.
    pub fn get(&self, index: usize) -> Option<&RSValueFFI> {
        self.values.get(index)
    }

    /// Returns an iterator over the values in the sorting vector.
    pub fn iter(&self) -> Iter<'_, RSValueFFI> {
        self.values.iter()
    }

    /// Returns a mutable iterator over the values in the sorting vector.
    pub fn iter_mut(&mut self) -> IterMut<'_, RSValueFFI> {
        self.values.iter_mut()
    }

    /// Set a number (double) at the given index
    pub fn try_insert_num(&mut self, idx: usize, num: f64) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::new_num(num);
        Ok(())
    }

    /// Set a string at the given index.
    pub fn try_insert_string(&mut self, idx: usize, str: Vec<u8>) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::new_string(str);
        Ok(())
    }

    /// Set a string at the given index, the string is normalized before being set.
    pub fn try_insert_string_normalize(
        &mut self,
        idx: usize,
        str: impl AsRef<str>,
    ) -> Result<(), IndexOutOfBounds> {
        let casemapper = CaseMapper::new();

        let normalized = casemapper.fold_string(str.as_ref()).into_owned();

        self.try_insert_string(idx, normalized.into_bytes())
    }

    /// Set a value at the given index
    pub fn try_insert_val(
        &mut self,
        idx: usize,
        value: RSValueFFI,
    ) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = value;
        Ok(())
    }

    /// Set a null value at the given index
    pub fn try_insert_null(&mut self, idx: usize) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::null_static();
        Ok(())
    }

    /// Get the len of the sorting vector.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// check if the sorting vector is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// approximate the memory size of the sorting vector.
    ///
    /// The implementation by-passes references in the middle of the chain, so it only counts the size of the final value,
    /// as in C.
    pub fn get_memory_size(&self) -> usize {
        Self::compute_memory_size(&self.values)
    }

    /// Compute the memory size of a slice of [`RSValueFFI`] values.
    ///
    /// This is a static version of [`RSSortingVector::get_memory_size`] that can be used with
    /// borrowed slices from raw pointers.
    pub fn compute_memory_size(values: &[RSValueFFI]) -> usize {
        let mut sz = std::mem::size_of_val(values);

        for value in values {
            if value.is_null() {
                continue;
            }

            sz += RSValueFFI::mem_size();

            // the original behavior would by-pass references in the middle of the chain
            // fixup in: MOD-10347
            let value = value.deep_deref();

            if value.get_type() == ffi::RSValueType_RSValueType_String {
                sz += value.as_str_bytes().unwrap().len();
            }
        }
        sz
    }

    /// Returns the raw header pointer of the backing [`SmallThinVec`] without consuming the
    /// `RSSortingVector`. This is useful for storing the pointer in contexts where the sorting
    /// vector is borrowed (e.g., `RLookupRow`).
    pub const fn as_raw_ptr(&self) -> *const Header<u16> {
        self.values.as_header_ptr().as_ptr()
    }

    /// Consumes the `RSSortingVector` and returns a raw pointer to the backing [`SmallThinVec`]
    /// header allocation.
    ///
    /// The caller is responsible for eventually calling [`RSSortingVector::from_raw_ptr`] to free
    /// the memory.
    pub fn into_raw_ptr(self) -> NonNull<Header<u16>> {
        self.values.into_raw()
    }

    /// Reconstructs an `RSSortingVector` from a raw header pointer previously obtained via
    /// [`RSSortingVector::into_raw_ptr`].
    ///
    /// # Safety
    ///
    /// - `ptr` must have been obtained from [`RSSortingVector::into_raw_ptr`] and must not have
    ///   been freed.
    pub const unsafe fn from_raw_ptr(ptr: NonNull<Header<u16>>) -> Self {
        Self {
            // SAFETY: The caller guarantees the pointer was obtained from into_raw_ptr.
            values: unsafe { SmallThinVec::from_raw(ptr) },
        }
    }

    /// Borrows the data behind a raw header pointer as a shared slice, without taking ownership.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been obtained from [`RSSortingVector::into_raw_ptr`] and must not have
    ///   been freed.
    /// - The returned slice must not outlive the allocation.
    /// - No mutable references to the data may exist for the duration of the returned borrow.
    pub unsafe fn borrow_values_from_raw<'a>(ptr: *const Header<u16>) -> &'a [RSValueFFI] {
        // SAFETY: The caller guarantees the pointer is valid and was produced by into_raw_ptr.
        unsafe { SmallThinVec::<RSValueFFI>::borrow_from_raw(ptr) }
    }

    /// Borrows the data behind a raw header pointer as a mutable slice, without taking ownership.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been obtained from [`RSSortingVector::into_raw_ptr`] and must not have
    ///   been freed.
    /// - The returned slice must not outlive the allocation.
    /// - No other references to the data may exist for the duration of the returned borrow.
    pub unsafe fn borrow_values_mut_from_raw<'a>(ptr: *mut Header<u16>) -> &'a mut [RSValueFFI] {
        // SAFETY: The caller guarantees the pointer is valid and exclusively borrowed.
        unsafe { SmallThinVec::<RSValueFFI>::borrow_mut_from_raw(ptr) }
    }

    /// Borrows values from an opaque raw pointer (as used by C FFI).
    ///
    /// This is a convenience wrapper around [`Self::borrow_values_from_raw`] that casts from the
    /// opaque `*const ()` type used in `RLookupRow` to the concrete header pointer type.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::borrow_values_from_raw`].
    pub unsafe fn borrow_values_from_opaque_ptr<'a>(ptr: *const ()) -> &'a [RSValueFFI] {
        // SAFETY: The caller guarantees the pointer is a valid header pointer.
        unsafe { Self::borrow_values_from_raw(ptr as *const Header<u16>) }
    }

    /// Returns the length stored in the header at the given opaque raw pointer, without borrowing
    /// the data slice.
    ///
    /// # Safety
    ///
    /// - `ptr` must have been obtained from [`RSSortingVector::into_raw_ptr`] (or
    ///   [`RSSortingVector::as_raw_ptr`]) and must not have been freed.
    pub unsafe fn len_from_opaque_ptr(ptr: *const ()) -> usize {
        // SAFETY: The caller guarantees the pointer is a valid header pointer.
        let header = unsafe { &*(ptr as *const Header<u16>) };
        header.len().to_usize()
    }
}

impl FromIterator<RSValueFFI> for RSSortingVector {
    fn from_iter<T: IntoIterator<Item = RSValueFFI>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}

// Consuming iterator: yields owned T by consuming the vector.
impl IntoIterator for RSSortingVector {
    type Item = RSValueFFI;
    type IntoIter = thin_vec::IntoIter<RSValueFFI, u16>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

// implementing Index opens the usage of the bracket operators `[]` to access elements in the sorting vector.
impl<I> Index<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValueFFI]>,
{
    type Output = <[RSValueFFI] as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.values.index(index)
    }
}

// implementing IndexMut opens the usage of the bracket operators `[]` to mutate elements in the sorting vector.
impl<I> IndexMut<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValueFFI]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.values.index_mut(index)
    }
}
