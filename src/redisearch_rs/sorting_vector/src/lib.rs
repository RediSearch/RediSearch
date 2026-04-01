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
    slice::{Iter, IterMut},
};

use icu_casemap::CaseMapper;
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
/// The struct uses `#[repr(C)]` to allow C code to directly access `len` and `values` without FFI
/// function calls. This is critical for performance in the sort comparison hot path.
#[repr(C)]
pub struct RSSortingVector {
    /// Number of elements in the values array.
    pub len: usize,
    /// Pointer to a heap-allocated array of [`RSValueFFI`] elements.
    ///
    /// # Invariant
    ///
    /// When `len > 0`, `values` is a valid, non-null pointer to an allocation of exactly `len`
    /// elements produced by `Vec::into_raw_parts`. When `len == 0`, `values` may be dangling
    /// (from `Vec::new()`).
    pub values: *mut RSValueFFI,
}

// SAFETY: The raw pointer `values` is an owned heap allocation that is never shared.
// RSSortingVector has exclusive ownership of the allocation, similar to `Box<[RSValueFFI]>`.
unsafe impl Send for RSSortingVector {}

// SAFETY: RSSortingVector does not provide interior mutability through shared references.
// All mutation requires `&mut self`.
unsafe impl Sync for RSSortingVector {}

impl RSSortingVector {
    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        let vec = vec![RSValueFFI::null_static(); len];
        let (values, actual_len, _cap) = vec.into_raw_parts();
        debug_assert_eq!(actual_len, len);
        Self { len, values }
    }

    /// Returns a shared slice over the values.
    #[inline]
    fn as_slice(&self) -> &[RSValueFFI] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: `values` points to a valid allocation of `len` elements (struct invariant).
            unsafe { std::slice::from_raw_parts(self.values, self.len) }
        }
    }

    /// Returns a mutable slice over the values.
    #[inline]
    fn as_slice_mut(&mut self) -> &mut [RSValueFFI] {
        if self.len == 0 {
            &mut []
        } else {
            // SAFETY: `values` points to a valid allocation of `len` elements (struct invariant),
            // and we have exclusive access via `&mut self`.
            unsafe { std::slice::from_raw_parts_mut(self.values, self.len) }
        }
    }

    /// Returns an immutable reference to the value at index `index`,
    /// or `None` if the index is out-of-bounds for this sorting vector.
    pub fn get(&self, index: usize) -> Option<&RSValueFFI> {
        self.as_slice().get(index)
    }

    /// Returns an iterator over the values in the sorting vector.
    pub fn iter(&self) -> Iter<'_, RSValueFFI> {
        self.as_slice().iter()
    }

    /// Returns a mutable iterator over the values in the sorting vector.
    pub fn iter_mut(&mut self) -> IterMut<'_, RSValueFFI> {
        self.as_slice_mut().iter_mut()
    }

    /// Set a number (double) at the given index
    pub fn try_insert_num(&mut self, idx: usize, num: f64) -> Result<(), IndexOutOfBounds> {
        let spot = self.as_slice_mut().get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::new_num(num);
        Ok(())
    }

    /// Set a string at the given index.
    pub fn try_insert_string(&mut self, idx: usize, str: Vec<u8>) -> Result<(), IndexOutOfBounds> {
        let spot = self.as_slice_mut().get_mut(idx).ok_or(IndexOutOfBounds(()))?;
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
        let spot = self.as_slice_mut().get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = value;
        Ok(())
    }

    /// Set a null value at the given index
    pub fn try_insert_null(&mut self, idx: usize) -> Result<(), IndexOutOfBounds> {
        let spot = self.as_slice_mut().get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::null_static();
        Ok(())
    }

    /// Get the len of the sorting vector.
    pub fn len(&self) -> usize {
        self.len
    }

    /// check if the sorting vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// approximate the memory size of the sorting vector.
    ///
    /// The implementation by-passes references in the middle of the chain, so it only counts the size of the final value,
    /// as in C.
    pub fn get_memory_size(&self) -> usize {
        let slice = self.as_slice();
        let mut sz = slice.len() * size_of::<RSValueFFI>();

        for idx in 0..slice.len() {
            if slice[idx].is_null() {
                continue;
            }

            sz += RSValueFFI::mem_size();

            // the original behavior would by-pass references in the middle of the chain
            // fixup in: MOD-10347
            let value = slice[idx].deep_deref();

            if value.get_type() == ffi::RSValueType_RSValueType_String {
                sz += value.as_str_bytes().unwrap().len();
            }
        }
        sz
    }
}

impl Clone for RSSortingVector {
    fn clone(&self) -> Self {
        // Clone each element (incrementing refcounts) and collect into a new Vec.
        let cloned: Vec<RSValueFFI> = self.as_slice().iter().cloned().collect();
        let (values, len, _cap) = cloned.into_raw_parts();
        Self { len, values }
    }
}

impl fmt::Debug for RSSortingVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSSortingVector")
            .field("len", &self.len)
            .field("values", &self.as_slice())
            .finish()
    }
}

impl Drop for RSSortingVector {
    fn drop(&mut self) {
        if self.len > 0 {
            // SAFETY: `values` was allocated by `Vec::into_raw_parts` with exactly `len` elements
            // and capacity equal to `len`. Reconstructing the Vec and dropping it frees the
            // allocation and drops each `RSValueFFI` (decrementing refcounts).
            let _ = unsafe { Vec::from_raw_parts(self.values, self.len, self.len) };
        }
    }
}

impl FromIterator<RSValueFFI> for RSSortingVector {
    fn from_iter<T: IntoIterator<Item = RSValueFFI>>(iter: T) -> Self {
        let vec: Vec<RSValueFFI> = iter.into_iter().collect();
        let (values, len, _cap) = vec.into_raw_parts();
        Self { len, values }
    }
}

// Consuming iterator: yields owned T by consuming the vector.
impl IntoIterator for RSSortingVector {
    type Item = RSValueFFI;
    type IntoIter = std::vec::IntoIter<RSValueFFI>;
    fn into_iter(self) -> Self::IntoIter {
        let len = self.len;
        let values = self.values;
        // Prevent Drop from running (which would free the same allocation).
        std::mem::forget(self);

        if len == 0 {
            return Vec::new().into_iter();
        }

        // SAFETY: `values` was produced by `Vec::into_raw_parts` with capacity == len.
        let vec = unsafe { Vec::from_raw_parts(values, len, len) };
        vec.into_iter()
    }
}

// implementing Index opens the usage of the bracket operators `[]` to access elements in the sorting vector.
impl<I> Index<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValueFFI]>,
{
    type Output = <[RSValueFFI] as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.as_slice().index(index)
    }
}

// implementing IndexMut opens the usage of the bracket operators `[]` to mutate elements in the sorting vector.
impl<I> IndexMut<I> for RSSortingVector
where
    I: std::slice::SliceIndex<[RSValueFFI]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.as_slice_mut().index_mut(index)
    }
}
