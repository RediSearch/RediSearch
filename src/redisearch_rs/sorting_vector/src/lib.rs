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
use thin_vec::MediumThinVec;
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
/// # Layout
///
/// This struct is `#[repr(transparent)]` over [`MediumThinVec<RSValueFFI>`], which is
/// pointer-sized (8 bytes). The length is stored in the heap header alongside the data.
///
/// The `MediumThinVec` (i.e. `ThinVec<T, u32>`) heap layout is:
/// ```text
///   Header { len: u32, cap: u32 }  (8 bytes, no padding for pointer-aligned T)
///   data: [RSValueFFI; len]
/// ```
///
/// An empty vector points to a static sentinel header (not null), so no allocation is needed.
#[repr(transparent)]
pub struct RSSortingVector {
    inner: MediumThinVec<RSValueFFI>,
}

impl RSSortingVector {
    /// Creates an empty [`RSSortingVector`] with no allocation.
    ///
    /// The returned vector points to a static sentinel header with `len == 0` and `cap == 0`.
    /// This is the canonical "no sorting vector" sentinel for inline storage in
    /// [`RSDocumentMetadata`].
    pub const fn empty() -> Self {
        Self {
            inner: MediumThinVec::new(),
        }
    }

    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        let mut inner = MediumThinVec::new();
        inner.resize(len, RSValueFFI::null_static());
        Self { inner }
    }

    /// Returns the values as a slice.
    pub fn as_slice(&self) -> &[RSValueFFI] {
        &self.inner
    }

    /// Returns the values as a mutable slice.
    fn as_mut_slice(&mut self) -> &mut [RSValueFFI] {
        &mut self.inner
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
        self.as_mut_slice().iter_mut()
    }

    /// Set a number (double) at the given index
    pub fn try_insert_num(&mut self, idx: usize, num: f64) -> Result<(), IndexOutOfBounds> {
        let spot = self
            .as_mut_slice()
            .get_mut(idx)
            .ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::new_num(num);
        Ok(())
    }

    /// Set a string at the given index.
    pub fn try_insert_string(&mut self, idx: usize, str: Vec<u8>) -> Result<(), IndexOutOfBounds> {
        let spot = self
            .as_mut_slice()
            .get_mut(idx)
            .ok_or(IndexOutOfBounds(()))?;
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
        let spot = self
            .as_mut_slice()
            .get_mut(idx)
            .ok_or(IndexOutOfBounds(()))?;
        *spot = value;
        Ok(())
    }

    /// Set a null value at the given index
    pub fn try_insert_null(&mut self, idx: usize) -> Result<(), IndexOutOfBounds> {
        let spot = self
            .as_mut_slice()
            .get_mut(idx)
            .ok_or(IndexOutOfBounds(()))?;
        *spot = RSValueFFI::null_static();
        Ok(())
    }

    /// Get the len of the sorting vector.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// check if the sorting vector is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Deallocates the inner values buffer and zeros the struct.
    ///
    /// After calling this, the vector is in the same state as [`RSSortingVector::empty()`].
    /// Each [`RSValueFFI`] element is dropped (decrementing its refcount) and the
    /// heap buffer is freed. Calling `clear` on an already-cleared vector is a no-op.
    pub fn clear(&mut self) {
        self.inner.clear();
        // Shrink to free the allocation, returning to the singleton empty state.
        self.inner.shrink_to_fit();
    }

    /// approximate the memory size of the sorting vector.
    ///
    /// The implementation by-passes references in the middle of the chain, so it only counts the size of the final value,
    /// as in C.
    pub fn get_memory_size(&self) -> usize {
        let values = self.as_slice();
        let mut sz = std::mem::size_of_val(values);

        for value in values {
            if value.is_null_static() {
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
}

impl Clone for RSSortingVector {
    fn clone(&self) -> Self {
        // `to_vec()` calls `RSValueFFI::clone` on each element, incrementing refcounts.
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl fmt::Debug for RSSortingVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSSortingVector")
            .field("values", &self.as_slice())
            .field("len", &self.len())
            .finish()
    }
}

impl FromIterator<RSValueFFI> for RSSortingVector {
    fn from_iter<T: IntoIterator<Item = RSValueFFI>>(iter: T) -> Self {
        Self {
            inner: iter.into_iter().collect(),
        }
    }
}

// Consuming iterator: yields owned T by consuming the vector.
impl IntoIterator for RSSortingVector {
    type Item = RSValueFFI;
    type IntoIter = thin_vec::IntoIter<RSValueFFI, u32>;
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
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
        self.as_mut_slice().index_mut(index)
    }
}
