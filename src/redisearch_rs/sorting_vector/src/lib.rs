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
/// # Layout
///
/// This struct is `#[repr(C)]` so that C code can directly access its fields for
/// simple read operations (length, element access) without FFI call overhead.
#[repr(C)]
pub struct RSSortingVector {
    /// Pointer to an array of [`RSValueFFI`] values.
    ///
    /// In C this field is `RSValue **values`. The layout is identical because
    /// [`RSValueFFI`] is `#[repr(transparent)]` over `NonNull<ffi::RSValue>`,
    /// which has the same size and alignment as `*mut ffi::RSValue`
    /// (verified by const assertions in `rs_value_ffi.rs`).
    values: *mut RSValueFFI,
    /// Number of elements in the array.
    len: usize,
}

impl RSSortingVector {
    /// Creates an empty [`RSSortingVector`] with no allocation.
    ///
    /// The returned vector has a null `values` pointer and zero length.
    /// This is the canonical "no sorting vector" sentinel for inline storage.
    pub const fn empty() -> Self {
        Self {
            values: std::ptr::null_mut(),
            len: 0,
        }
    }

    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        Self::from_vec(vec![RSValueFFI::null_static(); len])
    }

    /// Constructs from a `Vec`, taking ownership of its buffer.
    fn from_vec(v: Vec<RSValueFFI>) -> Self {
        let mut boxed = v.into_boxed_slice();
        let ptr = boxed.as_mut_ptr();
        let len = boxed.len();
        std::mem::forget(boxed);
        Self { values: ptr, len }
    }

    /// Returns the values as a slice.
    ///
    /// Returns an empty slice if the vector is empty or has been cleared.
    pub const fn as_slice(&self) -> &[RSValueFFI] {
        if self.values.is_null() {
            return &[];
        }
        // SAFETY: `values` and `len` were constructed from a valid `Vec<RSValueFFI>`.
        // The pointer is valid for `len` elements as long as `self` is alive.
        unsafe { std::slice::from_raw_parts(self.values, self.len) }
    }

    /// Returns the values as a mutable slice.
    ///
    /// Returns an empty slice if the vector is empty or has been cleared.
    const fn as_mut_slice(&mut self) -> &mut [RSValueFFI] {
        if self.values.is_null() {
            return &mut [];
        }
        // SAFETY: Same as `as_slice`, plus we have exclusive access via `&mut self`.
        unsafe { std::slice::from_raw_parts_mut(self.values, self.len) }
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
    pub const fn len(&self) -> usize {
        self.len
    }

    /// check if the sorting vector is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Deallocates the inner values buffer and zeros the struct.
    ///
    /// After calling this, the vector is in the same state as [`RSSortingVector::empty()`].
    /// Each [`RSValueFFI`] element is dropped (decrementing its refcount) and the
    /// heap buffer is freed. Calling `clear` on an already-cleared vector is a no-op.
    pub fn clear(&mut self) {
        if !self.values.is_null() {
            // SAFETY: `values` and `len` came from a `Vec<RSValueFFI>` via `into_boxed_slice`,
            // so capacity == len. Reconstructing the Vec lets it drop each `RSValueFFI`
            // (decrementing refcounts) and deallocate the buffer.
            unsafe {
                let _ = Vec::from_raw_parts(self.values, self.len, self.len);
            }
        }
        self.values = std::ptr::null_mut();
        self.len = 0;
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

impl Drop for RSSortingVector {
    fn drop(&mut self) {
        if self.values.is_null() {
            return;
        }
        // SAFETY: `values` and `len` came from a `Vec<RSValueFFI>` via `into_boxed_slice`,
        // so capacity == len. Reconstructing the Vec lets it drop each `RSValueFFI`
        // (decrementing refcounts) and deallocate the buffer.
        unsafe {
            let _ = Vec::from_raw_parts(self.values, self.len, self.len);
        };
    }
}

impl Clone for RSSortingVector {
    fn clone(&self) -> Self {
        // `to_vec()` calls `RSValueFFI::clone` on each element, incrementing refcounts.
        Self::from_vec(self.as_slice().to_vec())
    }
}

impl fmt::Debug for RSSortingVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSSortingVector")
            .field("values", &self.as_slice())
            .field("len", &self.len)
            .finish()
    }
}

impl FromIterator<RSValueFFI> for RSSortingVector {
    fn from_iter<T: IntoIterator<Item = RSValueFFI>>(iter: T) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

// Consuming iterator: yields owned T by consuming the vector.
impl IntoIterator for RSSortingVector {
    type Item = RSValueFFI;
    type IntoIter = std::vec::IntoIter<RSValueFFI>;
    fn into_iter(self) -> Self::IntoIter {
        if self.values.is_null() {
            std::mem::forget(self);
            return Vec::new().into_iter();
        }
        // SAFETY: `values` and `len` came from a `Vec<RSValueFFI>` with capacity == len.
        let v = unsafe { Vec::from_raw_parts(self.values, self.len, self.len) };
        // Prevent `Drop` from double-freeing the buffer.
        std::mem::forget(self);
        v.into_iter()
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
