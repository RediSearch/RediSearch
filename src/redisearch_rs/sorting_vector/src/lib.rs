/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    fmt::Display,
    ops::{Index, IndexMut},
    slice::{Iter, IterMut},
};

use icu_casemap::CaseMapper;
use value::RSValueTrait;

/// IndexOutOfBounds error can be returned by [`RSSortingVector::try_insert_num`] and the other `try_insert_*` methods.
///
/// In case for debug builds, it contains the index and the length of the vector for better debugging but has zero size in release builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexOutOfBounds(());

impl Display for IndexOutOfBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RSSortingVector<T: RSValueTrait> {
    values: Box<[T]>,
}

impl<T: RSValueTrait> RSSortingVector<T> {
    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        Self {
            values: vec![T::create_null(); len].into_boxed_slice(),
        }
    }

    /// Returns an immutable reference to the value at index `index`,
    /// or `None` if the index is out-of-bounds for this sorting vector.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.values.get(index)
    }

    /// Returns an iterator over the values in the sorting vector.
    pub fn iter(&self) -> Iter<'_, T> {
        self.values.iter()
    }

    /// Returns a mutable iterator over the values in the sorting vector.
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        self.values.iter_mut()
    }

    /// Set a number (double) at the given index
    pub fn try_insert_num(&mut self, idx: usize, num: f64) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = T::create_num(num);
        Ok(())
    }

    /// Set a string at the given index, the string is normalized before being set.
    pub fn try_insert_string<S: AsRef<str>>(
        &mut self,
        idx: usize,
        str: S,
    ) -> Result<(), IndexOutOfBounds> {
        let rs_value_from_c = T::is_ptr_type();
        if rs_value_from_c {
            // If the RSValue type is owned and allocated by C, we cannot allocate parts of it in Rust yet.
            // See MOD-10347 for details.
            // See the details about case folding below too.
            unimplemented!("We cannot yet allocate RSValues in Rust. See MOD-10347 for details.");
        }

        let casemapper = CaseMapper::new();

        // In Rust we will use ICU4X for case folding. This has been done in C by a different library, called
        // lib_nu. Before we switch to use this code in production we need to make sure that the behavior is the same.
        // See MOD-10320 for details.
        let normalized = casemapper.fold_string(str.as_ref()).into_owned();
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = T::create_string(normalized);
        Ok(())
    }

    /// Set a value at the given index
    pub fn try_insert_val(&mut self, idx: usize, value: T) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = value;
        Ok(())
    }

    /// Set a reference to the value at the given index
    pub fn try_insert_val_as_ref(&mut self, idx: usize, value: T) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = T::create_ref(value);
        Ok(())
    }

    /// Set a null value at the given index
    pub fn try_insert_null(&mut self, idx: usize) -> Result<(), IndexOutOfBounds> {
        let spot = self.values.get_mut(idx).ok_or(IndexOutOfBounds(()))?;
        *spot = T::create_null();
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
    /// as in C. We have another implementation for the Rust type based on it's [RSValueTrait] implementation.
    pub fn get_memory_size(&self) -> usize {
        let mut sz = if T::is_ptr_type() {
            // Each RSValue is a pointer, so we multiply by the size of a pointer
            self.values.len() * std::mem::size_of::<*const T>()
        } else {
            // Each RSValue is in the heap array, so we multiply by the size of the type
            self.values.len() * T::mem_size()
        };

        for idx in 0..self.values.len() {
            if self.values[idx].is_null() {
                continue;
            }
            if T::is_ptr_type() {
                // count the size of the struct if not null, like in C we add the pointer and the struct size
                sz += T::mem_size();
            }

            // the original behavior would by-pass references in the middle of the chain
            // fixup in: MOD-10347
            let value = walk_down_rsvalue_ref_chain(&self.values[idx]);

            if value.get_type() == ffi::RSValueType_RSValueType_String {
                sz += value.as_str().unwrap().len();
            }
        }
        sz
    }
}

impl<A: RSValueTrait> FromIterator<A> for RSSortingVector<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        Self {
            values: iter.into_iter().collect(),
        }
    }
}

// Consuming iterator: yields owned T by consuming the vector.
impl<T: RSValueTrait> IntoIterator for RSSortingVector<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        // this does not use a new allocation
        Vec::from(self.values).into_iter()
    }
}

// implementing Index opens the usage of the bracket operators `[]` to access elements in the sorting vector.
impl<I, T: RSValueTrait> Index<I> for RSSortingVector<T>
where
    I: std::slice::SliceIndex<[T]>,
{
    type Output = <[T] as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.values.index(index)
    }
}

// implementing IndexMut opens the usage of the bracket operators `[]` to mutate elements in the sorting vector.
impl<I, T: RSValueTrait> IndexMut<I> for RSSortingVector<T>
where
    I: std::slice::SliceIndex<[T]>,
{
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.values.index_mut(index)
    }
}

/// Walks down the reference chain of a `RSValue` until it reaches a non-reference value.
fn walk_down_rsvalue_ref_chain<T: RSValueTrait>(origin: &T) -> &T {
    let mut loop_var = origin;
    while loop_var.get_ref().is_some() {
        loop_var = loop_var.get_ref().unwrap();
    }
    loop_var
}
