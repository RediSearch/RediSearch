/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CString, c_char},
    ops::{Index, IndexMut},
    slice::{Iter, IterMut},
};

/// A trait that defines the behavior of a RediSearch RSValue.
///
/// This trait is used to create, manipulate, and free RSValue instances. It is
/// implemented by a mock type for testing purposes, and by a new-type in the ffi layer to
/// interact with the C API.
pub trait RSValueTrait
where
    Self: Sized,
{
    /// Creates a new RSValue instance which is null
    fn create_null() -> Self;

    /// Creates a new RSValue instance with a string value.
    fn create_string(s: &str) -> Self;

    /// Creates a new RSValue instance with a number (double) value.
    fn create_num(num: f64) -> Self;

    /// Creates a new RSValue instance that is a reference to the given value.
    fn create_ref(value: Self) -> Self;

    /// Checks if the RSValue is null
    fn is_null(&self) -> bool;

    /// gets a reference to the RSValue instance, if it is a reference type or None.
    fn get_ref(&self) -> Option<&Self>;

    /// gets a mutable reference to the RSValue instance, if it is a reference type or None.
    fn get_ref_mut(&mut self) -> Option<&mut Self>;

    /// gets the string slice of the RSValue instance, if it is a string type or None otherwise.
    fn as_str(&self) -> Option<&str>;

    /// gets the number (double) value of the RSValue instance, if it is a number type or None otherwise.
    fn as_num(&self) -> Option<f64>;

    /// gets the type of the RSValue instance, it either null, string, number, or reference.
    fn get_type(&self) -> ffi::RSValueType;

    /// returns true if the RSValue is stored as a pointer on the heap (the C implementation)
    fn is_ptr_type() -> bool;

    /// returns the approximate memory size of the RSValue instance.
    fn mem_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// A [`RSSortingVector`] is a vector of a type T implementing [`RSValueTrait`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RSSortingVector<T: RSValueTrait + Clone> {
    values: Box<[T]>,
}

// Consuming iterator: yields owned T by consuming the vector.
impl<T: RSValueTrait + Clone> IntoIterator for RSSortingVector<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        // this does not use a new allocation
        Vec::from(self.values).into_iter()
    }
}

// Immutable borrowing iterator: yields &T without consuming the vector.
impl<'a, T: RSValueTrait + Clone> IntoIterator for &'a RSSortingVector<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

// Mutable borrowing iterator: yields &mut T for element modification.
impl<'a, T: RSValueTrait + Clone> IntoIterator for &'a mut RSSortingVector<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter_mut()
    }
}

impl<T: RSValueTrait + Clone> RSSortingVector<T> {
    /// Creates a new [`RSSortingVector`] with the given length.
    pub fn new(len: usize) -> Self {
        Self {
            values: vec![T::create_null(); len].into_boxed_slice(),
        }
    }

    /// Checks if the index is valid and decrements the reference count of previous RSValue instances.
    /// Returns `true` if the index is valid , `false` otherwise.
    fn sanity_check(&self, idx: usize) -> bool {
        idx < self.values.len()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.values.iter()
    }

    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        self.values.iter_mut()
    }

    /// Set a number (double) at the given index
    pub fn put_num(&mut self, idx: usize, num: f64) {
        if !self.sanity_check(idx) {
            return;
        }

        // Safety: We are creating a new RSValue with a number, we need to ensure that
        // We ensure the previous value is decremented in `check_and_cleanup`.
        self.values[idx] = T::create_num(num);
    }

    /// Set a string at the given index
    pub fn put_string(&mut self, idx: usize, str: &str) {
        if !self.sanity_check(idx) {
            return;
        }

        self.values[idx] = T::create_string(str);
    }

    /// Set a string at the given index, normalizing it to lower case and to be sortable ("Straße" -> "Strasse").
    pub fn put_string_and_normalize(&mut self, idx: usize, str: &str) {
        if !self.sanity_check(idx) {
            return;
        }

        // The following is a workaround which calls a C method to normalize the string.
        // It based on nulib and shall be replaced by ICU in the future.
        // To understand why ICU is not working, see the test case `test_case_folding_aka_normlization_rust_impl`.
        let str = CString::new(str).expect("Failed to create CString");

        let cptr: *mut c_char = str.as_c_str().as_ptr().cast_mut();

        // Safety: The given cptr was just generated from a CString, so it is valid.
        let cstr: *mut c_char = unsafe { ffi::normalizeStr(cptr) };

        // Safety: We assume that the C function `normalizeStr` returns a valid CString pointer.
        let normalized_str = unsafe { CString::from_raw(cstr) };

        self.values[idx] = T::create_string(&normalized_str.to_string_lossy());

        // -----

        // the following is not working without Miri errors, see test case 'test_case_folding_aka_normlization_rust_impl'
        /*
        let casemapper = CaseMapper::new();
        let normalized = casemapper.fold_string(str);
        self.values[idx] = T::create_string(&normalized);
        */
    }

    /// Set a value at the given index
    pub fn put_val(&mut self, idx: usize, value: T) {
        if !self.sanity_check(idx) {
            return;
        }

        self.values[idx] = value;
    }

    /// Set a reference to the value at the given index
    pub fn put_val_as_ref(&mut self, idx: usize, value: T) {
        if !self.sanity_check(idx) {
            return;
        }

        self.values[idx] = T::create_ref(value);
    }

    /// Set a null value at the given index
    pub fn put_null(&mut self, idx: usize) {
        if !self.sanity_check(idx) {
            return;
        }

        self.values[idx] = T::create_null();
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
    /// The implementation by-passes references in the middle of the chain, so it only counts the size of the final value.
    /// TODO: Is that C behavior correct? Should we count the size of the entire chain?
    pub fn get_memory_size(&self) -> usize {
        // Each RSValue is a pointer, so we multiply by the size of a pointer
        let mut sz = if T::is_ptr_type() {
            self.values.len() * std::mem::size_of::<*const T>()
        } else {
            self.values.len() * T::mem_size()
        };

        for idx in 0..self.values.len() {
            if self.values[idx].is_null() {
                continue;
            }
            if T::is_ptr_type() {
                // count the size of the struct if not null, like in C
                sz += T::mem_size();
            }

            // the original behavior would by-pass references in the middle of the chain
            let value = walk_down_rsvalue_ref_chain(&self.values[idx]);

            if value.get_type() == ffi::RSValueType_RSValue_String {
                // todo string ptr len
                sz += value.as_str().unwrap().len();
            }
        }
        sz
    }
}

impl<I, T: RSValueTrait + Clone> Index<I> for RSSortingVector<T>
where
    I: std::slice::SliceIndex<[T]>,
{
    type Output = <[T] as std::ops::Index<I>>::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.values.index(index)
    }
}

impl<I, T: RSValueTrait + Clone> IndexMut<I> for RSSortingVector<T>
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
