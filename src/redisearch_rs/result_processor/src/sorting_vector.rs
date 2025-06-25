/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ops::{Index, IndexMut},
    slice::{Iter, IterMut},
};

use icu::casemap::CaseMapper;

/// A trait that defines the behavior of a RediSearch RSValue.
///
/// This trait is used to create, manipulate, and free RSValue instances. It is
/// implemented by a mock type for testing purposes, and by a new-type in the ffi layer to
/// interact with the C API.
pub trait RSValueTrait {
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
}

/// A [`RSSortingVector`] is a vector of a type T implementing [`RSValueTrait`].
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

        // TOOD: Move elsewhere
        //let (ptr, len) = alloc_c_string(str);

        self.values[idx] = T::create_string(str);
    }

    /// Set a string at the given index, normalizing it to lower case and to be sortable ("Straße" -> "Strasse").
    pub fn put_string_and_normalize(&mut self, idx: usize, str: &str) {
        if !self.sanity_check(idx) {
            return;
        }

        // todo: find a rust implementation of `normalizeStr` that is equivalent to the C implementation.
        // todo move elsewhere
        //let normalized = str.to_lowercase();
        //let (ptr, len) = alloc_c_string(&normalized);

        let casemapper = CaseMapper::new();
        let normalized = casemapper.fold_string(str);
        self.values[idx] = T::create_string(&normalized);
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
        let mut sz = self.values.len() * std::mem::size_of::<*mut ffi::RSValue>();

        for idx in 0..self.values.len() {
            if self.values[idx].is_null() {
                continue;
            }

            // Each RSValue has a refcount and a value, so we add the size of those
            sz += std::mem::size_of::<T>();

            // the original behavior would by-pass references in the middle of the chain
            let value = walk_down_rsvalue_ref_chain(&self.values[idx]);

            if value.get_type() == ffi::RSValueType_RSValue_String {
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

#[cfg(test)]
mod tests {

    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    enum RSValueMock {
        Null,
        Number(f64),
        String(String),
        Reference(Box<RSValueMock>),
    }

    impl RSValueTrait for RSValueMock {
        fn create_null() -> Self {
            RSValueMock::Null
        }

        fn create_string(s: &str) -> Self {
            RSValueMock::String(s.to_string())
        }

        fn create_num(num: f64) -> Self {
            RSValueMock::Number(num)
        }

        fn create_ref(value: Self) -> Self {
            RSValueMock::Reference(Box::new(value))
        }

        fn is_null(&self) -> bool {
            matches!(self, RSValueMock::Null)
        }

        fn get_ref(&self) -> Option<&Self> {
            if let RSValueMock::Reference(boxed) = self {
                Some(boxed)
            } else {
                None
            }
        }

        fn get_ref_mut(&mut self) -> Option<&mut Self> {
            if let RSValueMock::Reference(boxed) = self {
                Some(boxed.as_mut())
            } else {
                None
            }
        }

        fn as_str(&self) -> Option<&str> {
            if let RSValueMock::String(s) = self {
                Some(s)
            } else {
                None
            }
        }

        fn as_num(&self) -> Option<f64> {
            if let RSValueMock::Number(num) = self {
                Some(*num)
            } else {
                None
            }
        }

        fn get_type(&self) -> ffi::RSValueType {
            // Mock implementation, return a dummy type
            match &self {
                RSValueMock::Null => ffi::RSValueType_RSValue_Null,
                RSValueMock::Number(_) => ffi::RSValueType_RSValue_Number,
                RSValueMock::String(_) => ffi::RSValueType_RSValue_String,
                RSValueMock::Reference(reference) => reference.get_type(),
            }
        }
    }

    #[test]
    fn test_rssortingvector_creation() {
        let vector: RSSortingVector<RSValueMock> = RSSortingVector::new(10);
        assert_eq!(vector.len(), 10);

        for value in vector {
            assert!(value.is_null());
        }
    }

    fn build_vector() -> RSSortingVector<RSValueMock> {
        let mut vector = RSSortingVector::new(5);
        vector.put_num(0, 42.0);
        vector.put_string(1, "Hello");
        vector.put_val_as_ref(2, RSValueMock::create_num(3.));
        vector.put_val(3, RSValueMock::create_string("World"));
        vector.put_null(4);
        vector
    }

    #[test]
    fn test_rssortingvector_put() {
        let vector: &mut RSSortingVector<RSValueMock> = &mut build_vector();

        assert_eq!(vector[0].as_num(), Some(42.0));
        assert_eq!(vector[1].as_str(), Some("Hello"));
        assert_eq!(vector[2].get_ref().unwrap().as_num(), Some(3.0));
        assert_eq!(vector[3].as_str(), Some("World"));
        assert!(vector[4].is_null());
    }

    #[test]
    fn test_rssortingvector_override() {
        let src = build_vector();
        let mut dst: RSSortingVector<RSValueMock> = RSSortingVector::new(1);
        assert_eq!(dst[0], RSValueMock::create_null());

        for (idx, val) in src.iter().enumerate() {
            dst.put_val(0, val.clone());
            assert_eq!(dst[0], src[idx]);
        }

        assert_eq!(dst[0], RSValueMock::create_null());
    }

    #[test]
    fn test_normlize_in_c_equals_rust_impl() {
        let str = "Straße";
        let mut vec: RSSortingVector<RSValueMock> = RSSortingVector::new(1);
        vec.put_string_and_normalize(0, str);
        assert_eq!(vec[0].as_str(), Some("strasse"));
    }
}
