/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use sorting_vector::RSSortingVector;
use value::RSValueTrait;

/// Row data for a lookup key. This abstracts the question of if the data comes from a SortingVector
/// or from a dynamic value.
///
/// The type itself exposes the dynamic values as a vector of `Option<T>`, where `T` is the type of the value and
/// implements the index and the index_mut traits to allow for easy access to the values by index. It also provides
/// methods to get the length of the dynamic values array and check if it is empty.
///
/// The type `T` is the type of the value stored in the row, which must implement the [`RSValueTrait`].
#[derive(Debug)]
pub struct RLookupRow<T: RSValueTrait> {
    /// Sorting vector attached to document
    sorting_vector: RSSortingVector<T>,

    /// Dynamic values obtained from prior processing
    values: Vec<Option<T>>,

    /// How many values actually exist in dyn. Note that this is not the length of the array!
    num: u32,
}

impl<T: RSValueTrait> RLookupRow<T> {
    /// Creates a new `RLookupRow` with an empty dynamic values vector and a sorting vector of the given length.
    pub fn new(sorting_vector_len: usize) -> Self {
        Self {
            sorting_vector: RSSortingVector::new(sorting_vector_len),
            values: vec![],
            num: 0,
        }
    }

    /// Returns the length of the dynamic values vector.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the dynamic values vector is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Readonly access to the dynamic values vector
    pub fn values(&self) -> &Vec<Option<T>> {
        &self.values
    }

    /// Readonly access to the sorting vector
    pub fn sorting_vector(&self) -> &RSSortingVector<T> {
        &self.sorting_vector
    }

    /// Mutable access to the sorting vector
    pub fn sorting_vector_mut(&mut self) -> &mut RSSortingVector<T> {
        &mut self.sorting_vector
    }

    /// How many values actually exist in dyn. Note that this is not the length of the array!
    pub fn num(&self) -> u32 {
        self.num
    }
}
