/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The value that a NUMERIC member of a NESTED group keeps in a document's sorting vector.

use crate::{Array, SharedValue, Value};

/// The numbers that the elements of a JSON array hold for one field.
///
/// The stored [`Value`] sorts and displays as the value that a plain multi-value sortable
/// field stores for the same values:
///
/// - for an array of one element, its number;
/// - otherwise a [`Trio`](crate::Trio) of the number to compare by, a carrier, and what
///   that value shows expanded. The carrier is a [`Trio`](crate::Trio) too, which a reply
///   shows as its middle, the text of that value. Its right is an [`Array`] with one entry
///   per element: the element's number, or null where it has none.
///
/// The number to compare by is the first one, so an element without a number does not hide
/// the ones after it.
#[derive(Debug, Clone, Copy)]
pub enum ElementNumbers<'a> {
    /// The number of the only element.
    One(f64),
    /// The entries by element.
    Many(&'a Array),
}

impl<'a> ElementNumbers<'a> {
    /// Builds the [`Value`] to store for an array of `num_elements` elements, in which
    /// element `elements[i]` holds `numbers[i]`. `shown` is what a plain multi-value
    /// sortable field stores for the values at the same path.
    ///
    /// # Panics
    ///
    /// Panics if no element holds a number, if `elements` and `numbers` differ in length, or
    /// if an element number is not below `num_elements`.
    pub fn store(
        elements: &[u32],
        numbers: &[f64],
        num_elements: u32,
        shown: SharedValue,
    ) -> SharedValue {
        assert_eq!(elements.len(), numbers.len());
        let first = *numbers.first().expect("an element holds a number");
        if num_elements == 1 {
            return SharedValue::new_num(first);
        }
        let (text, expanded) = match &*shown {
            Value::Trio(multi_value) => (multi_value.middle().clone(), multi_value.right().clone()),
            _ => (shown.clone(), shown.clone()),
        };
        let by_element = match &*expanded {
            // Every element has a value, a number or a null.
            Value::Array(values) if values.len_u32() == num_elements => expanded.clone(),
            _ => {
                let mut entries = vec![SharedValue::null_static(); num_elements as usize];
                for (&element, &number) in elements.iter().zip(numbers) {
                    entries[element as usize] = SharedValue::new_num(number);
                }
                SharedValue::new_array(entries)
            }
        };
        SharedValue::new_trio(
            SharedValue::new_num(first),
            SharedValue::new_trio(SharedValue::null_static(), text, by_element),
            expanded,
        )
    }

    /// Reads back what [`Self::store`] built. Returns `None` for any other value, such as
    /// the null of a document in which no element holds a number.
    pub fn stored_in(value: &'a Value) -> Option<Self> {
        match value {
            Value::Number(number) => Some(Self::One(*number)),
            Value::Trio(stored) => match &**stored.middle() {
                Value::Trio(carrier) => match &**carrier.right() {
                    Value::Array(entries) => Some(Self::Many(entries)),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }

    /// The number of `element`, or `None` when it has none.
    pub fn get(&self, element: u32) -> Option<f64> {
        match self {
            Self::One(number) => (element == 0).then_some(*number),
            Self::Many(entries) => entries.get(element as usize)?.as_num(),
        }
    }

    /// How many elements the array has.
    pub fn num_elements(&self) -> u32 {
        match self {
            Self::One(_) => 1,
            Self::Many(entries) => entries.len_u32(),
        }
    }
}
