/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{ElementNumbers, SharedValue, Value};

const TEXT: &[u8] = b"the JSON text";

/// What a plain multi-value sortable field stores for `values`.
fn plain(values: &[SharedValue]) -> SharedValue {
    match values {
        [single] => single.clone(),
        _ => SharedValue::new_trio(
            values[0].clone(),
            SharedValue::new_string(TEXT.to_vec()),
            SharedValue::new_array(values.to_vec()),
        ),
    }
}

/// The value stored for an array of `num_elements` elements in which `elements` hold
/// `numbers`, and the other elements hold no value.
fn stored(elements: &[u32], numbers: &[f64], num_elements: u32) -> SharedValue {
    let values: Vec<_> = numbers.iter().copied().map(SharedValue::new_num).collect();
    ElementNumbers::store(elements, numbers, num_elements, plain(&values))
}

/// The parts of a stored [`Value::Trio`]: what it shows expanded, and the text and the
/// entries by element of its carrier.
fn parts(value: &SharedValue) -> (&SharedValue, &SharedValue, &SharedValue) {
    let Value::Trio(stored) = &**value else {
        panic!("expected a trio, got {value:?}")
    };
    let Value::Trio(carrier) = &**stored.middle() else {
        panic!("expected a carrier, got {:?}", stored.middle())
    };
    (stored.right(), carrier.middle(), carrier.right())
}

#[test]
fn stored_numbers_are_read_back_by_element() {
    let value = stored(&[0, 2], &[90.0, 150.0], 4);
    let numbers = ElementNumbers::stored_in(&value).expect("a stored value");

    assert_eq!(numbers.num_elements(), 4);
    assert_eq!(numbers.get(0), Some(90.0));
    assert_eq!(numbers.get(1), None, "an element without a number");
    assert_eq!(numbers.get(2), Some(150.0));
    assert_eq!(numbers.get(3), None, "a trailing element without a number");
    assert_eq!(numbers.get(4), None, "past the last element");
}

#[test]
fn array_of_one_element_is_stored_as_its_number() {
    let value = stored(&[0], &[200.0], 1);

    assert!(matches!(&*value, Value::Number(number) if *number == 200.0));
    let numbers = ElementNumbers::stored_in(&value).expect("a stored value");
    assert_eq!(numbers.num_elements(), 1);
    assert_eq!(numbers.get(0), Some(200.0));
    assert_eq!(numbers.get(1), None);
}

#[test]
fn stored_value_compares_by_its_first_number() {
    // Element 0 has no number: the 700 of element 1 is the one to compare by.
    let value = stored(&[1, 2], &[700.0, 300.0], 3);

    assert_eq!(value.to_number(), Some(700.0));
}

#[test]
fn value_that_was_not_stored_holds_no_numbers() {
    for value in [
        SharedValue::null_static(),
        SharedValue::new_string(b"150".to_vec()),
    ] {
        assert!(ElementNumbers::stored_in(&value).is_none());
    }
}

#[test]
fn elements_without_a_value_are_not_shown() {
    let value = stored(&[1, 3], &[150.0, 90.0], 4);

    let (expanded, text, _) = parts(&value);
    let Value::Array(shown) = &**expanded else {
        panic!("expected an array, got {expanded:?}")
    };
    assert_eq!(shown.len_u32(), 2);
    assert_eq!(
        shown.first().and_then(|number| number.as_num()),
        Some(150.0)
    );
    assert_eq!(text.as_str_bytes(), Some(TEXT));
}

#[test]
fn single_value_of_several_elements_is_shown_as_itself() {
    let value = stored(&[1], &[150.0], 2);

    let (expanded, text, _) = parts(&value);
    assert_eq!(expanded.as_num(), Some(150.0));
    assert_eq!(text.as_num(), Some(150.0));
    assert_eq!(value.to_number(), Some(150.0));
    let numbers = ElementNumbers::stored_in(&value).expect("a stored value");
    assert_eq!(numbers.num_elements(), 2);
    assert_eq!(numbers.get(0), None);
    assert_eq!(numbers.get(1), Some(150.0));
}

#[test]
fn null_value_is_shown_and_is_not_a_number() {
    let values = [SharedValue::null_static(), SharedValue::new_num(700.0)];
    let value = ElementNumbers::store(&[1], &[700.0], 2, plain(&values));

    let (expanded, _, by_element) = parts(&value);
    assert!(
        SharedValue::ptr_eq(expanded, by_element),
        "every element has a value, so what is shown is the entries by element"
    );
    let numbers = ElementNumbers::stored_in(&value).expect("a stored value");
    assert_eq!(numbers.get(0), None);
    assert_eq!(numbers.get(1), Some(700.0));
    assert_eq!(value.to_number(), Some(700.0));
}
