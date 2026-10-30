/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{SharedValue, Trio, Value};

fn string(s: &str) -> SharedValue {
    SharedValue::new_string(s.as_bytes().to_vec())
}

#[test]
fn numbers_and_numeric_strings_convert() {
    assert_eq!(SharedValue::new_num(-2.5).to_number(), Some(-2.5));
    assert_eq!(string("12.5").to_number(), Some(12.5));
    assert_eq!(string("abc").to_number(), None);
    assert_eq!(string("").to_number(), None);
}

/// References and the left element of trios are followed to the value they hold.
#[test]
fn references_and_trios_are_followed() {
    let trio = SharedValue::new(Value::Trio(Trio::new(
        string("7"),
        SharedValue::new_num(1.0),
        SharedValue::new_num(2.0),
    )));
    let chain = SharedValue::new(Value::Ref(trio));
    assert_eq!(chain.to_number(), Some(7.0));
}

#[test]
fn other_types_have_no_number() {
    for value in [
        SharedValue::new(Value::Undefined),
        SharedValue::new(Value::Null),
        SharedValue::new_array([SharedValue::new_num(1.0)]),
        SharedValue::new_map(Vec::<(SharedValue, SharedValue)>::new()),
    ] {
        assert_eq!(value.to_number(), None, "{value:?}");
    }
}
