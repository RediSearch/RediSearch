/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{SharedValue, Trio, Value};

// Deep enough to catch shallow dereference regressions, but kept moderate because
// dropping the nested SharedValue/Trio structure still walks the chain recursively.
const CHAIN_DEPTH: usize = 256;

fn ref_chain(depth: usize, terminal: Value) -> Value {
    (0..depth).fold(terminal, |inner, _| {
        Value::Ref(SharedValue::new(inner))
    })
}

fn trio_left_chain(depth: usize, terminal: Value) -> Value {
    (0..depth).fold(terminal, |inner, _| {
        Value::Trio(Trio::new(
            SharedValue::new(inner),
            SharedValue::new(Value::Null),
            SharedValue::new(Value::Null),
        ))
    })
}

#[test]
fn fully_dereferenced_ref_follows_nested_refs() {
    let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

    assert!(matches!(
        value.fully_dereferenced_ref(),
        Value::Number(42.0)
    ));
}

#[test]
fn fully_dereferenced_ref_and_trio_follows_nested_refs() {
    let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

    assert!(matches!(
        value.fully_dereferenced_ref_and_trio(),
        Value::Number(42.0)
    ));
}

#[test]
fn fully_dereferenced_ref_and_trio_follows_nested_trio_left_values() {
    let value = trio_left_chain(CHAIN_DEPTH, Value::Number(42.0));

    assert!(matches!(
        value.fully_dereferenced_ref_and_trio(),
        Value::Number(42.0)
    ));
}
