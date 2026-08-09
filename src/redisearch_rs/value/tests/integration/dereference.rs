/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::mem;

use value::{SharedValue, Trio, Value};

// Large enough to overflow the old recursive dereference implementation. The tests call
// `mem::forget` on the root so recursive destruction of the intentionally deep chain does not
// become the limiting factor.
const CHAIN_DEPTH: usize = 100_000;

fn ref_chain(depth: usize, terminal: Value) -> Value {
    (0..depth).fold(terminal, |inner, _| Value::Ref(SharedValue::new(inner)))
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

fn intentional_leak_stress_disabled() -> bool {
    // Sanitizer CI sets `SAN=address`. Do not deliberately leak the 100k-node chains under
    // LeakSanitizer; normal and coverage runs still execute the stress path.
    std::env::var("SAN").as_deref() == Ok("address")
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_follows_nested_refs() {
    if intentional_leak_stress_disabled() {
        return;
    }

    let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

    let dereferenced = matches!(value.fully_dereferenced_ref(), Value::Number(42.0));
    mem::forget(value);

    assert!(dereferenced);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_refs() {
    if intentional_leak_stress_disabled() {
        return;
    }

    let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

    let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
    mem::forget(value);

    assert!(dereferenced);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_trio_left_values() {
    if intentional_leak_stress_disabled() {
        return;
    }

    let value = trio_left_chain(CHAIN_DEPTH, Value::Number(42.0));

    let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
    mem::forget(value);

    assert!(dereferenced);
}
