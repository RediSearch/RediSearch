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

// Moderate stress depth: with the deliberately small test stack below it is enough to overflow
// recursive dereferencing in non-optimized test profiles, without allocating a 100k-node chain.
// The tests call `mem::forget` on the root so recursive destruction of the intentionally deep
// chain does not become the limiting factor.
const CHAIN_DEPTH: usize = 8 * 1024;

// Stack bytes for the stress thread. Keep this deliberately small so `CHAIN_DEPTH` measures
// whether dereferencing grows the call stack rather than relying on the default test stack size.
const TEST_STACK_SIZE: usize = 16 * 1024;

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
    // Sanitizer CI sets `SAN=address`. Do not deliberately leak the stress-test chains under
    // LeakSanitizer; normal and coverage runs still execute the stress path.
    std::env::var("SAN").as_deref() == Ok("address")
}

fn run_with_small_stack(test: impl FnOnce() + Send + 'static) {
    if intentional_leak_stress_disabled() {
        return;
    }

    std::thread::Builder::new()
        .stack_size(TEST_STACK_SIZE)
        .spawn(test)
        .expect("failed to spawn small-stack dereference test")
        .join()
        .expect("small-stack dereference test panicked");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_follows_nested_refs() {
    run_with_small_stack(|| {
        let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref(), Value::Number(42.0));
        mem::forget(value);

        assert!(dereferenced);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_refs() {
    run_with_small_stack(|| {
        let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
        mem::forget(value);

        assert!(dereferenced);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Intentionally leaks a deep chain and is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_trio_left_values() {
    run_with_small_stack(|| {
        let value = trio_left_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
        mem::forget(value);

        assert!(dereferenced);
    });
}
