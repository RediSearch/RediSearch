/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value::{SharedValue, Trio, Value};

// Moderate stress depth: with the deliberately small test stack below it is enough to overflow
// recursive dereferencing in non-optimized test profiles, without allocating a 100k-node chain.
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

/// The next link of a chain built by [`ref_chain`] or [`trio_left_chain`].
fn chain_link(value: &Value) -> Option<SharedValue> {
    match value {
        Value::Ref(next) => Some(next.clone()),
        Value::Trio(trio) => Some(trio.left().clone()),
        _ => None,
    }
}

/// Drops a chain built by [`ref_chain`] or [`trio_left_chain`] one link at a time.
///
/// Dropping the root directly would recurse once per link and overflow the small test stack.
/// Holding a clone of the next link while dropping the current one keeps that link alive, so
/// freeing each link only decrements the next link's refcount instead of descending into it.
fn drop_chain(value: Value) {
    let mut next = chain_link(&value);
    drop(value);
    while let Some(link) = next {
        // A link with another owner outlives this drop, and whichever owner releases it last
        // tears the rest of the chain down recursively.
        assert_eq!(SharedValue::refcount(&link), 1, "chain link is shared");
        next = chain_link(&link);
    }
}

fn run_with_small_stack(test: impl FnOnce() + Send + 'static) {
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
    ignore = "Building and dropping a deep chain is too slow under Miri"
)]
fn fully_dereferenced_ref_follows_nested_refs() {
    run_with_small_stack(|| {
        let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref(), Value::Number(42.0));
        drop_chain(value);

        assert!(dereferenced);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Building and dropping a deep chain is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_refs() {
    run_with_small_stack(|| {
        let value = ref_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
        drop_chain(value);

        assert!(dereferenced);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Building and dropping a deep chain is too slow under Miri"
)]
fn fully_dereferenced_ref_and_trio_follows_nested_trio_left_values() {
    run_with_small_stack(|| {
        let value = trio_left_chain(CHAIN_DEPTH, Value::Number(42.0));

        let dereferenced = matches!(value.fully_dereferenced_ref_and_trio(), Value::Number(42.0));
        drop_chain(value);

        assert!(dereferenced);
    });
}
