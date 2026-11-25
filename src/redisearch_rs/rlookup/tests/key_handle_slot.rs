/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ptr::NonNull;

use rlookup::{KeyHandleSlot, RLookupKeyHandle};

fn handle() -> RLookupKeyHandle<'static> {
    RLookupKeyHandle {
        key_ptr: std::ptr::null_mut(),
        is_valid: true,
    }
}

#[test]
fn dropping_the_slot_invalidates_its_handle() {
    let mut handle = handle();
    let mut slot = KeyHandleSlot::new();

    // SAFETY: `handle` outlives `slot` and is only touched through it until
    // `slot` is dropped.
    unsafe { slot.set(Some(NonNull::from(&mut handle))) };
    drop(slot);

    assert!(!handle.is_valid, "dropping the slot invalidates the handle");
}

#[test]
fn reattaching_the_same_handle_preserves_it() {
    let mut handle = handle();
    let ptr = NonNull::from(&mut handle);
    let mut slot = KeyHandleSlot::new();

    // SAFETY: `handle` outlives `slot` and is only touched through `ptr` until
    // `slot` is dropped.
    unsafe { slot.set(Some(ptr)) };
    // SAFETY: as above.
    unsafe { slot.set(Some(ptr)) };
    // SAFETY: `ptr` is still the attached handle, alive and not borrowed.
    assert!(unsafe { ptr.as_ref() }.is_valid, "reattaching preserves it");

    drop(slot);
    assert!(!handle.is_valid);
}

#[test]
fn replacing_and_detaching_invalidate_the_previous_handle() {
    let mut first = handle();
    let mut second = handle();
    let first_ptr = NonNull::from(&mut first);
    let second_ptr = NonNull::from(&mut second);
    let mut slot = KeyHandleSlot::new();

    // SAFETY: both handles outlive `slot` and are only touched through their
    // pointers until they are detached.
    unsafe { slot.set(Some(first_ptr)) };
    // SAFETY: as above.
    unsafe { slot.set(Some(second_ptr)) };
    // SAFETY: both pointers are alive and not borrowed.
    assert!(
        !unsafe { first_ptr.as_ref() }.is_valid,
        "replacement invalidates the old handle"
    );
    // SAFETY: as above.
    assert!(
        unsafe { second_ptr.as_ref() }.is_valid,
        "replacement preserves the new one"
    );

    // SAFETY: as above.
    unsafe { slot.set(None) };
    assert!(!second.is_valid, "detachment invalidates the handle");
    assert!(!first.is_valid);
    drop(slot);
}
