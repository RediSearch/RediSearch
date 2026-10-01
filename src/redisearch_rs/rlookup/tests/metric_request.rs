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

use std::ptr;

use rlookup::{MetricKeyError, MetricRequests, RLookup, RLookupKey, RLookupKeyFlag};

/// Registration gives every metric a key, but writes it only into the slots of
/// iterators that are still alive: one that was freed has cleared its handle
/// through its own copy of the pointer, and one that was never built has no
/// handle at all.
#[test]
fn register_keys_resolves_only_live_slots() {
    // Stand in for the key slots inside two iterators, which outlive the list
    // as iterators do in production.
    let mut live_slot: *mut RLookupKey<'_> = ptr::null_mut();
    let mut freed_slot: *mut RLookupKey<'_> = ptr::null_mut();

    let mut lookup = RLookup::new();
    let mut requests = MetricRequests::default();
    let live = requests.push(c"__live", false);
    let freed = requests.push(c"__freed", true);
    requests.push(c"__unbound", false);

    // SAFETY: the slot outlives the list.
    unsafe { requests.bind(live, &mut live_slot) };
    // SAFETY: as above, and the slot stops being written through once the flag
    // is cleared below.
    let freed_handle = unsafe { requests.bind(freed, &mut freed_slot) };
    // The iterator owning `freed_slot` is freed, which clears the flag through
    // its own copy of the handle.
    // SAFETY: the handle is live, as the list owning it is.
    unsafe { (*freed_handle.as_ptr()).is_valid = false };

    assert_eq!(requests.register_keys(&mut lookup, |_| false), Ok(()));

    let key = |name| {
        lookup
            .find_key_by_name(name)
            .and_then(|cursor| cursor.into_current())
            .unwrap_or_else(|| panic!("{name:?} must be registered"))
    };
    assert!(ptr::eq(live_slot, key(c"__live")));
    assert!(
        freed_slot.is_null(),
        "a freed iterator's slot is left alone"
    );
    assert!(key(c"__freed").flags.contains(RLookupKeyFlag::Hidden));
    assert!(!key(c"__unbound").flags.contains(RLookupKeyFlag::Hidden));
}

/// Rebinding a request would free the handle an iterator may still hold, so it
/// is refused instead.
#[test]
#[should_panic(expected = "a metric request is bound at most once")]
fn binding_a_request_twice_panics() {
    // Declared before the list, so that it outlives the handle.
    let mut slot: *mut RLookupKey<'_> = ptr::null_mut();
    let mut requests = MetricRequests::default();
    let index = requests.push(c"__v_score", false);

    // SAFETY: the slot outlives the list.
    unsafe { requests.bind(index, &mut slot) };
    // SAFETY: as above.
    unsafe { requests.bind(index, &mut slot) };
}

/// The first refused metric stops registration, after the keys before it and
/// before the ones after it.
#[test]
fn register_keys_stops_at_the_first_refused_metric() {
    let mut lookup = RLookup::new();
    let mut requests = MetricRequests::default();
    for name in [c"__a", c"__b", c"__c"] {
        requests.push(name, false);
    }

    assert_eq!(
        requests.register_keys(&mut lookup, |name| name == c"__b"),
        Err(MetricKeyError::InSchema(c"__b"))
    );
    assert!(lookup.find_key_by_name(c"__a").is_some());
    assert!(lookup.find_key_by_name(c"__c").is_none());

    assert_eq!(
        requests.register_keys(&mut lookup, |_| false),
        Err(MetricKeyError::Duplicate(c"__a"))
    );
}
