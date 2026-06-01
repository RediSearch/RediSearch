/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the `ttl_table_ffi` C-callable surface. Exercise
//! every exported symbol from the Rust side using raw pointers, mirroring
//! how the C code will eventually call the FFI.

#![expect(
    clippy::undocumented_unsafe_blocks,
    reason = "raw-pointer FFI integration tests intentionally elide per-call SAFETY comments"
)]

use std::ptr;

use field::FieldExpirationPredicate;
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use ttl_table::test_utils::{FUTURE, NOW, PAST, fe};
use ttl_table::{FieldExpiration, FieldExpirations};
use ttl_table_ffi::*;

mock_or_stub_missing_redis_c_symbols!();

const MAX_SIZE: usize = 1024;

/// Run `f` against a freshly initialized, automatically-destroyed table.
fn with_table<F>(f: F)
where
    F: FnOnce(*mut TimeToLiveTable),
{
    let mut t: *mut TimeToLiveTable = ptr::null_mut();
    unsafe { TimeToLiveTable_VerifyInit(&mut t, MAX_SIZE) };
    assert!(!t.is_null());
    f(t);
    unsafe { TimeToLiveTable_Destroy(&mut t) };
    assert!(t.is_null());
}

/// Build a [`FieldExpirations`] from a `[FieldExpiration; N]` literal whose
/// entries are sorted ascending by `index`. Mirrors what the C side produces
/// by calling `FieldExpirations_Empty` / `_WithCapacity` followed by repeated
/// `_Push`.
fn into_ffi<const N: usize>(entries: [FieldExpiration; N]) -> FieldExpirations {
    let mut v = FieldExpirations_WithCapacity(N);
    for fe in entries {
        // SAFETY: callers in this test file always pass entries in strictly
        // ascending `index` order.
        unsafe { FieldExpirations_Push(&mut v, fe) };
    }
    v
}

#[test]
fn verify_init_is_idempotent_when_slot_already_set() {
    let mut t: *mut TimeToLiveTable = ptr::null_mut();
    unsafe { TimeToLiveTable_VerifyInit(&mut t, MAX_SIZE) };
    let first = t;
    unsafe { TimeToLiveTable_VerifyInit(&mut t, MAX_SIZE) };
    assert_eq!(t, first, "second VerifyInit should be a no-op");
    unsafe { TimeToLiveTable_Destroy(&mut t) };
}

#[test]
fn destroy_of_null_slot_is_a_noop() {
    let mut t: *mut TimeToLiveTable = ptr::null_mut();
    unsafe { TimeToLiveTable_Destroy(&mut t) };
    assert!(t.is_null());
}

#[test]
fn empty_after_init_then_not_after_add() {
    with_table(|t| {
        assert!(unsafe { TimeToLiveTable_IsEmpty(t) });
        let v = into_ffi([fe(3, FUTURE)]);
        unsafe { TimeToLiveTable_Add(t, 1, v) };
        assert!(!unsafe { TimeToLiveTable_IsEmpty(t) });
        unsafe { TimeToLiveTable_Remove(t, 1) };
        assert!(unsafe { TimeToLiveTable_IsEmpty(t) });
    });
}

#[test]
fn get_field_expirations_round_trip() {
    with_table(|t| {
        let v = into_ffi([fe(2, FUTURE), fe(5, PAST)]);
        unsafe { TimeToLiveTable_Add(t, 7, v) };

        let hit = unsafe { TimeToLiveTable_GetFieldExpirations(t, 7) };
        assert!(!hit.ptr.is_null());
        assert_eq!(hit.len, 2);
        let slice = unsafe { std::slice::from_raw_parts(hit.ptr, hit.len) };
        assert_eq!(slice[0].index, 2);
        assert_eq!(slice[1].index, 5);

        // Miss
        let miss = unsafe { TimeToLiveTable_GetFieldExpirations(t, 999) };
        assert!(miss.ptr.is_null());
        assert_eq!(miss.len, 0);
    });
}

#[test]
fn remove_absent_doc_is_a_noop() {
    with_table(|t| {
        unsafe { TimeToLiveTable_Remove(t, 42) };
        assert!(unsafe { TimeToLiveTable_IsEmpty(t) });
    });
}

#[test]
fn verify_doc_and_field_present_expired_and_future() {
    with_table(|t| {
        let v = into_ffi([fe(1, PAST), fe(2, FUTURE)]);
        unsafe { TimeToLiveTable_Add(t, 1, v) };

        // Field 1 is in the past at NOW → Default is false, Missing is true.
        assert!(!unsafe {
            TimeToLiveTable_VerifyDocAndField(t, 1, 1, FieldExpirationPredicate::Default, &NOW)
        });
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndField(t, 1, 1, FieldExpirationPredicate::Missing, &NOW)
        });

        // Field 2 is in the future at NOW → Default is true.
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndField(t, 1, 2, FieldExpirationPredicate::Default, &NOW)
        });

        // Unknown doc id → trivially true under both predicates.
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndField(t, 999, 1, FieldExpirationPredicate::Default, &NOW)
        });
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndField(t, 999, 1, FieldExpirationPredicate::Missing, &NOW)
        });
    });
}

#[test]
fn verify_doc_and_field_mask_u32_routes_through_translation_table() {
    // Mask bit 3 ↔ field index 1 (expired). Default → false, Missing → true.
    let map: Vec<u16> = (0u16..32).collect();
    let _ = &map[3]; // index 3 ↔ field 3 in identity map; pick a different layout below.
    // Make the translation interesting: bit 3 maps to field 1.
    let mut map: Vec<u16> = vec![0; 32];
    map[3] = 1;
    with_table(|t| {
        let v = into_ffi([fe(1, PAST)]);
        unsafe { TimeToLiveTable_Add(t, 1, v) };

        let mask: u32 = 1 << 3;
        assert!(!unsafe {
            TimeToLiveTable_VerifyDocAndFieldMask(
                t,
                1,
                mask,
                FieldExpirationPredicate::Default,
                &NOW,
                map.as_ptr(),
            )
        });
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndFieldMask(
                t,
                1,
                mask,
                FieldExpirationPredicate::Missing,
                &NOW,
                map.as_ptr(),
            )
        });
    });
}

#[test]
fn verify_doc_and_wide_field_mask_uses_active_width() {
    // Pick a bit that fits in both u64 and u128 so the test is width-agnostic.
    const BIT: usize = 5;
    let mut map: Vec<u16> = vec![0; 128];
    map[BIT] = 1;

    with_table(|t| {
        let v = into_ffi([fe(1, PAST)]);
        unsafe { TimeToLiveTable_Add(t, 1, v) };

        // Construct the mask via a u64 value the FFI accepts under either
        // typedef width (u64 or u128 — both can hold `1 << 5` losslessly).
        // `t_fieldMask` is `ffi::FieldMask`; build it from a u64 literal.
        let mask: ffi::FieldMask = (1u64 << BIT) as ffi::FieldMask;

        assert!(!unsafe {
            TimeToLiveTable_VerifyDocAndWideFieldMask(
                t,
                1,
                mask,
                FieldExpirationPredicate::Default,
                &NOW,
                map.as_ptr(),
            )
        });
        assert!(unsafe {
            TimeToLiveTable_VerifyDocAndWideFieldMask(
                t,
                1,
                mask,
                FieldExpirationPredicate::Missing,
                &NOW,
                map.as_ptr(),
            )
        });
    });
}

#[test]
fn field_expirations_helpers_handoff_to_add_after_push() {
    with_table(|t| {
        let mut v = FieldExpirations_WithCapacity(2);
        unsafe { FieldExpirations_Push(&mut v, fe(1, FUTURE)) };
        unsafe { FieldExpirations_Push(&mut v, fe(3, PAST)) };
        // Transfer ownership to the table; v must not be touched after this.
        unsafe { TimeToLiveTable_Add(t, 42, v) };

        let read = unsafe { TimeToLiveTable_GetFieldExpirations(t, 42) };
        assert!(!read.ptr.is_null());
        assert_eq!(read.len, 2);
        let slice = unsafe { std::slice::from_raw_parts(read.ptr, read.len) };
        assert_eq!(slice[0].index, 1);
        assert_eq!(slice[1].index, 3);
    });
}
