/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Behavioural tests for the faithful Rust port of `src/ttl_table/`. Each
//! test mirrors a branch of the C `TimeToLiveTable_*` functions in
//! `src/ttl_table/ttl_table.c` so the two implementations can be compared
//! on identical inputs.

use libc::timespec;
use thin_vec::{ThinVec, thin_vec};
use ttl_table_2::{FieldExpiration, FieldExpirationPredicate, TimeToLiveTable};

const NEVER: timespec = timespec {
    tv_sec: 0,
    tv_nsec: 0,
};

const fn ts(sec: i64, nsec: i64) -> timespec {
    timespec {
        tv_sec: sec as libc::time_t,
        tv_nsec: nsec as libc::c_long,
    }
}

const NOW: timespec = ts(1000, 0);

/// Default modulus for tests where the value doesn't matter for the
/// behaviour under test. Large enough to keep `doc_id` and `slot` aligned
/// in trivial cases.
const TEST_MAX_SIZE: usize = 1024;

const fn empty_fields() -> ThinVec<FieldExpiration> {
    ThinVec::new()
}

// ---------------------------------------------------------------------------
// Construction & basics
// ---------------------------------------------------------------------------

#[test]
fn new_table_is_empty() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.is_empty());
    assert_eq!(t.debug_allocated_buckets(), 0);
}

#[test]
#[should_panic(expected = "max_size must be >= 1")]
fn new_with_zero_max_size_panics() {
    let _ = TimeToLiveTable::new(0);
}

#[test]
fn add_then_remove_leaves_table_empty() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        7,
        thin_vec![FieldExpiration {
            index: 0,
            point: ts(2000, 0),
        }],
    );
    assert!(!t.is_empty());
    t.remove(7);
    assert!(t.is_empty());
}

#[test]
fn remove_unknown_doc_is_a_noop() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.remove(123);
    assert!(t.is_empty());
}

#[test]
#[should_panic(expected = "at least one field expiration")]
fn add_with_empty_fields_panics() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, empty_fields());
}

// ---------------------------------------------------------------------------
// did_expire — exposed indirectly through verify_doc_and_field
// ---------------------------------------------------------------------------

const fn fe(index: u16, point: timespec) -> FieldExpiration {
    FieldExpiration { index, point }
}

#[test]
fn field_with_zero_expiration_never_expires() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, NEVER)]);
    // Default: not expired ⇒ true; Missing: not expired ⇒ false.
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn field_with_past_expiration_has_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(999, 0))]);
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn field_with_equal_expiration_has_expired() {
    // The C `DidExpire` treats an exact-match timestamp as expired.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(1000, 0))]);
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn field_with_future_expiration_has_not_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(1001, 0))]);
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn nanoseconds_break_seconds_tie() {
    // tv_sec equal, field nsec greater than now ⇒ not expired.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(1000, 1))]);
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

// ---------------------------------------------------------------------------
// verify_doc_and_field — single-field path
// ---------------------------------------------------------------------------

#[test]
fn verify_field_returns_true_for_unknown_doc_default() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.verify_doc_and_field(1, 0, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_returns_true_for_unknown_doc_missing() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.verify_doc_and_field(1, 0, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_present_and_expired_default_returns_false() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(999, 0))]);
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_present_and_expired_missing_returns_true() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(999, 0))]);
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_present_and_valid_default_returns_true() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(2000, 0))]);
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_present_and_valid_missing_returns_false() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(2000, 0))]);
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_absent_default_returns_true() {
    // Field 5 is not tracked; for Default this means "trivially valid".
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(999, 0))]);
    assert!(t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_absent_missing_returns_false() {
    // Field 5 is not tracked; for Missing this means "not actually missing".
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(3, ts(999, 0))]);
    assert!(!t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Missing, &NOW));
}

// ---------------------------------------------------------------------------
// verify_doc_and_field_mask (32-bit)
// ---------------------------------------------------------------------------

/// Identity mapping from bit position → field index (bit `i` ↔ field `i`).
fn identity_ft_id() -> Vec<u16> {
    (0u16..128).collect()
}

#[test]
fn verify_mask_returns_true_for_unknown_doc() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        1,
        0b1111u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_field_mask(
        1,
        0b1111u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_short_circuits_when_more_bits_than_field_expirations() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(999, 0)), fe(1, ts(999, 0))]);
    let map = identity_ft_id();
    // 4 bits set, only 2 expirations recorded — at least 2 mask bits cannot
    // be expired, so Default is trivially true.
    assert!(t.verify_doc_and_field_mask(
        1,
        0b1111u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_returns_false_when_all_matched_fields_expired_and_no_extras() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(999, 0)), fe(1, ts(999, 0))]);
    let map = identity_ft_id();
    // Mask covers exactly the two expired fields ⇒ no field is valid.
    assert!(!t.verify_doc_and_field_mask(
        1,
        0b11u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_missing_returns_true_when_any_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(2000, 0)), fe(1, ts(999, 0))]);
    let map = identity_ft_id();
    assert!(
        t.verify_doc_and_field_mask(1, 0b11u32, FieldExpirationPredicate::Missing, &NOW, &map,)
    );
}

#[test]
fn verify_mask_missing_returns_false_when_no_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(2000, 0)), fe(1, ts(2000, 0))]);
    let map = identity_ft_id();
    assert!(!t.verify_doc_and_field_mask(
        1,
        0b11u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_returns_true_when_at_least_one_matched_field_valid() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(999, 0)), fe(1, ts(2000, 0))]);
    let map = identity_ft_id();
    assert!(
        t.verify_doc_and_field_mask(1, 0b11u32, FieldExpirationPredicate::Default, &NOW, &map,)
    );
}

#[test]
fn verify_mask_skips_bits_whose_field_index_is_not_tracked() {
    // The mask asks about bit 2, which translates to field index 5, but the
    // doc only tracks field indices 0 and 1. Behaves as "field absent": for
    // Default ⇒ true, for Missing ⇒ false.
    let mut map: Vec<u16> = (0u16..32).collect();
    map[2] = 5;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(0, ts(999, 0)), fe(1, ts(999, 0))]);
    assert!(t.verify_doc_and_field_mask(
        1,
        0b100u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_field_mask(
        1,
        0b100u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_with_sparse_monotonic_translation_table() {
    // Bits 0,1,2 translate to fields 1,3,5; field 3 is expired while 1 and
    // 5 are valid. Mirrors the equivalent test in the existing ttl_table
    // crate for cross-implementation comparability.
    let map: Vec<u16> = vec![1, 3, 5, 0, 0];
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        1,
        thin_vec![fe(1, ts(2000, 0)), fe(3, ts(999, 0)), fe(5, ts(2000, 0))],
    );
    assert!(t.verify_doc_and_field_mask(
        1,
        0b111u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_field_mask(
        1,
        0b111u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

// ---------------------------------------------------------------------------
// verify_doc_and_wide_field_mask (128-bit)
// ---------------------------------------------------------------------------

#[test]
fn verify_wide_mask_high_bits_use_correct_field_index() {
    // Single bit at position 100 in a u128 mask. The translation table maps
    // bit 100 to field index 7. Field 7 is expired ⇒ Default returns false,
    // Missing returns true.
    let mut map: Vec<u16> = vec![0; 128];
    map[100] = 7;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, thin_vec![fe(7, ts(999, 0))]);
    let mask: u128 = 1u128 << 100;
    assert!(!t.verify_doc_and_wide_field_mask(
        1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_wide_field_mask(
        1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_returns_true_for_unknown_doc() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    let map = identity_ft_id();
    assert!(t.verify_doc_and_wide_field_mask(
        1,
        0b1111u128,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

// ---------------------------------------------------------------------------
// Faithfulness-specific tests: bucket growth, slot collisions
// ---------------------------------------------------------------------------

#[test]
fn bucket_array_grows_lazily_from_zero() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert_eq!(t.debug_allocated_buckets(), 0);
    t.add(0, thin_vec![fe(0, ts(2000, 0))]);
    // First grow seeds at TTL_BUCKET_INITIAL_CAP = 64.
    assert_eq!(t.debug_allocated_buckets(), 64);
}

#[test]
fn bucket_array_grows_to_cover_requested_slot() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    // doc_id 200 is past the initial-cap of 64, so growth must round up to
    // at least 201. Geometric step from 0 → 64 → 64+1+32 = 97; still not
    // enough for slot 200, so newcap is bumped to slot+1 = 201.
    t.add(200, thin_vec![fe(0, ts(2000, 0))]);
    assert!(t.debug_allocated_buckets() >= 201);
}

#[test]
fn bucket_array_never_exceeds_max_size() {
    const MAX: usize = 16;
    let mut t = TimeToLiveTable::new(MAX);
    t.add(0, thin_vec![fe(0, ts(2000, 0))]);
    // Initial cap of 64 gets clamped down to MAX.
    assert_eq!(t.debug_allocated_buckets(), MAX);
}

#[test]
fn slot_collisions_are_handled_via_bucket_chains() {
    const MAX: usize = 8;
    let mut t = TimeToLiveTable::new(MAX);
    // doc_id 1 and doc_id 9 both land in slot 1.
    t.add(1, thin_vec![fe(0, ts(2000, 0))]);
    t.add(9, thin_vec![fe(0, ts(999, 0))]);
    assert!(!t.is_empty());
    assert!(t.verify_doc_and_field(1, 0, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.verify_doc_and_field(9, 0, FieldExpirationPredicate::Default, &NOW));
    // Removing one collision-mate doesn't disturb the other.
    t.remove(1);
    assert!(!t.verify_doc_and_field(9, 0, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn no_shrink_on_delete() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(0, thin_vec![fe(0, ts(2000, 0))]);
    let cap_after_add = t.debug_allocated_buckets();
    t.remove(0);
    assert_eq!(t.debug_allocated_buckets(), cap_after_add);
}
