/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZeroUsize;

use ttl_table::{test_utils::*, *};

#[test]
fn new_table_doesnt_allocate() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.is_empty());
    assert_eq!(t.n_allocated_buckets(), 0);
}

#[test]
fn add_then_remove_leaves_table_empty() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([FieldExpiration {
            index: FIELD_INDEX_1,
            point: FUTURE,
        }]),
    );
    assert!(!t.is_empty());
    t.remove(DOC_ID_1);
    assert!(t.is_empty());
}

#[test]
fn remove_unknown_doc_is_a_noop() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.remove(DOC_ID_1);
    assert!(t.is_empty());
}

#[test]
fn field_expirations_on_empty_table_is_none() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.field_expirations(DOC_ID_1).is_none());
    // A docId beyond max_size also resolves to None without panic.
    assert!(t.field_expirations(u64::MAX).is_none());
}

#[test]
fn field_expirations_returns_inserted_slice() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    let inserted = fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, PAST)]);
    t.add(DOC_ID_1, inserted.clone());

    let got = t.field_expirations(DOC_ID_1).expect("entry must exist");
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].index, FIELD_INDEX_1);
    assert_eq!(got[0].point.tv_sec, FUTURE.tv_sec);
    assert_eq!(got[0].point.tv_nsec, FUTURE.tv_nsec);
    assert_eq!(got[1].index, FIELD_INDEX_2);
    assert_eq!(got[1].point.tv_sec, PAST.tv_sec);
    assert_eq!(got[1].point.tv_nsec, PAST.tv_nsec);

    // A docId that was never added returns None even with other entries present.
    assert!(t.field_expirations(DOC_ID_2).is_none());
}

#[test]
fn field_expirations_after_remove_is_none() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert!(t.field_expirations(DOC_ID_1).is_some());
    t.remove(DOC_ID_1);
    assert!(t.field_expirations(DOC_ID_1).is_none());
}

#[test]
#[should_panic(expected = "at least one field expiration")]
fn add_with_empty_fields_panics() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, empty_fields());
}

#[test]
fn field_with_zero_expiration_never_expires() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    // Field never expires
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, NEVER)]));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn field_with_past_expiration_has_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    // Expired field
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn field_with_equal_expiration_has_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, NOW)]));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn nanoseconds_break_seconds_tie() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn field_with_future_expiration_has_not_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FAR_IN_THE_FUTURE)]));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn verify_field_returns_true_for_unknown_doc() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn verify_field_absent_default_returns_true() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_2,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_2,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn verify_mask_returns_true_for_unknown_doc() {
    let t = TimeToLiveTable::new(TEST_MAX_SIZE);
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[0, 1, 2, 3]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[0, 1, 2, 3]),
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_short_circuits_when_more_bits_than_field_expirations() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        // The bitmask is longer than the entry, so at least one is not expired (or don't have expiration date)
        mask_bit(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_returns_false_when_all_matched_fields_expired_and_no_extras() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    // Mask covers exactly the two expired fields ⇒ no field is valid.
    let map = identity_ft_id();
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_INDEX_1, FIELD_INDEX_2]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_missing_returns_true_when_any_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, PAST)]),
    );
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_INDEX_1, FIELD_INDEX_2]),
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_missing_returns_false_when_no_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, FUTURE)]),
    );
    let map = identity_ft_id();
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_INDEX_1, FIELD_INDEX_2]),
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_returns_true_when_at_least_one_matched_field_valid() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, FUTURE)]),
    );
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_INDEX_1, FIELD_INDEX_2]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_skips_bits_whose_field_index_is_not_tracked() {
    const FIELD_ID: usize = 1;

    let mut map: Vec<u16> = (0u16..32).collect();
    map[FIELD_ID] = FIELD_INDEX_3;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    // The doc only tracks FIELD_INDEX_1 and FIELD_INDEX_2; bit
    // UNTRACKED_FIELD_ID translates to FIELD_INDEX_3, which the entry
    // does not record — so the scan should treat it as "field absent".
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_ID as u16]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[FIELD_ID as u16]),
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_with_sparse_monotonic_translation_table() {
    // Bits 0,1,2 translate to fields 1,3,5; field 3 is expired while 1 and
    // 5 are valid.
    let map: Vec<u16> = vec![FIELD_INDEX_1, FIELD_INDEX_2, FIELD_INDEX_3, 0, 0];
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([
            fe(FIELD_INDEX_1, FUTURE),
            fe(FIELD_INDEX_2, PAST),
            fe(FIELD_INDEX_3, FUTURE),
        ]),
    );
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[0, 1, 2]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[0, 1, 2]),
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
    const MAPPING_BIT: usize = 100;

    let mut map: Vec<u16> = vec![0; 128];
    map[MAPPING_BIT] = FIELD_INDEX_1;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let mask: u128 = 1u128 << MAPPING_BIT;
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
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

#[test]
fn bucket_array_grows_lazily_from_zero() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    assert_eq!(t.n_allocated_buckets(), 0);
    t.add(0, fes([fe(FIELD_INDEX_1, FUTURE)]));
    // First grow seeds at TTL_BUCKET_INITIAL_CAP = 64.
    assert_eq!(t.n_allocated_buckets(), 64);
}

#[test]
fn bucket_array_grows_to_cover_requested_slot() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    // doc_id 200 is past the initial-cap of 64, so growth must round up to
    // at least 201. Geometric step from 0 → 64 → 64+1+32 = 97; still not
    // enough for slot 200, so newcap is bumped to slot+1 = 201.
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert!(t.n_allocated_buckets() >= (DOC_ID_1 as usize + 1));
}

#[test]
fn bucket_array_never_exceeds_max_size() {
    const MAX: usize = 16;
    let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
    t.add(0, fes([fe(0, FUTURE)]));
    // Initial cap of 64 gets clamped down to MAX.
    assert_eq!(t.n_allocated_buckets(), MAX);
}

#[test]
fn slot_collisions_are_handled_via_bucket_chains() {
    const MAX: usize = 8;
    let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
    // doc_id 1 and doc_id 9 both land in slot 1.
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    t.add(DOC_ID_2, fes([fe(FIELD_INDEX_1, PAST)]));
    assert!(!t.is_empty());
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_2,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    // Removing one collision-mate doesn't disturb the other.
    t.remove(DOC_ID_1);
    assert!(!t.field_satisfies_predicate(
        DOC_ID_2,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
}

#[test]
fn no_shrink_on_delete() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(0, FUTURE)]));
    let cap_after_add = t.n_allocated_buckets();
    t.remove(DOC_ID_1);
    assert_eq!(t.n_allocated_buckets(), cap_after_add);
}

#[test]
fn verify_wide_mask_with_bits_in_both_halves() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([
            fe(FIELD_INDEX_1, PAST),
            fe(FIELD_INDEX_2, PAST),
            fe(FIELD_INDEX_3, FUTURE),
            fe(FIELD_INDEX_4, PAST),
        ]),
    );
    // NB: 64 and 65 fall in the second half
    let mut map = vec![0u16; 128];
    map[0] = FIELD_INDEX_1;
    map[1] = FIELD_INDEX_2;
    map[64] = FIELD_INDEX_3;
    map[65] = FIELD_INDEX_4;
    let mask = mask_bit_u128(&[0, 1, 64, 65]);
    // Default: field FIELD_INDEX_3 is FUTURE (valid) ⇒ "one of the fields valid" ⇒ true.
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    // Missing: fields FIELD_INDEX_1, FIELD_INDEX_2, FIELD_INDEX_3 are PAST ⇒ "one of the fields expired" ⇒ true.
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_with_empty_mask_returns_false_for_both_predicates() {
    // No fields are queried. Per the predicate definitions
    // ("one of the fields need to be valid"/"expired"), an empty query
    // cannot satisfy either ⇒ both predicates return false.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let map = identity_ft_id();
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        0u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        0u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_with_empty_mask_returns_false_for_both_predicates() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let map = identity_ft_id();
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        0u128,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        0u128,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
#[should_panic(expected = "ft_id_to_field_index must cover the highest set bit of the mask")]
fn verify_mask_panics_when_translation_table_is_too_short() {
    let count = 5;

    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    let map: Vec<u16> = vec![0u16; count];
    let _ = t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_bit(&[count as u16]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    );
}

#[test]
#[should_panic(expected = "ft_id_to_field_index must cover the highest set bit of the mask")]
fn verify_wide_mask_panics_when_translation_table_is_too_short() {
    let count = 70;

    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    let map: Vec<u16> = vec![0u16; count];
    let _ = t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask_bit_u128(&[count as u16]),
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    );
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "duplicate docId in TTL table")]
fn add_duplicate_doc_id_panics_in_debug() {
    // Per docs: in debug builds, `add` panics if `doc_id` is already
    // present in the table.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_2, FUTURE)]));
}

#[test]
fn add_then_remove_then_add_keeps_count_consistent() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    t.remove(DOC_ID_1);
    assert!(t.is_empty());
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_2, FUTURE)]));
    assert!(!t.is_empty());

    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_2,
        FieldExpirationPredicate::Default,
        &NOW,
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_2,
        FieldExpirationPredicate::Missing,
        &NOW,
    ));

    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW,
    ));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW,
    ));

    t.remove(DOC_ID_1);
    assert!(t.is_empty());
}

#[test]
fn remove_same_doc_twice_is_idempotent() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    t.remove(DOC_ID_1);
    t.remove(DOC_ID_1);
    assert!(t.is_empty());
}

#[test]
fn verify_field_walks_multi_field_entry() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(1, FUTURE), fe(3, PAST), fe(5, FUTURE)]));
    // Field 1: FUTURE.
    assert!(t.field_satisfies_predicate(DOC_ID_1, 1, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 1, FieldExpirationPredicate::Missing, &NOW));
    // Field 3: PAST.
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 3, FieldExpirationPredicate::Default, &NOW));
    assert!(t.field_satisfies_predicate(DOC_ID_1, 3, FieldExpirationPredicate::Missing, &NOW));
    // Field 5: FUTURE.
    assert!(t.field_satisfies_predicate(DOC_ID_1, 5, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 5, FieldExpirationPredicate::Missing, &NOW));
    // Field 2 (gap, untracked).
    assert!(t.field_satisfies_predicate(DOC_ID_1, 2, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 2, FieldExpirationPredicate::Missing, &NOW));
    // Field 4 (gap, untracked).
    assert!(t.field_satisfies_predicate(DOC_ID_1, 4, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 4, FieldExpirationPredicate::Missing, &NOW));
    // Field 6 (past last entry, untracked).
    assert!(t.field_satisfies_predicate(DOC_ID_1, 6, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(DOC_ID_1, 6, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_mask_interleaved_skip_and_match_pattern() {
    // Entry: 5 fields at indices 1, 3, 5, 7, 9.
    // Identity translation lets us mix tracked and untracked bits.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([
            fe(1, PAST),
            fe(3, FUTURE),
            fe(5, PAST),
            fe(7, FUTURE),
            fe(9, PAST),
        ]),
    );
    let map = identity_ft_id();

    // Mask {1, 2, 3, 5, 7}: 5 bits, entry has 5 fields ⇒ no Default
    // short-circuit. Tracked: 1 PAST, 3 FUTURE, 5 PAST, 7 FUTURE.
    // Untracked: 2 (between entries 1 and 3) — counts as valid.
    let mask_mixed = mask_bit(&[1, 2, 3, 5, 7]);
    // Default: a valid field exists (2 untracked, 3 FUTURE, 7 FUTURE) ⇒ true.
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_mixed,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    // Missing: 1 and 5 are PAST ⇒ true.
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_mixed,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));

    // Mask {1, 5, 9}: every queried field is tracked AND expired,
    // and no extras ⇒ no field is valid.
    let mask_all_expired_tracked = mask_bit(&[1, 5, 9]);
    // Default: no valid field ⇒ false.
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_all_expired_tracked,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    // Missing: every queried field is expired ⇒ true.
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask_all_expired_tracked,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn field_with_zero_seconds_but_nonzero_nanos_is_not_the_never_sentinel() {
    // Per docs: NEVER is `(tv_sec, tv_nsec) == (0, 0)`. A point with
    // tv_sec == 0 but tv_nsec > 0 is a legitimate time (1ns past
    // epoch), which is in the past relative to NOW ⇒ expired.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, ts(0, 1))]));
    assert!(!t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW,
    ));
    assert!(t.field_satisfies_predicate(
        DOC_ID_1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW,
    ));
}

#[test]
fn verify_wide_mask_at_bit_64_uses_correct_field_index() {
    // Bit 64 sits at the boundary between the two `u64` halves of the
    // wide mask. Per docs, it must translate via `ft_id_to_field_index`
    // exactly like any other bit.
    let mut map = vec![0u16; 128];
    map[64] = FIELD_INDEX_1;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let mask = mask_bit_u128(&[64]);
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_at_bit_127_uses_correct_field_index() {
    let mut map = vec![0u16; 128];
    map[127] = FIELD_INDEX_1;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    let mask = mask_bit_u128(&[127]);
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_default_short_circuits_when_more_bits_than_field_expirations() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    let map = identity_ft_id();
    let mask = mask_bit_u128(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_default_returns_false_when_all_matched_fields_expired_and_no_extras() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    let map = identity_ft_id();
    let mask = mask_bit_u128(&[FIELD_INDEX_1, FIELD_INDEX_2]);
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_missing_returns_true_when_any_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, PAST)]),
    );
    let map = identity_ft_id();
    let mask = mask_bit_u128(&[FIELD_INDEX_1, FIELD_INDEX_2]);
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_missing_returns_false_when_no_matched_field_expired() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, FUTURE)]),
    );
    let map = identity_ft_id();
    let mask = mask_bit_u128(&[FIELD_INDEX_1, FIELD_INDEX_2]);
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_skips_bits_whose_field_index_is_not_tracked() {
    const FIELD_ID: usize = 1;
    let mut map: Vec<u16> = (0u16..128).collect();
    map[FIELD_ID] = FIELD_INDEX_3;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, PAST), fe(FIELD_INDEX_2, PAST)]),
    );
    let mask = mask_bit_u128(&[FIELD_ID as u16]);
    // Untracked field ⇒ Default true, Missing false.
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    assert!(!t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn bucket_array_grows_geometrically_across_multiple_steps() {
    // Steps:
    //   step 1: 0  → 64           (initial cap)
    //   step 2: 64 → 64+1+32 = 97 (slot 64 forces a grow)
    //   step 3: 97 → 97+1+48 = 146 (slot 97 forces a grow)
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(0, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert_eq!(t.n_allocated_buckets(), 64);
    t.add(64, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert_eq!(t.n_allocated_buckets(), 97);
    t.add(97, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert_eq!(t.n_allocated_buckets(), 146);
}

#[test]
fn bucket_array_rounds_up_to_slot_plus_one_when_geometric_step_too_small() {
    // Per docs: newcap is rounded up to cover the requested slot.
    // First add into slot 500: initial cap of 64 is too small, so the
    // final cap must be `slot + 1 = 501`.
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(500, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert_eq!(t.n_allocated_buckets(), 501);
}

#[test]
fn add_at_max_size_minus_one_works() {
    const MAX: usize = 16;
    let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
    t.add((MAX - 1) as u64, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert!(!t.is_empty());
    assert!(t.field_satisfies_predicate(
        (MAX - 1) as u64,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW,
    ));
}

#[test]
fn multiple_docs_on_distinct_slots_are_independently_retrievable() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(1, fes([fe(FIELD_INDEX_1, FUTURE)]));
    t.add(2, fes([fe(FIELD_INDEX_1, PAST)]));
    t.add(3, fes([fe(FIELD_INDEX_1, FUTURE)]));
    t.add(4, fes([fe(FIELD_INDEX_1, PAST)]));
    t.add(5, fes([fe(FIELD_INDEX_1, FUTURE)]));
    // FUTURE ⇒ Default true; PAST ⇒ Default false.
    assert!(t.field_satisfies_predicate(1, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(
        2,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(3, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW));
    assert!(!t.field_satisfies_predicate(
        4,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Default,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(5, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW));
    // Mirror for Missing.
    assert!(!t.field_satisfies_predicate(
        1,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(2, FIELD_INDEX_1, FieldExpirationPredicate::Missing, &NOW));
    assert!(!t.field_satisfies_predicate(
        3,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
    assert!(t.field_satisfies_predicate(4, FIELD_INDEX_1, FieldExpirationPredicate::Missing, &NOW));
    assert!(!t.field_satisfies_predicate(
        5,
        FIELD_INDEX_1,
        FieldExpirationPredicate::Missing,
        &NOW
    ));
}

#[test]
fn verify_mask_with_two_bits_translating_to_same_field_index() {
    let mut map = vec![0u16; 32];
    map[0] = FIELD_INDEX_1;
    map[1] = FIELD_INDEX_1;
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(
        DOC_ID_1,
        fes([fe(FIELD_INDEX_1, FUTURE), fe(FIELD_INDEX_2, FUTURE)]),
    );
    let mask = mask_bit(&[0, 1]);

    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    // No expired field ⇒ Missing false.
    assert!(!t.verify_doc_and_field_mask(
        DOC_ID_1,
        mask,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_with_all_bits_set_default_short_circuits_to_true() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        DOC_ID_1,
        u32::MAX,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_wide_mask_with_all_bits_set_default_short_circuits_to_true() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(DOC_ID_1, fes([fe(FIELD_INDEX_1, PAST)]));
    let map = identity_ft_id();
    assert!(t.verify_doc_and_wide_field_mask(
        DOC_ID_1,
        u128::MAX,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn doc_id_zero_is_a_valid_doc_id() {
    let mut t = TimeToLiveTable::new(TEST_MAX_SIZE);
    t.add(0, fes([fe(FIELD_INDEX_1, FUTURE)]));
    assert!(!t.is_empty());
    assert!(
        t.field_satisfies_predicate(0, FIELD_INDEX_1, FieldExpirationPredicate::Default, &NOW,)
    );
    t.remove(0);
    assert!(t.is_empty());
}

#[test]
fn high_density_chain_alternating_states_with_swap_last_removes() {
    // Load factor ≈ 4 forces multiple entries per slot. Alternating PAST /
    // FUTURE proves the chain walk returns the right entry, and removing
    // every third docId exercises swap-last from arbitrary chain positions
    // repeatedly — a regression catcher for swap-last bugs that single-3
    // collider tests can't reach.
    const MAX: usize = 32;
    const N: u64 = (MAX as u64) * 4;
    let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());

    for d in 1..=N {
        let point = if d & 1 == 1 { PAST } else { FUTURE };
        t.add(d, fes([fe(0, point)]));
    }
    for d in 1..=N {
        let valid = t.field_satisfies_predicate(d, 0, FieldExpirationPredicate::Default, &NOW);
        assert_eq!(valid, d & 1 == 0, "docId={d}");
    }

    for d in (3..=N).step_by(3) {
        t.remove(d);
    }
    for d in 1..=N {
        let valid = t.field_satisfies_predicate(d, 0, FieldExpirationPredicate::Default, &NOW);
        if d % 3 == 0 {
            // Removed entries are absent ⇒ Default returns true.
            assert!(valid, "docId={d}");
        } else {
            assert_eq!(valid, d & 1 == 0, "docId={d}");
        }
    }

    for d in 1..=N {
        if d % 3 != 0 {
            t.remove(d);
        }
    }
    assert!(t.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Too slow to run under miri due to the huge memory allocation"
)]
fn production_scale_max_size_keeps_cap_proportional_to_use() {
    // With a million-bucket modulus, a sparse workload must not balloon
    // the bucket allocation, and a wrap-around docId must reuse an
    // already-allocated slot without further growth.
    const MAX: usize = 1_000_000;
    let mut t = TimeToLiveTable::new(NonZeroUsize::new(MAX).unwrap());
    assert_eq!(t.n_allocated_buckets(), 0);

    let small_ids: [u64; 4] = [1, 5, 42, 100];
    for d in small_ids {
        t.add(d, fes([fe(0, PAST)]));
    }
    let cap_after_small = t.n_allocated_buckets();
    assert!(
        cap_after_small >= 101,
        "cap must cover slot 100, got {cap_after_small}"
    );
    assert!(
        cap_after_small < MAX / 10,
        "cap must stay far below max_size, got {cap_after_small}"
    );

    // Reads for docIds whose slot is still unallocated report "no TTL".
    assert!(t.field_satisfies_predicate(999_999, 0, FieldExpirationPredicate::Default, &NOW));
    t.add(999_999, fes([fe(0, PAST)]));
    assert!(t.n_allocated_buckets() >= 1_000_000);
    assert!(!t.field_satisfies_predicate(999_999, 0, FieldExpirationPredicate::Default, &NOW));

    for d in small_ids {
        assert!(
            !t.field_satisfies_predicate(d, 0, FieldExpirationPredicate::Default, &NOW),
            "docId={d}"
        );
    }

    // Wrap-around docId routes via modulo into an already-allocated slot,
    // so cap must NOT change.
    let cap_before_wrap = t.n_allocated_buckets();
    t.add(MAX as u64 + 5, fes([fe(0, PAST)]));
    assert_eq!(t.n_allocated_buckets(), cap_before_wrap);
    assert!(!t.field_satisfies_predicate(
        MAX as u64 + 5,
        0,
        FieldExpirationPredicate::Default,
        &NOW,
    ));
    // The original docId=5 entry must remain distinct from the wrap.
    assert!(!t.field_satisfies_predicate(5, 0, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn bitmask_iter_empty_yields_nothing() {
    assert_eq!(BitU64Iter::new(0u64).next(), None);
}

#[test]
fn bitmask_iter_single_bit() {
    assert_eq!(BitU64Iter::new(1u64 << 0).collect::<Vec<_>>(), vec![0]);
    assert_eq!(BitU64Iter::new(1u64 << 17).collect::<Vec<_>>(), vec![17]);
    assert_eq!(BitU64Iter::new(1u64 << 63).collect::<Vec<_>>(), vec![63]);
}

#[test]
fn bitmask_iter_multiple_bits_low_to_high() {
    let mask: u64 = (1 << 0) | (1 << 5) | (1 << 63);
    assert_eq!(BitU64Iter::new(mask).collect::<Vec<_>>(), vec![0, 5, 63]);
}

#[test]
fn bitmask_iter_max_yields_all_indices() {
    assert_eq!(
        BitU64Iter::new(u64::MAX).collect::<Vec<_>>(),
        (0u32..64).collect::<Vec<_>>()
    );
}
