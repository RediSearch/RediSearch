/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Behavioural tests for the Rust `ttl_table` port. Each test mirrors a branch
//! of the C `TimeToLiveTable_*` functions in `src/ttl_table/ttl_table.c`.

use libc::timespec;
use ttl_table::{FieldExpiration, FieldExpirationPredicate, TimeToLiveTable};

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

/// Build a TimeToLiveTable populated with one entry. Useful to factor out
/// boilerplate from the verify_* tests.
fn table_with_entry(
    doc_id: u64,
    doc_expiration: timespec,
    fields: Vec<FieldExpiration>,
) -> TimeToLiveTable {
    let mut t = TimeToLiveTable::new();
    t.add(doc_id, doc_expiration, fields);
    t
}

// ---------------------------------------------------------------------------
// did_expire — exposed indirectly through has_doc_expired
// ---------------------------------------------------------------------------

#[test]
fn doc_with_zero_expiration_never_expires() {
    let t = table_with_entry(1, NEVER, Vec::new());
    assert!(!t.has_doc_expired(1, &NOW));
}

#[test]
fn doc_with_past_expiration_has_expired() {
    let t = table_with_entry(1, ts(999, 0), Vec::new());
    assert!(t.has_doc_expired(1, &NOW));
}

#[test]
fn doc_with_equal_expiration_has_expired() {
    // The C predicate considers an exact-match timestamp as expired.
    let t = table_with_entry(1, ts(1000, 0), Vec::new());
    assert!(t.has_doc_expired(1, &NOW));
}

#[test]
fn doc_with_future_expiration_has_not_expired() {
    let t = table_with_entry(1, ts(1001, 0), Vec::new());
    assert!(!t.has_doc_expired(1, &NOW));
}

#[test]
fn nanoseconds_break_seconds_tie() {
    // tv_sec equal, field nsec greater than now ⇒ not expired.
    let t = table_with_entry(1, ts(1000, 1), Vec::new());
    assert!(!t.has_doc_expired(1, &NOW));
}

#[test]
fn has_doc_expired_returns_false_for_unknown_doc() {
    let t = TimeToLiveTable::new();
    assert!(!t.has_doc_expired(42, &NOW));
}

// ---------------------------------------------------------------------------
// Basic table operations
// ---------------------------------------------------------------------------

#[test]
fn new_table_is_empty() {
    let t = TimeToLiveTable::new();
    assert!(t.is_empty());
}

#[test]
fn add_then_remove_leaves_table_empty() {
    let mut t = TimeToLiveTable::new();
    t.add(7, ts(2000, 0), Vec::new());
    assert!(!t.is_empty());
    t.remove(7);
    assert!(t.is_empty());
}

#[test]
fn remove_unknown_doc_is_a_noop() {
    let mut t = TimeToLiveTable::new();
    t.remove(123);
    assert!(t.is_empty());
}

// ---------------------------------------------------------------------------
// verify_doc_and_field — single-field path
// ---------------------------------------------------------------------------

#[test]
fn verify_field_returns_true_for_unknown_doc_default() {
    let t = TimeToLiveTable::new();
    assert!(t.verify_doc_and_field(1, 0, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_returns_true_for_unknown_doc_missing() {
    let t = TimeToLiveTable::new();
    assert!(t.verify_doc_and_field(1, 0, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_returns_true_when_no_field_expirations() {
    let t = table_with_entry(1, NEVER, Vec::new());
    assert!(t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Default, &NOW));
    assert!(t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_present_and_expired_default_returns_false() {
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(999, 0),
        }],
    );
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_present_and_expired_missing_returns_true() {
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(999, 0),
        }],
    );
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_present_and_valid_default_returns_true() {
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(2000, 0),
        }],
    );
    assert!(t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_present_and_valid_missing_returns_false() {
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(2000, 0),
        }],
    );
    assert!(!t.verify_doc_and_field(1, 3, FieldExpirationPredicate::Missing, &NOW));
}

#[test]
fn verify_field_absent_default_returns_true() {
    // Field 5 is not tracked; for Default this means "trivially valid".
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(999, 0),
        }],
    );
    assert!(t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Default, &NOW));
}

#[test]
fn verify_field_absent_missing_returns_false() {
    // Field 5 is not tracked; for Missing this means "not actually missing".
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 3,
            point: ts(999, 0),
        }],
    );
    assert!(!t.verify_doc_and_field(1, 5, FieldExpirationPredicate::Missing, &NOW));
}

// ---------------------------------------------------------------------------
// verify_doc_and_field_mask — covers both the 32-bit and 128-bit C functions
// ---------------------------------------------------------------------------

/// Identity mapping from bit position → field index (so bit `i` ↔ field `i`).
fn identity_ft_id() -> Vec<u16> {
    (0u16..128).collect()
}

#[test]
fn verify_mask_returns_true_for_unknown_doc() {
    let t = TimeToLiveTable::new();
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
fn verify_mask_returns_true_when_no_field_expirations() {
    let t = table_with_entry(1, NEVER, Vec::new());
    let map = identity_ft_id();
    assert!(t.verify_doc_and_field_mask(
        1,
        0b1111u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
}

#[test]
fn verify_mask_default_short_circuits_when_more_bits_than_field_expirations() {
    // 4 bits set in the mask, but the doc only has 2 field expirations.
    // Therefore at least 2 fields in the mask cannot be expired ⇒ true.
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(999, 0), // expired
            },
            FieldExpiration {
                index: 1,
                point: ts(999, 0), // expired
            },
        ],
    );
    let map = identity_ft_id();
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
    // Mask covers exactly the two expired fields: no field is valid ⇒ false.
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(999, 0),
            },
            FieldExpiration {
                index: 1,
                point: ts(999, 0),
            },
        ],
    );
    let map = identity_ft_id();
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
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(2000, 0), // valid
            },
            FieldExpiration {
                index: 1,
                point: ts(999, 0), // expired
            },
        ],
    );
    let map = identity_ft_id();
    assert!(
        t.verify_doc_and_field_mask(1, 0b11u32, FieldExpirationPredicate::Missing, &NOW, &map,)
    );
}

#[test]
fn verify_mask_missing_returns_false_when_no_matched_field_expired() {
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(2000, 0),
            },
            FieldExpiration {
                index: 1,
                point: ts(2000, 0),
            },
        ],
    );
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
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(999, 0), // expired
            },
            FieldExpiration {
                index: 1,
                point: ts(2000, 0), // valid
            },
        ],
    );
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
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 0,
                point: ts(999, 0),
            },
            FieldExpiration {
                index: 1,
                point: ts(999, 0),
            },
        ],
    );
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
fn verify_mask_u128_high_bits_use_correct_field_index() {
    // Set a single bit at position 100 in a u128 mask. The translation table
    // maps bit 100 to field index 7. Field 7 is expired ⇒ Default returns
    // false (no valid field), Missing returns true.
    let mut map: Vec<u16> = vec![0; 128];
    map[100] = 7;
    let t = table_with_entry(
        1,
        NEVER,
        vec![FieldExpiration {
            index: 7,
            point: ts(999, 0),
        }],
    );
    let mask: u128 = 1u128 << 100;
    assert!(!t.verify_doc_and_field_mask(1, mask, FieldExpirationPredicate::Default, &NOW, &map,));
    assert!(t.verify_doc_and_field_mask(1, mask, FieldExpirationPredicate::Missing, &NOW, &map,));
}

#[test]
fn verify_mask_with_sparse_monotonic_translation_table() {
    // The mask is iterated low-to-high; the C algorithm relies on the
    // resulting sequence of field indices to be non-decreasing (so the
    // two-pointer scan over the sorted `fields` only advances forward).
    // Here bits 0,1,2 translate to fields 1,3,5 — monotonic but not the
    // identity — and field 3 is expired while 1 and 5 are valid.
    let map: Vec<u16> = vec![1, 3, 5, /* padding */ 0, 0];
    let t = table_with_entry(
        1,
        NEVER,
        vec![
            FieldExpiration {
                index: 1,
                point: ts(2000, 0), // valid
            },
            FieldExpiration {
                index: 3,
                point: ts(999, 0), // expired
            },
            FieldExpiration {
                index: 5,
                point: ts(2000, 0), // valid
            },
        ],
    );
    // Default: at least one valid match (field 1 or 5) ⇒ true.
    assert!(t.verify_doc_and_field_mask(
        1,
        0b111u32,
        FieldExpirationPredicate::Default,
        &NOW,
        &map,
    ));
    // Missing: field 3 is expired ⇒ true.
    assert!(t.verify_doc_and_field_mask(
        1,
        0b111u32,
        FieldExpirationPredicate::Missing,
        &NOW,
        &map,
    ));
}
