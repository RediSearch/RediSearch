/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::num::NonZero;

use ffi::timespec;
use rand::{SeedableRng as _, rngs::StdRng};
use rqe_core::DocId;
use ttl_table::test_utils::*;
use ttl_table_bencher::*;

fn assert_timespec_eq(a: timespec, b: timespec) {
    assert_eq!(a.tv_sec, b.tv_sec);
    assert_eq!(a.tv_nsec, b.tv_nsec);
}

#[test]
fn test_create_field_expiration() {
    // Seed a deterministic RNG so every assertion below — in particular the
    // probabilistic "not all are present" case — has a fixed, reproducible
    // outcome rather than a small chance of spurious failure.
    let mut rng = StdRng::seed_from_u64(42);

    // All items in FAR_IN_THE_FUTURE
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 10,
            count_variation: 0,
            fill_probability: 1.,
            expired_probability: 0.,
            far_future_percentage: 1.,
        },
        &mut rng,
    );
    assert_eq!(output.len(), 10);
    for (index, field) in output.iter().enumerate() {
        assert_eq!(index as u16, field.index);
        assert_timespec_eq(field.point, FAR_IN_THE_FUTURE);
    }

    // All items in FUTURE
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 10,
            count_variation: 0,
            fill_probability: 1.,
            expired_probability: 0.,
            far_future_percentage: 0.,
        },
        &mut rng,
    );
    assert_eq!(output.len(), 10);
    for (index, field) in output.iter().enumerate() {
        assert_eq!(index as u16, field.index);
        assert_timespec_eq(field.point, FUTURE);
    }

    // All items in PAST
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 10,
            count_variation: 0,
            fill_probability: 1.,
            expired_probability: 1.,
            far_future_percentage: 0.,
        },
        &mut rng,
    );
    assert_eq!(output.len(), 10);
    for (index, field) in output.iter().enumerate() {
        assert_eq!(index as u16, field.index);
        assert_timespec_eq(field.point, PAST);
    }

    // Not all are present.
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 10,
            count_variation: 0,
            fill_probability: 0.5,
            expired_probability: 1.,
            far_future_percentage: 0.,
        },
        &mut rng,
    );
    assert!(output.len() < 10);

    // Count variation
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 10,
            count_variation: 5,
            fill_probability: 1.,
            expired_probability: 1.,
            far_future_percentage: 0.,
        },
        &mut rng,
    );
    assert!(output.len() <= 15 && output.len() >= 5);

    // 0 is cast to 1
    let output = create_field_expiration(
        0,
        &FieldExpirationInput {
            count_mean: 0,
            count_variation: 0,
            fill_probability: 1.,
            expired_probability: 0.,
            far_future_percentage: 1.,
        },
        &mut rng,
    );
    assert_eq!(output.len(), 1);
}

#[test]
fn test_create_docs() {
    let field_expiration_input = FieldExpirationInput {
        count_mean: 10,
        count_variation: 0,
        fill_probability: 1.,
        expired_probability: 1.,
        far_future_percentage: 0.,
    };

    let output: Vec<_> = create_docs(
        DocsInput {
            count: 10,
            start_doc_id_from: 0,
            field_expiration_input,
            fill_probability: 1.0,
        },
        rand::rng(),
    );
    assert_eq!(output.len(), 10);
    for i in 0..10 {
        assert_eq!(output[i].0, i as DocId);
        assert!(!output[i].1.is_empty());
    }

    let output: Vec<_> = create_docs(
        DocsInput {
            count: 10,
            start_doc_id_from: 5,
            field_expiration_input,
            fill_probability: 1.0,
        },
        rand::rng(),
    );
    assert_eq!(output.len(), 10);
    for (i, (doc_id, fields)) in (5..15).zip(output) {
        assert_eq!(doc_id, i as DocId);
        assert!(!fields.is_empty());
    }
}

#[test]
fn test_create_and_populate() {
    // SAFETY: inputs is empty
    let table = create_and_populate(NonZero::try_from(2).unwrap(), vec![]);

    assert!(table.is_empty());
}
