/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRange.

use index_result::RSIndexResult;
use inverted_index::IndexReader as _;
use inverted_index::numeric::PreparedValue;
use numeric_range_tree::{NumericIndex, NumericRange};
use rstest::rstest;

/// Ask an uncompressed encoder how it will represent `value` — the form
/// [`NumericRange::add`] takes.
fn prepared(value: f64) -> PreparedValue {
    NumericIndex::prepare_for(false, value)
}

#[test]
fn test_new_range_bounds() {
    let range = NumericRange::new(false);
    assert_eq!(range.min_val(), f64::INFINITY);
    assert_eq!(range.max_val(), f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_updates_bounds() {
    let mut range = NumericRange::new(false);

    range.add(1, prepared(5.0));
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 5.0);

    range.add(2, prepared(10.0));
    assert_eq!(range.min_val(), 5.0);
    assert_eq!(range.max_val(), 10.0);

    range.add(3, prepared(2.0));
    assert_eq!(range.min_val(), 2.0);
    assert_eq!(range.max_val(), 10.0);
}

#[test]
fn test_add_updates_cardinality() {
    let mut range = NumericRange::new(false);

    range.add(1, prepared(1.0));
    range.add(2, prepared(2.0));
    range.add(3, prepared(3.0));

    // HLL gives approximate count, but for small counts it should be reasonably accurate
    let card = range.cardinality();
    assert!(
        (2..=4).contains(&card),
        "Cardinality {card} not in expected range"
    );
}

#[test]
fn test_contained_in() {
    let mut range = NumericRange::new(false);
    range.add(1, prepared(5.0));
    range.add(2, prepared(10.0));

    // Range [5, 10] is contained in [0, 20]
    assert!(range.contained_in(0.0, 20.0));
    // Range [5, 10] is contained in [5, 10]
    assert!(range.contained_in(5.0, 10.0));
    // Range [5, 10] is NOT contained in [6, 20]
    assert!(!range.contained_in(6.0, 20.0));
    // Range [5, 10] is NOT contained in [0, 9]
    assert!(!range.contained_in(0.0, 9.0));
}

#[test]
fn test_overlaps() {
    let mut range = NumericRange::new(false);
    range.add(1, prepared(5.0));
    range.add(2, prepared(10.0));

    // Overlapping cases
    assert!(range.overlaps(0.0, 6.0)); // left overlap
    assert!(range.overlaps(8.0, 20.0)); // right overlap
    assert!(range.overlaps(6.0, 8.0)); // contained
    assert!(range.overlaps(0.0, 20.0)); // contains

    // Non-overlapping cases
    assert!(!range.overlaps(11.0, 20.0)); // completely right
    assert!(!range.overlaps(0.0, 4.0)); // completely left
}

#[test]
fn test_default_impl() {
    let range: NumericRange = Default::default();
    assert_eq!(range.min_val(), f64::INFINITY);
    assert_eq!(range.max_val(), f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_without_cardinality() {
    let mut range = NumericRange::new(false);

    // Add entries without updating cardinality
    range.add_without_cardinality(1, prepared(5.0));
    range.add_without_cardinality(2, prepared(10.0));
    range.add_without_cardinality(3, prepared(2.0));

    // Bounds should be updated
    assert_eq!(range.min_val(), 2.0);
    assert_eq!(range.max_val(), 10.0);
    assert_eq!(range.num_entries(), 3);

    // Cardinality should be 0 (HLL not updated)
    assert_eq!(range.cardinality(), 0);
}

#[test]
fn test_add_without_cardinality_vs_add() {
    let mut range_with_card = NumericRange::new(false);
    let mut range_without_card = NumericRange::new(false);

    // Add same values to both
    range_with_card.add(1, prepared(5.0));
    range_with_card.add(2, prepared(10.0));

    range_without_card.add_without_cardinality(1, prepared(5.0));
    range_without_card.add_without_cardinality(2, prepared(10.0));

    // Bounds should be the same
    assert_eq!(range_with_card.min_val(), range_without_card.min_val());
    assert_eq!(range_with_card.max_val(), range_without_card.max_val());
    assert_eq!(
        range_with_card.num_entries(),
        range_without_card.num_entries()
    );

    // Cardinality differs
    assert!(range_with_card.cardinality() > 0);
    assert_eq!(range_without_card.cardinality(), 0);
}

#[test]
fn test_num_docs() {
    let mut range = NumericRange::new(false);
    assert_eq!(range.num_docs(), 0);

    range.add(1, prepared(5.0));
    assert_eq!(range.num_docs(), 1);

    range.add(2, prepared(10.0));
    assert_eq!(range.num_docs(), 2);

    range.add(3, prepared(15.0));
    assert_eq!(range.num_docs(), 3);
}

#[test]
fn test_entries_accessor() {
    use numeric_range_tree::NumericIndex;

    let mut range = NumericRange::new(false);
    range.add(1, prepared(5.0));
    range.add(2, prepared(10.0));

    let entries = range.entries();
    match entries {
        NumericIndex::Uncompressed(idx) => {
            assert_eq!(idx.number_of_entries(), 2);
            assert!(idx.memory_usage() > 0);
        }
        NumericIndex::Compressed(idx) => {
            assert_eq!(idx.number_of_entries(), 2);
            assert!(idx.memory_usage() > 0);
        }
    }
}

#[test]
fn test_hll_accessor() {
    let mut range = NumericRange::new(false);
    range.add(1, prepared(5.0));
    range.add(2, prepared(10.0));
    range.add(3, prepared(15.0));

    let hll = range.hll();
    // HLL count should approximate the number of distinct values
    let count = hll.count();
    assert!(
        (2..=4).contains(&count),
        "HLL count {count} not in expected range"
    );
}

#[test]
fn test_inverted_index_size() {
    let mut range = NumericRange::new(false);
    let initial_size = range.memory_usage();

    range.add(1, prepared(5.0));
    let size_after_one = range.memory_usage();
    assert!(size_after_one >= initial_size);

    range.add(2, prepared(10.0));
    let size_after_two = range.memory_usage();
    assert!(size_after_two >= size_after_one);
}

// ============================================================================
// Stored-value quantization
// ============================================================================

#[test]
fn test_compressed_range_treats_collapsing_values_as_one() {
    // Two distinct f64s inside the same f32 bucket. A compressed range stores one
    // value for both, so its statistics must report one value.
    let mut range = NumericRange::new(true);
    let index = NumericIndex::new(true);
    range.add(1, index.prepare(100.5));
    range.add(2, index.prepare(100.500001));

    assert_eq!(range.num_entries(), 2);
    assert_eq!(
        range.cardinality(),
        1,
        "both inputs collapse onto the same stored value"
    );
    assert_eq!(
        (range.min_val(), range.max_val()),
        (100.5, 100.5),
        "bounds must equal the stored value, not the inputs"
    );

    // The same two inputs stay distinct without compression.
    let mut range = NumericRange::new(false);
    let index = NumericIndex::new(false);
    range.add(1, index.prepare(100.5));
    range.add(2, index.prepare(100.500001));
    assert_eq!(range.cardinality(), 2);
    assert_eq!((range.min_val(), range.max_val()), (100.5, 100.500001));
}

#[rstest]
fn test_signed_zeros_are_one_stored_value(#[values(false, true)] compress_floats: bool) {
    // Every numeric encoder writes zero as a tiny integer, dropping the sign, so
    // `-0.0` and `+0.0` are indistinguishable once stored.
    let mut range = NumericRange::new(compress_floats);
    let index = NumericIndex::new(compress_floats);
    range.add(1, index.prepare(-0.0));
    range.add(2, index.prepare(0.0));

    assert_eq!(range.cardinality(), 1);
    assert!(range.min_val().is_sign_positive());
    assert_eq!((range.min_val(), range.max_val()), (0.0, 0.0));
}

#[rstest]
fn test_geohash_values_are_stored_exactly(#[values(false, true)] compress_floats: bool) {
    // GEO fields share this tree: `encodeGeo` yields a 52-bit integral geohash, which
    // takes the encoder's integer path and is never rounded — with or without float
    // compression. Quantization must therefore be the identity here.
    let geohashes = [0.0, 1.0, 3_659_155_824_453_222.0, (1u64 << 52) as f64 - 1.0];

    let index = NumericIndex::new(compress_floats);
    for geohash in geohashes {
        assert_eq!(
            index.prepare(geohash).stored_value().get(),
            geohash,
            "geohash {geohash} must survive preparation unchanged"
        );
    }

    let mut range = NumericRange::new(compress_floats);
    for (i, geohash) in geohashes.iter().enumerate() {
        range.add(i as u64 + 1, index.prepare(*geohash));
    }
    assert_eq!(range.cardinality(), geohashes.len());
    assert_eq!(range.min_val(), geohashes[0]);
    assert_eq!(range.max_val(), geohashes[geohashes.len() - 1]);
}

#[rstest]
fn test_numeric_index_write_paths_agree(#[values(false, true)] compress_floats: bool) {
    // The two write paths must be interchangeable. This also keeps `add_record`
    // exercised from inside this crate: with no in-crate caller it reads as dead code
    // and invites removal, which breaks downstream test suites.
    let values = [0.0, 3.0, -7.0, 1024.0, 0.5, 100.500001, f64::INFINITY];

    let mut from_records = NumericIndex::new(compress_floats);
    let mut from_prepared = NumericIndex::new(compress_floats);

    for (i, value) in values.iter().enumerate() {
        let doc_id = i as u64 + 1;
        let record = RSIndexResult::build_numeric(*value).doc_id(doc_id).build();

        let record_outcome = from_records.add_record(&record);
        let prepared_outcome =
            from_prepared.add_prepared_record(doc_id, from_prepared.prepare(*value));

        assert_eq!(
            record_outcome, prepared_outcome,
            "outcomes diverged while adding {value} for doc {doc_id}"
        );
    }

    assert_eq!(
        from_records.number_of_entries(),
        from_prepared.number_of_entries()
    );
    assert_eq!(from_records.memory_usage(), from_prepared.memory_usage());
    assert_eq!(
        from_records.summary().number_of_blocks,
        from_prepared.summary().number_of_blocks,
        "block layout diverged"
    );

    // And the entries decode identically.
    let decode = |index: &NumericIndex| {
        let mut reader = index.reader();
        let mut result = RSIndexResult::build_numeric(0.0).build();
        let mut decoded = Vec::new();
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: a numeric index only ever holds numeric records
            decoded.push((result.doc_id, unsafe { result.as_numeric_unchecked() }));
        }
        decoded
    };
    assert_eq!(decode(&from_records), decode(&from_prepared));
}

#[test]
fn test_compressed_signed_zero_collapse_counts_once() {
    // Under float compression a magnitude below the smallest f32 subnormal rounds to
    // zero, for either sign. Both must arrive as the same stored value, or the two
    // would compare equal to bounds, routing and filters while a cardinality estimate
    // keyed by bit pattern counted them separately — enough to make a leaf of
    // comparably identical values look splittable.
    let index = NumericIndex::new(true);
    let positive = index.prepare(5e-324).stored_value().get();
    let negative = index.prepare(-5e-324).stored_value().get();
    assert_eq!(positive.to_bits(), negative.to_bits());
    assert_eq!(positive, 0.0);
    assert!(positive.is_sign_positive());

    let mut range = NumericRange::new(true);
    range.add(1, index.prepare(5e-324));
    range.add(2, index.prepare(-5e-324));

    assert_eq!((range.min_val(), range.max_val()), (0.0, 0.0));
    assert_eq!(
        range.cardinality(),
        1,
        "stored values that compare equal must count once"
    );
}
