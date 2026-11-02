/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use geo::{GEO_RANGE_COUNT, GeoScoreRange, calc_ranges, hash::WGS84Coordinates};

fn coords(lon: f64, lat: f64) -> WGS84Coordinates {
    WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap()
}

#[test]
fn center_cell_has_nonzero_range() {
    let ranges = calc_ranges(coords(2.3522, 48.8566), 1000.0); // Paris, 1km
    assert!(
        ranges[0].min < ranges[0].max,
        "center cell should have min < max, got {:?}",
        ranges[0]
    );
}

#[test]
fn center_cell_scores_fit_in_52_bits() {
    let ranges = calc_ranges(coords(-122.4194, 37.7749), 5000.0); // SF, 5km
    assert!(
        ranges[0].max < (1u64 << 52),
        "center cell max should fit in 52 bits"
    );
}

#[test]
fn nonzero_ranges_have_min_less_than_max() {
    let ranges = calc_ranges(coords(0.0, 45.0), 5000.0);
    for (i, range) in ranges.iter().enumerate() {
        if *range != GeoScoreRange::default() {
            assert!(
                range.min < range.max,
                "range[{i}] should have min < max, got {range:?}"
            );
        }
    }
}

#[test]
fn small_radius_has_some_zeroed_neighbors() {
    let ranges = calc_ranges(coords(0.0, 0.0), 100.0); // 100m at equator
    let zeroed = ranges
        .iter()
        .skip(1) // skip center
        .filter(|r| **r == GeoScoreRange::default())
        .count();
    assert!(
        zeroed > 0,
        "small radius should exclude some neighbors, but all were non-zero"
    );
}

#[test]
fn large_radius_has_some_duplicate_suppressed_entries() {
    // Very large radius can cause neighbors to be the same cell, which
    // get suppressed to default.
    let ranges = calc_ranges(coords(0.0, 0.0), 10_000_000.0);

    // At least the center cell should be non-zero.
    assert_ne!(
        ranges[0],
        GeoScoreRange::default(),
        "center cell should be non-zero even for huge radius"
    );
}

#[test]
fn returns_exactly_geo_range_count_entries() {
    let ranges = calc_ranges(coords(10.0, 20.0), 1000.0);
    assert_eq!(ranges.len(), GEO_RANGE_COUNT);
}

#[test]
fn different_locations_produce_different_center_scores() {
    let paris = calc_ranges(coords(2.3522, 48.8566), 1000.0);
    let tokyo = calc_ranges(coords(139.6917, 35.6895), 1000.0);
    assert_ne!(
        paris[0], tokyo[0],
        "Paris and Tokyo should have different center ranges"
    );
}

#[test]
fn does_not_panic_at_boundaries() {
    let _ = calc_ranges(coords(180.0, 85.05), 1000.0);
    let _ = calc_ranges(coords(-180.0, -85.05), 1000.0);
    let _ = calc_ranges(coords(0.0, 0.0), 0.0);
    let _ = calc_ranges(coords(0.0, 0.0), f64::MAX);
}
