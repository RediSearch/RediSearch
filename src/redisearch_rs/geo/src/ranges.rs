/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Geohash range calculation for radius queries.
//!
//! Given a center point and radius, computes the sorted-set score ranges
//! for the 9 geohash cells (center + 8 neighbors) that cover the query area.

use crate::hash::{self, GeoHashBits, WGS84Coordinates};

/// The number of geohash ranges: the center cell plus its 8 neighbors.
pub const GEO_RANGE_COUNT: usize = 9;

/// A min/max score range for a geohash cell, suitable for sorted-set queries.
///
/// Scores are 52-bit aligned geohash values in `0..2^52`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeoScoreRange {
    /// Minimum score (inclusive).
    pub min: u64,
    /// Maximum score (exclusive).
    pub max: u64,
}

/// Compute the min (inclusive) and max (exclusive) sorted-set scores for a
/// geohash box.
fn scores_of_geohash_box(hash: GeoHashBits) -> GeoScoreRange {
    let min = hash::align_52bits(hash);
    let max = hash::align_52bits(GeoHashBits {
        bits: hash.bits.wrapping_add(1),
        step: hash.step,
    });
    debug_assert!(min <= max, "min ({min}) should be <= max ({max})");
    GeoScoreRange { min, max }
}

/// Calculate score ranges for the 9 geohash cells (center + 8 neighbors)
/// covering a radius around a point.
///
/// Returns a fixed-size array of [`GEO_RANGE_COUNT`] [`GeoScoreRange`] values.
/// Cells that are zero (outside the bounding box) or duplicate a previously
/// processed cell are left as default (both `min` and `max` are `0`).
pub fn calc_ranges(
    center: WGS84Coordinates,
    radius_meters: f64,
) -> [GeoScoreRange; GEO_RANGE_COUNT] {
    let georadius = hash::get_areas_by_radius(center, radius_meters);

    let cells: [Option<GeoHashBits>; GEO_RANGE_COUNT] = [
        Some(georadius.hash),
        georadius.neighbors.north,
        georadius.neighbors.south,
        georadius.neighbors.east,
        georadius.neighbors.west,
        georadius.neighbors.north_east,
        georadius.neighbors.north_west,
        georadius.neighbors.south_east,
        georadius.neighbors.south_west,
    ];

    let mut ranges = [GeoScoreRange::default(); GEO_RANGE_COUNT];
    // Tracks the index of the last processed cell for duplicate detection.
    // Starts at 0 so that cell[1] is never checked against cell[0].
    let mut last_processed: usize = 0;

    for (i, cell) in cells.iter().enumerate() {
        let Some(cell) = cell else {
            continue;
        };

        // Skip duplicate neighbors (can happen with very large radii).
        if last_processed != 0 && Some(*cell) == cells[last_processed] {
            continue;
        }

        ranges[i] = scores_of_geohash_box(*cell);

        last_processed = i;
    }

    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use decorum::R64;

    use crate::hash::{GEO_STEP_MAX, PrecisionStep, WGS84Coordinates, encode_wgs84};

    fn coords(lon: f64, lat: f64) -> WGS84Coordinates {
        WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap()
    }

    #[test]
    fn scores_of_geohash_box_min_less_than_max() {
        for step in [1, 5, 13, 26] {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(coords(10.0, 20.0), step);
            let range = scores_of_geohash_box(hash);
            assert!(
                range.min < range.max,
                "min ({}) should be less than max ({}) at step {}",
                range.min,
                range.max,
                step.as_u8()
            );
        }
    }

    #[test]
    fn scores_of_geohash_box_fits_in_52_bits() {
        let hash = encode_wgs84(coords(179.0, 85.0), GEO_STEP_MAX);
        let range = scores_of_geohash_box(hash);
        assert!(range.min < (1u64 << 52), "min should fit in 52 bits");
        assert!(range.max <= (1u64 << 52), "max should fit in 52 bits");
    }

    #[test]
    fn scores_of_geohash_box_adjacent_cells_are_contiguous() {
        let hash = encode_wgs84(coords(0.0, 0.0), GEO_STEP_MAX);
        let range = scores_of_geohash_box(hash);

        let next = GeoHashBits {
            bits: hash.bits + 1,
            step: hash.step,
        };
        let next_range = scores_of_geohash_box(next);

        assert_eq!(
            range.max, next_range.min,
            "max of one cell should equal min of the next"
        );
    }

    #[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
    mod proptests {
        use super::*;
        use crate::hash::{
            GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, PrecisionStep, WGS84Coordinates,
            encode_wgs84,
        };
        use decorum::R64;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn scores_min_less_than_max(
                lon in GEO_LONG_MIN..=GEO_LONG_MAX,
                lat in GEO_LAT_MIN..=GEO_LAT_MAX,
                step in 1u8..=26,
            ) {
                let step = PrecisionStep::new(step).unwrap();
                let hash = encode_wgs84(
                    WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(),
                    step,
                );
                let range = scores_of_geohash_box(hash);
                prop_assert!(
                    range.min < range.max,
                    "min ({}) should be < max ({}) at step={}",
                    range.min, range.max, step.as_u8()
                );
            }

            #[test]
            fn scores_fit_in_52_bits(
                lon in GEO_LONG_MIN..=GEO_LONG_MAX,
                lat in GEO_LAT_MIN..=GEO_LAT_MAX,
                step in 1u8..=26,
            ) {
                let step = PrecisionStep::new(step).unwrap();
                let hash = encode_wgs84(
                    WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(),
                    step,
                );
                let range = scores_of_geohash_box(hash);
                prop_assert!(
                    range.max <= (1u64 << 52),
                    "max ({}) should fit in 52 bits at step={}",
                    range.max, step.as_u8()
                );
            }

            #[test]
            fn calc_ranges_center_is_nonzero(
                lon in GEO_LONG_MIN..=GEO_LONG_MAX,
                lat in GEO_LAT_MIN..=GEO_LAT_MAX,
                radius in 1.0f64..=1_000_000.0,
            ) {
                let center = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap();
                let ranges = calc_ranges(center, radius);
                prop_assert_ne!(
                    ranges[0], GeoScoreRange::default(),
                    "center cell should be non-zero at ({}, {}) radius={}",
                    lon, lat, radius
                );
            }

            #[test]
            fn calc_ranges_nonzero_entries_have_min_less_than_max(
                lon in GEO_LONG_MIN..=GEO_LONG_MAX,
                lat in GEO_LAT_MIN..=GEO_LAT_MAX,
                radius in 1.0f64..=1_000_000.0,
            ) {
                let center = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap();
                let ranges = calc_ranges(center, radius);
                for (i, range) in ranges.iter().enumerate() {
                    if *range != GeoScoreRange::default() {
                        prop_assert!(
                            range.min < range.max,
                            "range[{}] min ({}) should be < max ({}) at ({}, {}) radius={}",
                            i, range.min, range.max, lon, lat, radius
                        );
                    }
                }
            }
        }
    }
}
