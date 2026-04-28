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

use crate::hash::{GeoHashBits, align_52bits, get_areas_by_radius};

/// The number of geohash ranges: the center cell plus its 8 neighbors.
pub const GEO_RANGE_COUNT: usize = 9;

/// A min/max score range for a geohash cell, suitable for sorted-set queries.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GeoScoreRange {
    /// Minimum score (inclusive).
    pub min: f64,
    /// Maximum score (exclusive).
    pub max: f64,
}

/// Compute the min (inclusive) and max (exclusive) sorted-set scores for a
/// geohash box.
const fn scores_of_geohash_box(hash: GeoHashBits) -> (u64, u64) {
    let min = align_52bits(hash);
    let max = align_52bits(GeoHashBits {
        bits: hash.bits.wrapping_add(1),
        step: hash.step,
    });
    (min, max)
}

/// Calculate score ranges for the 9 geohash cells (center + 8 neighbors)
/// covering a radius around a point.
///
/// Returns a fixed-size array of [`GEO_RANGE_COUNT`] [`GeoScoreRange`] values.
/// Cells that are zero (outside the bounding box) or duplicate a previously
/// processed cell are left as default (both `min` and `max` are `0.0`).
pub fn calc_ranges(
    longitude: f64,
    latitude: f64,
    radius_meters: f64,
) -> [GeoScoreRange; GEO_RANGE_COUNT] {
    let georadius = get_areas_by_radius(longitude, latitude, radius_meters);

    let cells = [
        georadius.hash,
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
        if cell.is_zero() {
            continue;
        }

        // Skip duplicate neighbors (can happen with very large radii).
        if last_processed != 0 && *cell == cells[last_processed] {
            continue;
        }

        let (min, max) = scores_of_geohash_box(*cell);
        ranges[i] = GeoScoreRange {
            min: min as f64,
            max: max as f64,
        };

        last_processed = i;
    }

    ranges
}
