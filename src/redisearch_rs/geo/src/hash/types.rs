/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Data types for geohash encoding.

/// A geohash value with its precision level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeoHashBits {
    /// The interleaved hash bits.
    pub bits: u64,
    /// The precision step (number of bits per coordinate, max 26).
    pub step: u8,
}

impl GeoHashBits {
    /// A zero/empty geohash.
    pub const ZERO: Self = Self { bits: 0, step: 0 };

    /// Returns `true` if both `bits` and `step` are zero.
    pub const fn is_zero(self) -> bool {
        self.bits == 0 && self.step == 0
    }
}

/// A min/max range for a single coordinate dimension.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GeoHashRange {
    /// Minimum value (inclusive).
    pub min: f64,
    /// Maximum value (exclusive for geohash cells, inclusive for coordinate
    /// bounds).
    pub max: f64,
}

/// A decoded geohash area with latitude and longitude bounds.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GeoHashArea {
    /// The original hash that was decoded.
    pub hash: GeoHashBits,
    /// Longitude bounds.
    pub longitude: GeoHashRange,
    /// Latitude bounds.
    pub latitude: GeoHashRange,
}

/// The 8 directional neighbors of a geohash cell.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeoHashNeighbors {
    /// Northern neighbor.
    pub north: GeoHashBits,
    /// Southern neighbor.
    pub south: GeoHashBits,
    /// Eastern neighbor.
    pub east: GeoHashBits,
    /// Western neighbor.
    pub west: GeoHashBits,
    /// North-eastern neighbor.
    pub north_east: GeoHashBits,
    /// North-western neighbor.
    pub north_west: GeoHashBits,
    /// South-eastern neighbor.
    pub south_east: GeoHashBits,
    /// South-western neighbor.
    pub south_west: GeoHashBits,
}

/// Result of a radius query: the center cell, its area, and its neighbors.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GeoHashRadius {
    /// The geohash of the center point.
    pub hash: GeoHashBits,
    /// The decoded area of the center cell.
    pub area: GeoHashArea,
    /// The 8 neighboring cells (zeroed if outside the bounding box).
    pub neighbors: GeoHashNeighbors,
}
