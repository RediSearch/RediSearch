/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Data types for geohash encoding.

use std::num::NonZeroU8;

use decorum::R64;

/// A WGS-84 coordinate pair with longitude and latitude validated to be
/// within the supported bounds.
///
/// Longitude is in [`GEO_LONG_MIN`]`..=`[`GEO_LONG_MAX`] and latitude is in
/// [`GEO_LAT_MIN`]`..=`[`GEO_LAT_MAX`] (EPSG:900913).
///
/// Use [`WGS84Coordinates::new`] to construct, which validates the bounds.
///
/// [`GEO_LONG_MIN`]: super::GEO_LONG_MIN
/// [`GEO_LONG_MAX`]: super::GEO_LONG_MAX
/// [`GEO_LAT_MIN`]: super::GEO_LAT_MIN
/// [`GEO_LAT_MAX`]: super::GEO_LAT_MAX
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WGS84Coordinates {
    /// Longitude in degrees, guaranteed to be in
    /// [`GEO_LONG_MIN`]`..=`[`GEO_LONG_MAX`].
    ///
    /// [`GEO_LONG_MIN`]: super::GEO_LONG_MIN
    /// [`GEO_LONG_MAX`]: super::GEO_LONG_MAX
    longitude: R64,
    /// Latitude in degrees, guaranteed to be in
    /// [`GEO_LAT_MIN`]`..=`[`GEO_LAT_MAX`].
    ///
    /// [`GEO_LAT_MIN`]: super::GEO_LAT_MIN
    /// [`GEO_LAT_MAX`]: super::GEO_LAT_MAX
    latitude: R64,
}

/// Error returned by [`WGS84Coordinates::new`] when a coordinate is outside
/// WGS-84 bounds.
///
/// Contains the out-of-bounds coordinate(s): [`longitude`](Self::longitude)
/// and/or [`latitude`](Self::latitude) are [`Some`] when outside bounds
/// (including non-finite values like NaN or infinity).
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("coordinates outside WGS-84 bounds{}", format_failing_coords(*.longitude, *.latitude))]
pub struct InvalidWGS84Coordinates {
    /// The longitude value, if it was outside bounds.
    pub longitude: Option<f64>,
    /// The latitude value, if it was outside bounds.
    pub latitude: Option<f64>,
}

fn format_failing_coords(longitude: Option<f64>, latitude: Option<f64>) -> String {
    match (longitude, latitude) {
        (Some(lon), Some(lat)) => format!(": longitude={lon}, latitude={lat}"),
        (Some(lon), None) => format!(": longitude={lon}"),
        (None, Some(lat)) => format!(": latitude={lat}"),
        (None, None) => String::new(),
    }
}

impl WGS84Coordinates {
    /// Longitude in degrees, in [`GEO_LONG_MIN`]`..=`[`GEO_LONG_MAX`].
    ///
    /// [`GEO_LONG_MIN`]: super::GEO_LONG_MIN
    /// [`GEO_LONG_MAX`]: super::GEO_LONG_MAX
    pub const fn longitude(self) -> R64 {
        self.longitude
    }

    /// Latitude in degrees, in [`GEO_LAT_MIN`]`..=`[`GEO_LAT_MAX`].
    ///
    /// [`GEO_LAT_MIN`]: super::GEO_LAT_MIN
    /// [`GEO_LAT_MAX`]: super::GEO_LAT_MAX
    pub const fn latitude(self) -> R64 {
        self.latitude
    }

    /// Create validated WGS-84 coordinates.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidWGS84Coordinates`] if `longitude` or `latitude` is
    /// outside the WGS-84 bounds.
    pub fn new(longitude: R64, latitude: R64) -> Result<Self, InvalidWGS84Coordinates> {
        let lon = longitude.into_inner();
        let lat = latitude.into_inner();
        let bad_lon = !(super::GEO_LONG_MIN..=super::GEO_LONG_MAX).contains(&lon);
        let bad_lat = !(super::GEO_LAT_MIN..=super::GEO_LAT_MAX).contains(&lat);
        if bad_lon || bad_lat {
            return Err(InvalidWGS84Coordinates {
                longitude: bad_lon.then_some(lon),
                latitude: bad_lat.then_some(lat),
            });
        }
        Ok(Self {
            longitude,
            latitude,
        })
    }

    /// Create validated WGS-84 coordinates from raw `f64` values.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidWGS84Coordinates`] if `longitude` or `latitude` is
    /// non-finite (NaN/infinity) or outside the WGS-84 bounds.
    pub fn from_f64(longitude: f64, latitude: f64) -> Result<Self, InvalidWGS84Coordinates> {
        let lon = R64::try_new(longitude);
        let lat = R64::try_new(latitude);
        match (lon, lat) {
            (Ok(lon), Ok(lat)) => Self::new(lon, lat),
            (lon, lat) => Err(InvalidWGS84Coordinates {
                longitude: lon.err().map(|_| longitude),
                latitude: lat.err().map(|_| latitude),
            }),
        }
    }
}

impl TryFrom<(f64, f64)> for WGS84Coordinates {
    type Error = InvalidWGS84Coordinates;

    /// Convert `(longitude, latitude)` into validated WGS-84 coordinates.
    fn try_from((longitude, latitude): (f64, f64)) -> Result<Self, Self::Error> {
        Self::from_f64(longitude, latitude)
    }
}

/// A validated geohash precision step in the range `1..=26`.
///
/// 26 steps × 2 bits = 52 bits of precision, which is the maximum
/// that fits in a Redis sorted-set score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrecisionStep(NonZeroU8);

/// Error returned by [`PrecisionStep::new`] when the value is outside `1..=26`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("precision step must be in 1..=26, got {0}")]
pub struct InvalidPrecisionStep(u8);

impl PrecisionStep {
    /// Create a new precision step.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidPrecisionStep`] if `step` is outside `1..=26`.
    pub const fn new(step: u8) -> Result<Self, InvalidPrecisionStep> {
        if step >= 1 && step <= 26 {
            Ok(Self(NonZeroU8::new(step).unwrap()))
        } else {
            Err(InvalidPrecisionStep(step))
        }
    }

    /// Return the raw step value.
    pub const fn as_u8(self) -> u8 {
        self.0.get()
    }
}

impl TryFrom<u8> for PrecisionStep {
    type Error = InvalidPrecisionStep;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A geohash value with its precision level.
///
/// Always represents a valid hash — use [`Option<GeoHashBits>`] where an
/// absent/empty cell is needed (e.g. excluded neighbors).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeoHashBits {
    /// The interleaved hash bits.
    pub bits: u64,
    /// The precision step (number of bits per coordinate, `1..=26`).
    pub step: PrecisionStep,
}

/// A min/max range for a single coordinate dimension.
///
/// Values are guaranteed to be finite ([`R64`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoHashRange {
    /// Minimum value (inclusive).
    pub min: R64,
    /// Maximum value (exclusive for geohash cells, inclusive for coordinate
    /// bounds).
    pub max: R64,
}

/// A decoded geohash area with latitude and longitude bounds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoHashArea {
    /// The original hash that was decoded.
    pub hash: GeoHashBits,
    /// Longitude bounds.
    pub longitude: GeoHashRange,
    /// Latitude bounds.
    pub latitude: GeoHashRange,
}

/// The 8 directional neighbors of a geohash cell.
///
/// Each neighbor is [`None`] if it falls outside the search bounding box.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GeoHashNeighbors {
    /// Northern neighbor.
    pub north: Option<GeoHashBits>,
    /// Southern neighbor.
    pub south: Option<GeoHashBits>,
    /// Eastern neighbor.
    pub east: Option<GeoHashBits>,
    /// Western neighbor.
    pub west: Option<GeoHashBits>,
    /// North-eastern neighbor.
    pub north_east: Option<GeoHashBits>,
    /// North-western neighbor.
    pub north_west: Option<GeoHashBits>,
    /// South-eastern neighbor.
    pub south_east: Option<GeoHashBits>,
    /// South-western neighbor.
    pub south_west: Option<GeoHashBits>,
}

/// Result of a radius query: the center cell, its area, and its neighbors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoHashRadius {
    /// The geohash of the center point.
    pub hash: GeoHashBits,
    /// The decoded area of the center cell.
    pub area: GeoHashArea,
    /// The 8 neighboring cells ([`None`] if outside the bounding box).
    pub neighbors: GeoHashNeighbors,
}
