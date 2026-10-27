/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use rqe_core::{FieldIndex, RS_INVALID_FIELD_INDEX};

use crate::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance};

/// Distance units accepted by geo filters, including an unrecognized token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoDistance {
    /// Unrecognized unit.
    Invalid,
    /// Kilometers.
    Kilometers,
    /// Meters.
    Meters,
    /// Feet.
    Feet,
    /// Miles.
    Miles,
}

impl GeoDistance {
    /// Parse case-insensitively, stopping at the first NUL as a C string does.
    pub fn parse(bytes: &[u8]) -> Self {
        let bytes = bytes.split(|&b| b == 0).next().unwrap_or_default();
        if bytes.eq_ignore_ascii_case(b"km") {
            Self::Kilometers
        } else if bytes.eq_ignore_ascii_case(b"m") {
            Self::Meters
        } else if bytes.eq_ignore_ascii_case(b"ft") {
            Self::Feet
        } else if bytes.eq_ignore_ascii_case(b"mi") {
            Self::Miles
        } else {
            Self::Invalid
        }
    }

    /// The bounded C unit parser rejects buffers of at least 16 bytes.
    ///
    /// Shorter buffers follow [`Self::parse`], including embedded NULs.
    pub fn parse_buffer(bytes: &[u8]) -> Self {
        if bytes.len() >= 16 {
            Self::Invalid
        } else {
            Self::parse(bytes)
        }
    }

    /// Canonical spelling, or the C placeholder for [`Self::Invalid`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "<badunit>",
            Self::Kilometers => "km",
            Self::Meters => "m",
            Self::Feet => "ft",
            Self::Miles => "mi",
        }
    }

    /// Multiplier converting this unit to meters.
    ///
    /// # Panics
    ///
    /// Panics for [`Self::Invalid`], corresponding to the C helper's abort.
    pub const fn factor(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
            Self::Invalid => panic!("invalid GeoDistance unit"),
        }
    }
}

/// A filter validation error, in the same precedence order as the C validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GeoFilterError {
    /// The unit was not recognized.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// Longitude or latitude is out of range.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// Radius is non-positive.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

/// A geo query specification, independent of the iterator's numeric-filter cache.
///
/// Unlike the C allocation, this value needs no explicit free. Its construction
/// does not imply validation; call [`Self::validate`] separately.
#[derive(Debug, Clone, PartialEq)]
pub struct GeoFilter {
    /// Stable field index, or [`RS_INVALID_FIELD_INDEX`] before resolution.
    pub field_index: FieldIndex,
    /// Longitude in degrees.
    pub lon: f64,
    /// Latitude in degrees.
    pub lat: f64,
    /// Radius in [`Self::unit`].
    pub radius: f64,
    /// Distance unit.
    pub unit: GeoDistance,
}

impl GeoFilter {
    /// Construct without validating. An absent unit defaults to kilometers;
    /// a supplied unit follows [`GeoDistance::parse_buffer`].
    pub fn new(lon: f64, lat: f64, radius: f64, unit: Option<&[u8]>) -> Self {
        Self {
            field_index: RS_INVALID_FIELD_INDEX,
            lon,
            lat,
            radius,
            unit: unit.map_or(GeoDistance::Kilometers, GeoDistance::parse_buffer),
        }
    }

    /// Set the resolved field index, or reset it to [`RS_INVALID_FIELD_INDEX`].
    pub fn set_field(&mut self, index: Option<FieldIndex>) {
        self.field_index = index.unwrap_or(RS_INVALID_FIELD_INDEX);
    }

    /// Validate units, coordinate bounds, then radius, preserving C error precedence.
    ///
    /// Comparison-based validation deliberately accepts NaN, and a positive infinite
    /// radius. Latitude bounds here are wider than the geohash projection's bounds.
    pub fn validate(&self) -> Result<(), GeoFilterError> {
        if self.unit == GeoDistance::Invalid {
            return Err(GeoFilterError::Unit);
        }
        if self.lat > 90.0 || self.lat < -90.0 || self.lon > 180.0 || self.lon < -180.0 {
            return Err(GeoFilterError::Coordinates);
        }
        if self.radius <= 0.0 {
            return Err(GeoFilterError::Radius);
        }
        Ok(())
    }

    /// Decode a numeric geohash score and return containment and distance in meters.
    ///
    /// The boundary is inclusive. Score conversion follows the existing Rust-backed
    /// C decoder's saturating float-to-integer cast, including non-finite scores.
    ///
    /// # Panics
    ///
    /// Panics if the unit is [`GeoDistance::Invalid`] or the center is non-finite,
    /// matching the existing C helpers. [`Self::validate`] does not reject NaN.
    pub fn is_within_radius(&self, score: f64) -> (bool, f64) {
        let (lon, lat) = decode_score(score);
        let radius_meters = self.radius * self.unit.factor();
        let distance = haversine_distance(R64::assert(self.lon), R64::assert(self.lat), lon, lat);
        (distance <= radius_meters, distance)
    }
}

pub(crate) fn decode_score(score: f64) -> (R64, R64) {
    decode_to_lon_lat(GeoHashBits {
        bits: score as u64,
        step: GEO_STEP_MAX,
    })
}
