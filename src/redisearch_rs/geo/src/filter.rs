/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;

use decorum::R64;

use crate::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance};

/// Distance units accepted by geo queries, with their historical C discriminants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum DistanceUnit {
    /// A supplied unit could not be parsed; distinct from an omitted unit.
    Invalid = -1,
    Kilometers = 0,
    Meters = 1,
    Feet = 2,
    Miles = 3,
}

impl DistanceUnit {
    /// Decode a C discriminant without interpreting an arbitrary integer as an enum.
    pub const fn from_code(code: i32) -> Option<Self> {
        match code {
            -1 => Some(Self::Invalid),
            0 => Some(Self::Kilometers),
            1 => Some(Self::Meters),
            2 => Some(Self::Feet),
            3 => Some(Self::Miles),
            _ => None,
        }
    }

    /// Parse a case-insensitive C unit string, without a buffer-length restriction.
    pub const fn parse(s: &CStr) -> Self {
        Self::parse_bytes(s.to_bytes())
    }

    /// Preserve the legacy buffer parser: reject lengths at least [`UNIT_BUFFER_SIZE`]
    /// before truncating at the first NUL. Non-UTF-8 input is accepted as bytes.
    pub fn parse_buffer(bytes: &[u8]) -> Self {
        if bytes.len() >= UNIT_BUFFER_SIZE {
            return Self::Invalid;
        }
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        Self::parse_bytes(&bytes[..end])
    }

    const fn parse_bytes(bytes: &[u8]) -> Self {
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

    /// Canonical spelling, including the historical invalid-unit placeholder.
    pub const fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
            Self::Invalid => c"<badunit>",
        }
    }

    /// Multiplier converting this unit to meters. Miles retain the original rounded factor.
    ///
    /// [`Self::Invalid`] retains C's negative fallback when assertions are disabled.
    /// Query validation rejects it, but unvalidated radius evaluation must not panic.
    pub const fn meters_factor(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
            Self::Invalid => -1.0,
        }
    }
}

/// Size of the historical unit parser's temporary C buffer, including its NUL terminator.
pub const UNIT_BUFFER_SIZE: usize = 16;

/// Query parameters independent of index metadata and numeric-range allocation ownership.
#[derive(Clone, Copy, Debug)]
pub struct RadiusFilter {
    /// Query-center longitude in degrees.
    pub lon: f64,
    /// Query-center latitude in degrees; query validation permits the geographic poles.
    pub lat: f64,
    /// Query radius in [`Self::unit`].
    pub radius: f64,
    /// Unit supplied by the query, including [`DistanceUnit::Invalid`] during parsing.
    pub unit: DistanceUnit,
}

impl RadiusFilter {
    /// Construct without validation. Only an omitted unit defaults to kilometers.
    pub fn new(lon: f64, lat: f64, radius: f64, unit: Option<&[u8]>) -> Self {
        Self {
            lon,
            lat,
            radius,
            unit: unit.map_or(DistanceUnit::Kilometers, DistanceUnit::parse_buffer),
        }
    }

    /// Validate in the original order: unit, coordinates, then radius.
    ///
    /// For compatibility this uses C's ordered comparisons: NaNs pass validation,
    /// as do positive-infinite radii. Latitude bounds are wider than indexed WGS-84
    /// coordinates. Neither behavior implies those values can be indexed or evaluated.
    pub fn validate(&self) -> Result<(), FilterError> {
        if self.unit == DistanceUnit::Invalid {
            return Err(FilterError::Unit);
        }
        // A range containment check would reject NaN, changing the C contract.
        if self.lat > 90.0 || self.lat < -90.0 || self.lon > 180.0 || self.lon < -180.0 {
            return Err(FilterError::Coordinates);
        }
        if self.radius <= 0.0 {
            return Err(FilterError::Radius);
        }
        Ok(())
    }

    /// Return inclusion and distance in meters for a geohash score.
    ///
    /// The boundary is inclusive. A NaN radius excludes every point, and positive
    /// infinity includes every finite-distance point. Score decoding uses the same
    /// saturating float-to-integer conversion as the existing Rust `decodeGeo` bridge.
    ///
    /// # Panics
    ///
    /// Panics if the center is non-finite, matching the existing radius checker.
    /// [`Self::validate`] alone does not exclude NaN centers; query evaluation
    /// separately checks WGS-84 coordinates.
    pub fn within_radius(&self, score: f64) -> (bool, f64) {
        let point = GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        };
        let (lon, lat) = decode_to_lon_lat(point);
        let radius = self.radius * self.unit.meters_factor();
        let distance = haversine_distance(R64::assert(self.lon), R64::assert(self.lat), lon, lat);
        (distance <= radius, distance)
    }
}

/// The first invalid geo-filter parameter, preserving client-visible error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FilterError {
    #[error("Invalid GeoFilter unit")]
    Unit,
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    #[error("Invalid GeoFilter radius")]
    Radius,
}
