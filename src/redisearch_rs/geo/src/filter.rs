/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GEO query parameters, independent of index ownership and command parsing.

use std::ffi::CStr;

use decorum::R64;

use crate::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance};

/// Units accepted by GEO filters, with the existing C `GeoDistance` discriminants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(i32)]
pub enum DistanceUnit {
    /// Kilometers, the default when no unit is provided.
    #[default]
    Kilometers = 0,
    /// Meters.
    Meters = 1,
    /// Feet.
    Feet = 2,
    /// Miles, using the historical GEO conversion factor.
    Miles = 3,
}

impl DistanceUnit {
    /// Parse an ASCII case-insensitive unit, without trimming whitespace.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [b'k' | b'K', b'm' | b'M'] => Some(Self::Kilometers),
            [b'm' | b'M'] => Some(Self::Meters),
            [b'f' | b'F', b't' | b'T'] => Some(Self::Feet),
            [b'm' | b'M', b'i' | b'I'] => Some(Self::Miles),
            _ => None,
        }
    }

    /// Parse the legacy length-delimited unit representation.
    ///
    /// The original C API copied into a 16-byte zeroed buffer: buffers of length
    /// 16 or greater are invalid, and an embedded NUL ends a shorter unit.
    pub fn parse_buffer(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 16 {
            return None;
        }
        let end = bytes
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(bytes.len());
        Self::parse(&bytes[..end])
    }

    /// The canonical lowercase, NUL-terminated unit name.
    pub const fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
        }
    }

    /// Multiplier converting this unit to meters.
    pub const fn meters(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
        }
    }
}

/// The first error found by [`FilterParams::validate`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FilterError {
    /// No recognized distance unit was supplied.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// Latitude or longitude exceeds the query bounds.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// The radius is non-positive.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

/// Unvalidated GEO query parameters, without field or numeric-filter ownership.
#[derive(Clone, Copy, Debug)]
pub struct FilterParams {
    /// Center longitude in degrees.
    pub lon: f64,
    /// Center latitude in degrees.
    pub lat: f64,
    /// Search radius in [`Self::unit`].
    pub radius: f64,
    /// [`None`] represents an unrecognized unit, reported before numeric errors.
    pub unit: Option<DistanceUnit>,
}

impl FilterParams {
    /// Preserve C filter validation, including its error precedence.
    ///
    /// Query bounds are ±90° latitude and ±180° longitude, not the narrower
    /// indexing bounds of [`crate::hash::WGS84Coordinates`]. These are comparison-only
    /// checks: NaN passes each numeric check and positive infinity passes the
    /// radius check. Command parsing rejects NaN separately; success here alone
    /// does not establish the preconditions of [`Self::within_radius`].
    pub const fn validate(&self) -> Result<(), FilterError> {
        if self.unit.is_none() {
            return Err(FilterError::Unit);
        }
        if self.lat.abs() > 90.0 || self.lon.abs() > 180.0 {
            return Err(FilterError::Coordinates);
        }
        if self.radius <= 0.0 {
            return Err(FilterError::Radius);
        }
        Ok(())
    }

    /// Test an encoded geohash score against the inclusive radius.
    ///
    /// Returns both the match and the distance in meters, including on a miss.
    /// Score decoding follows [`decode_to_lon_lat`] at [`GEO_STEP_MAX`].
    ///
    /// # Panics
    ///
    /// Panics if the unit is [`None`] or the center coordinates are not finite,
    /// as does the existing C path through the Rust distance entry point.
    pub fn within_radius(&self, score: f64) -> (bool, f64) {
        let unit = self.unit.expect("invalid GeoDistance unit");
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        let distance = haversine_distance(R64::assert(self.lon), R64::assert(self.lat), lon, lat);
        (distance <= self.radius * unit.meters(), distance)
    }
}
