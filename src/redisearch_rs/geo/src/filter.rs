/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Radius filters use geographic latitude bounds, not the narrower geohash bounds.

use std::ffi::CStr;

use decorum::R64;

use crate::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance};

/// Distance units accepted by geo queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceUnit {
    /// Kilometers.
    Kilometers,
    /// Meters.
    Meters,
    /// Feet.
    Feet,
    /// Miles.
    Miles,
}

impl DistanceUnit {
    /// Parse a case-insensitive unit name, stopping at the first NUL.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let bytes = bytes.split(|&b| b == 0).next().unwrap_or_default();
        [Self::Kilometers, Self::Meters, Self::Feet, Self::Miles]
            .into_iter()
            .find(|unit| bytes.eq_ignore_ascii_case(unit.as_c_str().to_bytes()))
    }

    /// Parse a bounded query token. Tokens occupying 16 bytes or more are invalid.
    pub fn parse_buffer(bytes: &[u8]) -> Option<Self> {
        (bytes.len() < 16).then(|| Self::parse(bytes)).flatten()
    }

    /// Canonical lowercase spelling, including its terminating NUL.
    pub const fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
        }
    }

    /// Meters per unit, shared by range construction and exact distance filtering.
    pub const fn meters(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
        }
    }
}

/// Invalid radius-query parameters, in validation precedence order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FilterError {
    /// Unrecognized distance unit.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// Non-finite or out-of-range coordinates.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// Radius is NaN or non-positive.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

/// A validated geographic center and positive radius in meters.
#[derive(Debug, Clone, Copy)]
pub struct RadiusFilter {
    lon: R64,
    lat: R64,
    radius_meters: f64,
}

impl RadiusFilter {
    /// Validate query parameters; positive infinite radii are accepted.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError::Unit`] first, then [`FilterError::Coordinates`] for
    /// longitude outside ±180° or latitude outside ±90°, then [`FilterError::Radius`].
    pub fn new(
        lon: f64,
        lat: f64,
        radius: f64,
        unit: Option<DistanceUnit>,
    ) -> Result<Self, FilterError> {
        let unit = unit.ok_or(FilterError::Unit)?;
        if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
            return Err(FilterError::Coordinates);
        }
        if radius.is_nan() || radius <= 0.0 {
            return Err(FilterError::Radius);
        }
        Ok(Self {
            lon: R64::assert(lon),
            lat: R64::assert(lat),
            radius_meters: radius * unit.meters(),
        })
    }

    /// Radius in meters, including infinity for an unbounded query.
    pub const fn radius_meters(self) -> f64 {
        self.radius_meters
    }

    /// Distance in meters from the center to an indexed geohash score.
    pub fn distance(self, score: f64) -> f64 {
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        haversine_distance(self.lon, self.lat, lon, lat)
    }
}

/// Shared C layout; validation converts raw fields into the safe domain.
pub mod abi {
    use super::{DistanceUnit, FilterError, RadiusFilter};
    use std::ffi::{c_int, c_void};

    #[cheadergen::config(export)]
    pub const GEO_DISTANCE_INVALID: c_int = -1;
    #[cheadergen::config(export)]
    pub const GEO_DISTANCE_KM: c_int = 0;
    #[cheadergen::config(export)]
    pub const GEO_DISTANCE_M: c_int = 1;
    #[cheadergen::config(export)]
    pub const GEO_DISTANCE_FT: c_int = 2;
    #[cheadergen::config(export)]
    pub const GEO_DISTANCE_MI: c_int = 3;

    /// Unvalidated query parameters and owned numeric ranges.
    #[cheadergen::config(export)]
    #[expect(non_snake_case, reason = "C ABI field names")]
    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    pub struct GeoFilter {
        /// Stable index, rather than a field pointer invalidated by schema changes.
        pub fieldIndex: u16,
        pub lat: f64,
        pub lon: f64,
        pub radius: f64,
        pub unitType: c_int,
        /// Owned array, released through the iterator subsystem's matching allocator.
        pub numericFilters: *mut c_void,
    }

    /// Legacy syntax keeps a borrowed field-name wrapper until schema resolution.
    #[cheadergen::config(export)]
    #[repr(C)]
    pub struct LegacyGeoFilter {
        pub base: GeoFilter,
        pub field: *mut c_void,
    }

    impl GeoFilter {
        /// Convert untrusted ABI fields into validated radius-query parameters.
        ///
        /// # Errors
        /// Returns the validation errors of [`RadiusFilter::new`].
        pub fn validated(&self) -> Result<RadiusFilter, FilterError> {
            RadiusFilter::new(self.lon, self.lat, self.radius, unit(self.unitType))
        }
    }

    /// Convert an integer ABI tag to a recognized distance unit.
    pub const fn unit(value: c_int) -> Option<DistanceUnit> {
        match value {
            GEO_DISTANCE_KM => Some(DistanceUnit::Kilometers),
            GEO_DISTANCE_M => Some(DistanceUnit::Meters),
            GEO_DISTANCE_FT => Some(DistanceUnit::Feet),
            GEO_DISTANCE_MI => Some(DistanceUnit::Miles),
            _ => None,
        }
    }
}
