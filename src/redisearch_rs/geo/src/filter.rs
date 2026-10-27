/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Legacy geo-filter units and parameter validation.

use std::ffi::CStr;

/// Distance units accepted by geo filters, in the historical C order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum GeoDistance {
    /// Unrecognized unit.
    Invalid = -1,
    /// Kilometers.
    Kilometers = 0,
    /// Meters.
    Meters = 1,
    /// Feet.
    Feet = 2,
    /// Miles.
    Miles = 3,
}

impl GeoDistance {
    /// Parse a length-delimited unit with the legacy buffer limit and C-string semantics.
    pub fn parse_buffer(bytes: &[u8]) -> Self {
        if bytes.len() >= 16 {
            return Self::Invalid;
        }
        Self::parse(bytes.split(|&b| b == 0).next().unwrap_or_default())
    }

    /// Parse a unit, ignoring ASCII case.
    pub fn parse(bytes: &[u8]) -> Self {
        for unit in [Self::Kilometers, Self::Meters, Self::Feet, Self::Miles] {
            if bytes.eq_ignore_ascii_case(unit.as_c_str().to_bytes()) {
                return unit;
            }
        }
        Self::Invalid
    }

    /// Convert a C discriminant, mapping unknown values to [`Self::Invalid`].
    pub const fn from_raw(unit: i32) -> Self {
        match unit {
            0 => Self::Kilometers,
            1 => Self::Meters,
            2 => Self::Feet,
            3 => Self::Miles,
            _ => Self::Invalid,
        }
    }

    /// Return the canonical unit spelling, or the historical invalid-unit marker.
    pub const fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
            Self::Invalid => c"<badunit>",
        }
    }

    /// Return the conversion factor to meters.
    ///
    /// # Panics
    /// Panics for [`Self::Invalid`].
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

/// Coordinate and radius parameters, before legacy validation.
#[derive(Clone, Copy, Debug)]
pub struct FilterParameters {
    /// Longitude in degrees.
    pub lon: f64,
    /// Latitude in degrees.
    pub lat: f64,
    /// Radius in the selected unit.
    pub radius: f64,
}

impl FilterParameters {
    /// Validate using the original ordered comparisons.
    ///
    /// NaN coordinates and radii intentionally pass these comparisons. This is
    /// the legacy validation contract; it does not imply they can be evaluated.
    pub fn validate(self) -> Result<(), &'static str> {
        if self.lat > 90.0 || self.lat < -90.0 || self.lon > 180.0 || self.lon < -180.0 {
            Err("Invalid GeoFilter lat/lon")
        } else if self.radius <= 0.0 {
            Err("Invalid GeoFilter radius")
        } else {
            Ok(())
        }
    }
}

/// Great-circle distance in meters, rounded to two decimals for aggregation.
/// Non-finite coordinates produce NaN, as in the existing distance wrapper.
pub fn aggregate_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    use decorum::R64;
    let (Ok(lon1), Ok(lat1), Ok(lon2), Ok(lat2)) = (
        R64::try_new(lon1),
        R64::try_new(lat1),
        R64::try_new(lon2),
        R64::try_new(lat2),
    ) else {
        return f64::NAN;
    };
    (crate::hash::haversine_distance(lon1, lat1, lon2, lat2) * 100.0).round() / 100.0
}
