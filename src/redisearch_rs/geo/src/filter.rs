/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Radius filters over indexed geohash scores.

use std::ffi::CStr;

use decorum::R64;

use crate::{Coordinates, hash};

/// Units accepted by geographic queries.
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
    /// Parse an ASCII case-insensitive unit, without trimming or NUL termination.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [b'm' | b'M'] => Some(Self::Meters),
            [b'k' | b'K', b'm' | b'M'] => Some(Self::Kilometers),
            [b'f' | b'F', b't' | b'T'] => Some(Self::Feet),
            [b'm' | b'M', b'i' | b'I'] => Some(Self::Miles),
            _ => None,
        }
    }

    /// Canonical spelling used in query explanations.
    pub const fn as_c_str(self) -> &'static CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
        }
    }

    /// Number of meters in one unit.
    pub const fn meters(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
        }
    }
}

/// A malformed radius query, in validation priority order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FilterError {
    /// The unit is not recognized.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// The center is non-finite or outside geographic bounds.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// The radius is non-positive or NaN.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

/// A validated circle whose radius is stored in meters.
///
/// Query centers allow the full geographic latitude range, including the poles;
/// indexed coordinates have the narrower [`hash::WGS84Coordinates`] bounds.
#[derive(Clone, Copy, Debug)]
pub struct RadiusFilter {
    center: Coordinates,
    radius_meters: f64,
}

impl RadiusFilter {
    /// Validate a query, checking the unit, coordinates, then radius.
    ///
    /// Positive infinity is a supported radius. Returns [`FilterError`] for an
    /// unknown unit, coordinates outside geographic bounds, or a non-positive/NaN radius.
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
            center: Coordinates {
                lon: R64::assert(lon),
                lat: R64::assert(lat),
            },
            radius_meters: radius * unit.meters(),
        })
    }

    /// Distance in meters from the center to an indexed geohash score.
    pub fn distance_to_score(self, score: f64) -> f64 {
        let (lon, lat) = hash::decode_to_lon_lat(hash::GeoHashBits {
            bits: score as u64,
            step: hash::GEO_STEP_MAX,
        });
        hash::haversine_distance(self.center.lon, self.center.lat, lon, lat)
    }

    /// Return inclusive radius membership and the distance in meters, even on a miss.
    pub fn contains_score(self, score: f64) -> (bool, f64) {
        let distance = self.distance_to_score(score);
        (distance <= self.radius_meters, distance)
    }
}
