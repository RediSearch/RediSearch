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

/// Distance units accepted by geo radius queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unit {
    /// Kilometers.
    Kilometers,
    /// Meters.
    Meters,
    /// Feet.
    Feet,
    /// Miles.
    Miles,
}

impl Unit {
    /// Parses a case-insensitive unit buffer, terminated by its first NUL.
    /// Buffers of sixteen or more bytes are rejected.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 16 {
            return None;
        }
        let bytes = bytes.split(|b| *b == 0).next().unwrap_or_default();
        [Self::Kilometers, Self::Meters, Self::Feet, Self::Miles]
            .into_iter()
            .find(|unit| bytes.eq_ignore_ascii_case(unit.as_c_str().to_bytes()))
    }

    /// Converts a C unit code, rejecting unrecognized values.
    pub const fn from_raw(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Kilometers),
            1 => Some(Self::Meters),
            2 => Some(Self::Feet),
            3 => Some(Self::Miles),
            _ => None,
        }
    }

    /// Canonical query spelling.
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

/// A validated query circle. Query latitude bounds include the poles, even
/// though indexing uses the narrower geohash latitude bounds.
#[derive(Clone, Copy, Debug)]
pub struct RadiusFilter {
    lon: R64,
    lat: R64,
    radius_meters: f64,
}

impl RadiusFilter {
    /// Validates units, coordinates, then radius, returning the query error text.
    /// Positive infinite radii are accepted; NaN is not.
    pub fn new(lon: f64, lat: f64, radius: f64, unit: Option<Unit>) -> Result<Self, &'static str> {
        let unit = unit.ok_or("Invalid GeoFilter unit")?;
        if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
            return Err("Invalid GeoFilter lat/lon");
        }
        if radius.is_nan() || radius <= 0.0 {
            return Err("Invalid GeoFilter radius");
        }
        Ok(Self {
            lon: R64::assert(lon),
            lat: R64::assert(lat),
            radius_meters: radius * unit.meters(),
        })
    }

    /// Distance in meters from this circle's center to an indexed geohash score.
    pub fn distance(self, score: f64) -> f64 {
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        haversine_distance(self.lon, self.lat, lon, lat)
    }

    /// Whether a distance in meters lies inside or on the circle.
    pub fn contains(self, distance: f64) -> bool {
        distance <= self.radius_meters
    }
}
