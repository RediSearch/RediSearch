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

/// Units accepted by geo radius queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
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
    /// Parse an ASCII case-insensitive unit abbreviation, without whitespace.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        [Self::Kilometers, Self::Meters, Self::Feet, Self::Miles]
            .into_iter()
            .find(|unit| bytes.eq_ignore_ascii_case(unit.as_c_str().to_bytes()))
    }

    /// Decode the integer unit stored in the C filter layout.
    pub const fn from_raw(unit: i32) -> Option<Self> {
        match unit {
            0 => Some(Self::Kilometers),
            1 => Some(Self::Meters),
            2 => Some(Self::Feet),
            3 => Some(Self::Miles),
            _ => None,
        }
    }

    /// Canonical abbreviation for query explanations and the C API.
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

/// A validated query circle; its center can reach the poles even though indexed
/// points are restricted to the Mercator latitude bounds.
#[derive(Debug, Clone, Copy)]
pub struct RadiusFilter {
    lon: R64,
    lat: R64,
    radius_meters: f64,
}

impl RadiusFilter {
    /// Validate query parameters, reporting unit, coordinate, then radius errors.
    /// A positive infinite radius covers the entire globe.
    pub fn new(
        lon: f64,
        lat: f64,
        radius: f64,
        unit: Option<DistanceUnit>,
    ) -> Result<Self, &'static str> {
        let unit = unit.ok_or("Invalid GeoFilter unit")?;
        if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
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

    /// Test a stored geohash score, returning inclusion and distance in meters.
    /// Points exactly on the circle are included.
    pub fn contains(self, score: f64) -> (bool, f64) {
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        let distance = haversine_distance(self.lon, self.lat, lon, lat);
        (distance <= self.radius_meters, distance)
    }
}
