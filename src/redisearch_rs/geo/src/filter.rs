/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, c_void};

use decorum::R64;
use rqe_core::{FieldIndex, RS_INVALID_FIELD_INDEX};

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

/// C unit code; unrecognized values are rejected by [`Unit::from_raw`].
#[cheadergen::config(export)]
pub type GeoDistance = i32;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_INVALID: GeoDistance = -1;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_KM: GeoDistance = Unit::Kilometers as i32;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_M: GeoDistance = Unit::Meters as i32;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_FT: GeoDistance = Unit::Feet as i32;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_MI: GeoDistance = Unit::Miles as i32;

/// Shared query filter layout. The numeric range allocations are owned by the
/// query and released by the iterator subsystem, not by copies of this value.
#[derive(Clone, Copy)]
#[repr(C)]
#[expect(non_snake_case, reason = "shared C field names")]
#[cheadergen::config(export)]
pub struct GeoFilter {
    /// Stable field index; schema growth can relocate field pointers.
    pub fieldIndex: FieldIndex,
    pub lat: f64,
    pub lon: f64,
    pub radius: f64,
    pub unitType: GeoDistance,
    /// Opaque array of owned numeric range filter pointers.
    pub numericFilters: *mut *mut c_void,
}

impl GeoFilter {
    /// Constructs an unbound filter, deferring validation until query evaluation.
    pub const fn new(lon: f64, lat: f64, radius: f64, unit: GeoDistance) -> Self {
        Self {
            fieldIndex: RS_INVALID_FIELD_INDEX,
            lat,
            lon,
            radius,
            unitType: unit,
            numericFilters: std::ptr::null_mut(),
        }
    }

    /// Validates this C-facing value into a safe query circle.
    pub fn radius_filter(&self) -> Result<RadiusFilter, &'static str> {
        RadiusFilter::new(
            self.lon,
            self.lat,
            self.radius,
            Unit::from_raw(self.unitType),
        )
    }
}

/// Legacy syntax retains the field name until a schema is available.
#[repr(C)]
#[cheadergen::config(export)]
pub struct LegacyGeoFilter {
    pub base: GeoFilter,
    /// Owned HiddenString wrapper borrowing the command argument's bytes.
    pub field: *mut c_void,
}
