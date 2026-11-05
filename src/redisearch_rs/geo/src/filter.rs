/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Geo radius filters: the units their radius is given in, the validation of
//! their parameters, and the check each indexed geohash goes through.

use decorum::R64;

use crate::hash::{self, GEO_STEP_MAX, GeoHashBits};

/// A unit a geo filter's radius is given in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceUnit {
    Kilometers,
    Meters,
    Feet,
    Miles,
}

impl DistanceUnit {
    /// Parses a unit from its [name](Self::name), ignoring ASCII case.
    pub fn parse(name: &[u8]) -> Option<Self> {
        [Self::Kilometers, Self::Meters, Self::Feet, Self::Miles]
            .into_iter()
            .find(|unit| unit.name().to_bytes().eq_ignore_ascii_case(name))
    }

    /// The name queries spell the unit with.
    pub const fn name(self) -> &'static std::ffi::CStr {
        match self {
            Self::Kilometers => c"km",
            Self::Meters => c"m",
            Self::Feet => c"ft",
            Self::Miles => c"mi",
        }
    }

    /// The length of one unit in meters.
    pub const fn meters(self) -> f64 {
        match self {
            Self::Kilometers => 1000.0,
            Self::Meters => 1.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
        }
    }
}

/// Why [`RadiusFilter::new`] rejects a filter. Displays as the error users see.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidGeoFilter {
    /// The unit did not parse.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// The latitude is outside `[-90, 90]` or the longitude outside `[-180, 180]`.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// The radius is not positive.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

/// A circle on the earth's surface that a geo filter matches the points within.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RadiusFilter {
    /// Longitude of the center, in degrees.
    pub lon: f64,
    /// Latitude of the center, in degrees.
    pub lat: f64,
    /// Radius, in [`unit`](Self::unit)s.
    pub radius: f64,
    /// The unit [`radius`](Self::radius) is given in.
    pub unit: DistanceUnit,
}

impl RadiusFilter {
    /// Validates a filter whose unit may have failed to parse ([`None`]),
    /// reporting the unit first, then the coordinates, then the radius.
    ///
    /// NaN coordinates and a NaN radius pass, so the filter matches nothing
    /// rather than failing the query.
    pub fn new(
        lon: f64,
        lat: f64,
        radius: f64,
        unit: Option<DistanceUnit>,
    ) -> Result<Self, InvalidGeoFilter> {
        let unit = unit.ok_or(InvalidGeoFilter::Unit)?;
        // `contains` would reject NaN, which must pass.
        #[expect(clippy::manual_range_contains)]
        if lat > 90.0 || lat < -90.0 || lon > 180.0 || lon < -180.0 {
            return Err(InvalidGeoFilter::Coordinates);
        }
        if radius <= 0.0 {
            return Err(InvalidGeoFilter::Radius);
        }
        Ok(Self {
            lon,
            lat,
            radius,
            unit,
        })
    }

    /// The radius in meters.
    pub const fn radius_meters(&self) -> f64 {
        self.radius * self.unit.meters()
    }

    /// The great-circle distance in meters from the center to the point
    /// `geohash` encodes, a 52-bit geohash as the geo index stores it, or NaN,
    /// which [`reaches`](Self::reaches) rejects, if the center is not finite.
    pub fn distance_to(&self, geohash: f64) -> f64 {
        let (Ok(center_lon), Ok(center_lat)) = (R64::try_new(self.lon), R64::try_new(self.lat))
        else {
            return f64::NAN;
        };
        let (lon, lat) = hash::decode_to_lon_lat(GeoHashBits {
            bits: geohash as u64,
            step: GEO_STEP_MAX,
        });
        hash::haversine_distance(center_lon, center_lat, lon, lat)
    }

    /// Whether a point `distance` meters from the center lies within the
    /// radius, the radius itself included.
    pub const fn reaches(&self, distance: f64) -> bool {
        distance <= self.radius_meters()
    }
}
