/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;

use crate::hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance};

/// Distance units, with discriminants matching the C geo filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum GeoDistance {
    /// An unrecognized unit.
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
    /// Parse a case-insensitive unit, ignoring bytes after the first NUL.
    pub fn parse(bytes: &[u8]) -> Self {
        let bytes = c_string_prefix(bytes);
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

    /// Parse a bounded unit buffer using the C constructor's scratch-buffer limit.
    ///
    /// Unlike [`Self::parse`], rejects buffers too large for that scratch buffer,
    /// even if a recognized unit precedes an embedded NUL.
    pub fn parse_buffer(bytes: &[u8]) -> Self {
        if bytes.len() < 16 {
            Self::parse(bytes)
        } else {
            Self::Invalid
        }
    }

    /// Return the canonical unit spelling, or the C invalid-unit placeholder.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "<badunit>",
            Self::Kilometers => "km",
            Self::Meters => "m",
            Self::Feet => "ft",
            Self::Miles => "mi",
        }
    }

    /// Return the multiplier for converting a radius to meters.
    ///
    /// # Panics
    ///
    /// Panics for [`Self::Invalid`], which the C implementation also treats as fatal.
    pub const fn meters_factor(self) -> f64 {
        match self {
            Self::Meters => 1.0,
            Self::Kilometers => 1000.0,
            Self::Feet => 0.3048,
            Self::Miles => 1609.34,
            Self::Invalid => panic!("invalid geo distance unit"),
        }
    }
}

fn c_string_prefix(bytes: &[u8]) -> &[u8] {
    &bytes[..bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len())]
}

/// Scalar geo filter parameters, independent of field binding and numeric-filter ownership.
///
/// The C-facing owner remains responsible for its field index and any numeric filters;
/// this value owns no external allocations.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoFilter {
    /// Longitude in degrees.
    pub lon: f64,
    /// Latitude in degrees.
    pub lat: f64,
    /// Search radius in [`Self::unit`].
    pub radius: f64,
    /// Radius unit.
    pub unit: GeoDistance,
}

/// Validation errors in the same precedence order as the C filter validator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GeoFilterError {
    /// The unit was not recognized.
    #[error("Invalid GeoFilter unit")]
    Unit,
    /// A coordinate exceeds the query filter's geographic bounds.
    #[error("Invalid GeoFilter lat/lon")]
    Coordinates,
    /// The radius is non-positive.
    #[error("Invalid GeoFilter radius")]
    Radius,
}

impl GeoFilter {
    /// Construct without validation, defaulting an absent unit to [`GeoDistance::Kilometers`].
    pub fn new(lon: f64, lat: f64, radius: f64, unit: Option<&[u8]>) -> Self {
        Self {
            lon,
            lat,
            radius,
            unit: unit.map_or(GeoDistance::Kilometers, GeoDistance::parse_buffer),
        }
    }

    /// Validate the query bounds, unit, and positive radius.
    ///
    /// NaN coordinates and radii deliberately pass, as in the C validator.
    pub fn validate(&self) -> Result<(), GeoFilterError> {
        if self.unit == GeoDistance::Invalid {
            return Err(GeoFilterError::Unit);
        }
        // Range::contains would reject NaN, unlike the existing C comparisons.
        if self.lat > 90.0 || self.lat < -90.0 || self.lon > 180.0 || self.lon < -180.0 {
            return Err(GeoFilterError::Coordinates);
        }
        if self.radius <= 0.0 {
            return Err(GeoFilterError::Radius);
        }
        Ok(())
    }

    /// Test a geohash score and return both inclusion and distance in meters.
    ///
    /// Decoding uses the same floating-point-to-integer cast as the existing FFI
    /// decoder. Distance is returned even when the point is outside the radius.
    ///
    /// # Panics
    ///
    /// Panics for an invalid unit or non-finite center coordinates, matching the
    /// existing radius-checking path even though [`Self::validate`] accepts NaN.
    pub fn is_within_radius(&self, score: f64) -> (bool, f64) {
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        let radius_meters = self.radius * self.unit.meters_factor();
        let distance = haversine_distance(R64::assert(self.lon), R64::assert(self.lat), lon, lat);
        (distance <= radius_meters, distance)
    }
}

/// Argument access for [`LegacyGeoFilter::parse`].
///
/// The adapter retains its existing numeric conversion: Redis strings use the
/// Redis conversion, while internal C-string/SDS cursors use the C-library conversion.
pub trait LegacyGeoArgs<'a> {
    /// Number of remaining arguments.
    fn remaining(&self) -> usize;
    /// Current argument bytes, excluding the storage's terminal NUL.
    ///
    /// Called only while [`Self::remaining`] is nonzero. Storage must outlive parsing,
    /// including subsequent cursor advancement.
    fn current(&self) -> &'a [u8];
    /// Convert the current argument without advancing, or return no value on failure.
    fn current_double(&self) -> Option<f64>;
    /// Advance past the current argument.
    fn advance(&mut self);
}

/// A legacy filter with an owned, not-yet-bound field name.
///
/// The field name is allocated only after parsing succeeds and is released on drop.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyGeoFilter {
    /// Parsed scalar parameters; validation is a separate operation.
    pub base: GeoFilter,
    /// Field bytes up to the first NUL, matching C's field-name copy.
    pub field: Box<[u8]>,
}

/// Legacy parsing failures, retaining the original error text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LegacyGeoParseError {
    /// Fewer than the required arguments remain; nothing was consumed.
    #[error("GEOFILTER requires 5 arguments")]
    MissingArguments,
    /// Longitude conversion failed; the longitude argument remains current.
    #[error("Bad arguments for <lon>: Could not convert argument to expected type")]
    Longitude,
    /// Latitude conversion failed; the latitude argument remains current.
    #[error("Bad arguments for <lat>: Could not convert argument to expected type")]
    Latitude,
    /// Radius conversion failed; the radius argument remains current.
    #[error("Bad arguments for <radius>: Could not convert argument to expected type")]
    Radius,
    /// The consumed unit was unrecognized. Bytes stop at the first NUL.
    #[error("Unknown distance unit {}", String::from_utf8_lossy(.0))]
    UnknownUnit(Box<[u8]>),
}

impl LegacyGeoFilter {
    /// Parse a field, longitude, latitude, radius, and unit without range validation.
    ///
    /// Empty numeric arguments coalesce to zero and set `has_empty`; an already
    /// set flag is never cleared, including on failure. Dialect-specific rejection
    /// of empty values belongs to the caller, as in the legacy C parser.
    pub fn parse<'a>(
        args: &mut impl LegacyGeoArgs<'a>,
        has_empty: &mut bool,
    ) -> Result<Self, LegacyGeoParseError> {
        if args.remaining() < 5 {
            return Err(LegacyGeoParseError::MissingArguments);
        }
        let field = c_string_prefix(args.current());
        args.advance();
        let mut values = [0.0; 3];
        for (value, error) in values.iter_mut().zip([
            LegacyGeoParseError::Longitude,
            LegacyGeoParseError::Latitude,
            LegacyGeoParseError::Radius,
        ]) {
            *value = match args.current_double() {
                Some(value) => value,
                None if args.current().is_empty() => {
                    *has_empty = true;
                    0.0
                }
                None => return Err(error),
            };
            args.advance();
        }
        let unit = GeoDistance::parse(args.current());
        let error = (unit == GeoDistance::Invalid)
            .then(|| LegacyGeoParseError::UnknownUnit(c_string_prefix(args.current()).into()));
        args.advance();
        if let Some(error) = error {
            return Err(error);
        }
        Ok(Self {
            base: GeoFilter {
                lon: values[0],
                lat: values[1],
                radius: values[2],
                unit,
            },
            field: field.into(),
        })
    }
}
