/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;

use crate::{Coordinates, ParseGeoError, filter::decode_score, hash::haversine_distance};

/// A dereferenced geo field value for aggregate distance evaluation.
#[derive(Debug, Clone, Copy)]
pub enum GeoValue<'a> {
    /// Coordinate-pair string.
    String(&'a [u8]),
    /// Numeric geohash score when used as a field; coordinate when used as a number.
    Number(f64),
    /// A value that is neither string nor number.
    Other,
}

/// The two distinct views required by aggregate distance evaluation.
///
/// An RSValue adapter must follow references for [`Self::field`], but references
/// and trios for [`Self::to_number`]. Keeping these separate preserves three-argument
/// dispatch even when the first argument's numeric and field views differ.
pub trait DistanceArgument {
    /// Dereferenced field value.
    fn field(&self) -> GeoValue<'_>;

    /// Numeric conversion, or no numeric value.
    fn to_number(&self) -> Option<f64>;
}

impl DistanceArgument for GeoValue<'_> {
    fn field(&self) -> GeoValue<'_> {
        *self
    }

    fn to_number(&self) -> Option<f64> {
        match self {
            Self::Number(number) => Some(*number),
            // Same grammar as RSValue's Rust-backed string-to-number conversion.
            Self::String(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
            Self::Other => None,
        }
    }
}

/// Aggregate distance result; evaluation succeeds even when the distance is NaN.
#[derive(Debug)]
pub struct DistanceResult {
    /// Distance in meters, rounded to two decimal places, or NaN on failure.
    pub distance: f64,
    /// Only string-field parse failures set a query error.
    pub error: Option<ParseGeoError>,
}

/// Evaluate the aggregate distance function with two, three, or four arguments.
///
/// Two arguments are fields; four are longitude/latitude pairs. With three, the
/// first argument's [`DistanceArgument::to_number`] selects whether the first two
/// arguments are a coordinate pair or the first argument is a field.
///
/// # Panics
///
/// Panics for arities outside the range enforced by the C function registry.
pub fn geodistance(args: &[impl DistanceArgument]) -> DistanceResult {
    assert!(
        (2..=4).contains(&args.len()),
        "geodistance requires 2 to 4 arguments"
    );
    let points = match args {
        [first, second] => parse_field(first).and_then(|a| parse_field(second).map(|b| (a, b))),
        [lon1, lat1, lon2, lat2] => {
            parse_lon_lat(lon1, lat1).and_then(|a| parse_lon_lat(lon2, lat2).map(|b| (a, b)))
        }
        [first, second, third] => {
            if first.to_number().is_some() {
                parse_lon_lat(first, second).and_then(|a| parse_field(third).map(|b| (a, b)))
            } else {
                parse_field(first).and_then(|a| parse_lon_lat(second, third).map(|b| (a, b)))
            }
        }
        _ => unreachable!(),
    };
    let distance = match points {
        Ok(([lon1, lat1], [lon2, lat2])) => {
            match (
                R64::try_new(lon1),
                R64::try_new(lat1),
                R64::try_new(lon2),
                R64::try_new(lat2),
            ) {
                (Ok(lon1), Ok(lat1), Ok(lon2), Ok(lat2)) => {
                    (haversine_distance(lon1, lat1, lon2, lat2) * 100.0).round() / 100.0
                }
                _ => f64::NAN,
            }
        }
        Err(error) => {
            return DistanceResult {
                distance: f64::NAN,
                error,
            };
        }
    };
    DistanceResult {
        distance,
        error: None,
    }
}

fn parse_field(arg: &impl DistanceArgument) -> Result<[f64; 2], Option<ParseGeoError>> {
    match arg.field() {
        GeoValue::String(bytes) => {
            let text = std::str::from_utf8(bytes).map_err(|_| Some(ParseGeoError::InvalidUtf8))?;
            let coords = Coordinates::parse_geo(text).map_err(Some)?;
            Ok([coords.lon.into_inner(), coords.lat.into_inner()])
        }
        GeoValue::Number(score) => {
            let (lon, lat) = decode_score(score);
            Ok([lon.into_inner(), lat.into_inner()])
        }
        GeoValue::Other => Err(None),
    }
}

fn parse_lon_lat(
    lon: &impl DistanceArgument,
    lat: &impl DistanceArgument,
) -> Result<[f64; 2], Option<ParseGeoError>> {
    Ok([lon.to_number().ok_or(None)?, lat.to_number().ok_or(None)?])
}
