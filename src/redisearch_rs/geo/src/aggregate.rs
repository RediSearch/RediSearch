/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;

use crate::{
    Coordinates, ParseGeoError,
    hash::{GEO_STEP_MAX, GeoHashBits, decode_to_lon_lat, haversine_distance},
};

/// Value access for [`geodistance`], keeping representation and numeric coercion distinct.
///
/// Field access follows references but not trios; numeric conversion follows both.
/// This distinction preserves the aggregate function's overload selection.
pub trait GeoDistanceValue {
    /// String bytes after following references, if this is a string value.
    fn geo_string(&self) -> Option<&[u8]>;
    /// Number after following references, if this is a numeric value (a geohash score).
    fn geo_number(&self) -> Option<f64>;
    /// Existing numeric coercion, including numeric strings and trios.
    fn to_number(&self) -> Option<f64>;
}

/// Aggregate conversion failures.
///
/// The C-facing evaluator returns a numeric NaN for either variant, but sets the
/// query error only for [`Self::Parse`]. Expression evaluation itself still succeeds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeoDistanceError {
    /// A value is neither a coordinate field nor convertible to a required number.
    #[error("Invalid geodistance argument")]
    Argument,
    /// A string coordinate field failed to parse.
    #[error(transparent)]
    Parse(#[from] ParseGeoError),
}

fn parse_field(value: &impl GeoDistanceValue) -> Result<[f64; 2], GeoDistanceError> {
    if let Some(bytes) = value.geo_string() {
        let string = std::str::from_utf8(bytes).map_err(|_| ParseGeoError::InvalidUtf8)?;
        let coordinates = Coordinates::parse_geo(string)?;
        Ok([coordinates.lon.into_inner(), coordinates.lat.into_inner()])
    } else if let Some(score) = value.geo_number() {
        let (lon, lat) = decode_to_lon_lat(GeoHashBits {
            bits: score as u64,
            step: GEO_STEP_MAX,
        });
        Ok([lon.into_inner(), lat.into_inner()])
    } else {
        Err(GeoDistanceError::Argument)
    }
}

fn parse_lon_lat(values: &[impl GeoDistanceValue]) -> Result<[f64; 2], GeoDistanceError> {
    Ok([
        values[0].to_number().ok_or(GeoDistanceError::Argument)?,
        values[1].to_number().ok_or(GeoDistanceError::Argument)?,
    ])
}

/// Evaluate the aggregate distance overloads and round the result to centimeters.
///
/// Two arguments are coordinate fields (strings or numeric geohash scores); four
/// are numeric longitude/latitude pairs. With three, numeric coercion of the first
/// argument selects whether the first or last argument pair supplies raw coordinates.
/// Raw numeric coordinates are not range-checked. Non-finite coordinates produce NaN.
///
/// # Errors
///
/// Returns [`GeoDistanceError`] on conversion failure, preserving whether the
/// caller must also set a coordinate parsing error.
///
/// # Panics
///
/// Panics outside the argument-count range enforced by the aggregate registry.
pub fn geodistance(values: &[impl GeoDistanceValue]) -> Result<f64, GeoDistanceError> {
    let [first, second] = match values.len() {
        2 => [parse_field(&values[0])?, parse_field(&values[1])?],
        4 => [parse_lon_lat(&values[..2])?, parse_lon_lat(&values[2..])?],
        3 if values[0].to_number().is_some() => {
            [parse_lon_lat(&values[..2])?, parse_field(&values[2])?]
        }
        3 => [parse_field(&values[0])?, parse_lon_lat(&values[1..])?],
        _ => panic!("geodistance requires two to four arguments"),
    };
    let [Ok(lon1), Ok(lat1), Ok(lon2), Ok(lat2)] =
        [first[0], first[1], second[0], second[1]].map(R64::try_new)
    else {
        return Ok(f64::NAN);
    };
    let distance = haversine_distance(lon1, lat1, lon2, lat2);
    Ok((distance * 100.0).round() / 100.0)
}
