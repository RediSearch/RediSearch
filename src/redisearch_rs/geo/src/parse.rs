/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Parsing of geo coordinate strings.

/// Maximum length of a geo string input (in bytes).
const MAX_GEO_STRING_LEN: usize = 128;

/// Error type for geo string parsing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseGeoError {
    /// The input string exceeds 128 bytes.
    #[error("Geo string cannot be longer than {MAX_GEO_STRING_LEN} bytes")]
    TooLong,
    /// The input string is not a valid `"lon,lat"` or `"lon lat"` pair.
    #[error("Invalid geo string")]
    Invalid,
}

/// Parse a string representing a `"lon,lat"` or `"lon lat"` pair into two
/// `f64` values.
///
/// The separator can be either a comma (`,`) or a space (` `).
///
/// Returns `(longitude, latitude)` on success.
///
/// # Errors
///
/// Returns [`ParseGeoError::TooLong`] if `s` is longer than 128 bytes.
/// Returns [`ParseGeoError::Invalid`] if the string cannot be parsed as two
/// floating-point numbers separated by a comma or space.
pub fn parse_geo(s: &str) -> Result<(f64, f64), ParseGeoError> {
    if s.len() > MAX_GEO_STRING_LEN {
        return Err(ParseGeoError::TooLong);
    }

    let (lon_str, lat_str) = s.split_once([',', ' ']).ok_or(ParseGeoError::Invalid)?;

    let lon: f64 = lon_str.trim().parse().map_err(|_| ParseGeoError::Invalid)?;
    let lat: f64 = lat_str.trim().parse().map_err(|_| ParseGeoError::Invalid)?;

    // Reject non-finite values (NaN, inf, -inf) that Rust's f64 parser
    // accepts but the original fast_float_strtod did not.
    if !lon.is_finite() || !lat.is_finite() {
        return Err(ParseGeoError::Invalid);
    }

    Ok((lon, lat))
}
