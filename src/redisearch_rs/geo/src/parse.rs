/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Parsing of geo coordinate strings.

use std::{num::ParseFloatError, str::FromStr};

use decorum::R64;

/// Maximum length of a geo string input (in bytes).
const MAX_GEO_STRING_LEN: usize = 128;

/// Error type for geo string parsing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseGeoError {
    /// The input string exceeds 128 bytes.
    #[error("Geo string cannot be longer than {MAX_GEO_STRING_LEN} bytes")]
    TooLong,
    /// The input string does not contain a comma or space separator.
    #[error("Invalid geo string: missing separator")]
    MissingSeparator,
    /// A coordinate value could not be parsed as a floating-point number.
    #[error("Invalid geo string {input:?}: {source}")]
    Invalid {
        /// The substring that failed to parse.
        input: String,
        /// The underlying parse error.
        source: ParseFloatError,
    },
    /// A coordinate value is not finite (NaN or infinity).
    #[error("Geo coordinates must be finite")]
    NotFinite,
}

/// A longitude/latitude coordinate pair.
///
/// Coordinates are stored as [`R64`] values, which are guaranteed to be real
/// (finite and not NaN). This enforces at the type level that invalid
/// floating-point values like `NaN`, `inf`, and `-inf` cannot be represented.
///
/// Created by parsing a `"lon,lat"` or `"lon lat"` string via [`Coordinates::parse_geo`]:
///
/// ```
/// use geo::Coordinates;
///
/// let coords = Coordinates::parse_geo("29.69465, 34.95126").unwrap();
/// assert_eq!(coords.lon, 29.69465);
/// assert_eq!(coords.lat, 34.95126);
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinates {
    /// Longitude value.
    pub lon: R64,
    /// Latitude value.
    pub lat: R64,
}

impl Coordinates {
    /// Convenience wrapper around [`str::parse`].
    pub fn parse_geo(s: &str) -> Result<Self, ParseGeoError> {
        s.parse()
    }
}

impl FromStr for Coordinates {
    type Err = ParseGeoError;

    /// Parse a string representing a `"lon,lat"` or `"lon lat"` pair.
    ///
    /// The separator can be either a comma (`,`) or a space (` `).
    ///
    /// # Errors
    ///
    /// Returns [`ParseGeoError::TooLong`] if `s` is longer than 128 bytes.
    /// Returns [`ParseGeoError::MissingSeparator`] if the string does not
    /// contain a comma or space.
    /// Returns [`ParseGeoError::Invalid`] if a coordinate cannot be parsed as a
    /// floating-point number.
    /// Returns [`ParseGeoError::NotFinite`] if a parsed coordinate is NaN or
    /// infinity.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() > MAX_GEO_STRING_LEN {
            return Err(ParseGeoError::TooLong);
        }

        let (lon_str, lat_str) = s
            .split_once([',', ' '])
            .ok_or(ParseGeoError::MissingSeparator)?;

        let lon_trimmed = lon_str.trim();
        let lat_trimmed = lat_str.trim();

        let lon: R64 = lon_trimmed
            .parse::<f64>()
            .map_err(|source| ParseGeoError::Invalid {
                input: lon_trimmed.to_owned(),
                source,
            })
            .and_then(|v| R64::try_new(v).map_err(|_| ParseGeoError::NotFinite))?;
        let lat: R64 = lat_trimmed
            .parse::<f64>()
            .map_err(|source| ParseGeoError::Invalid {
                input: lat_trimmed.to_owned(),
                source,
            })
            .and_then(|v| R64::try_new(v).map_err(|_| ParseGeoError::NotFinite))?;

        Ok(Self { lon, lat })
    }
}
