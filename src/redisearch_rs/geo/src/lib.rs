/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Geographic utilities for RediSearch.
//!
//! This crate provides geo coordinate parsing, validation, and geohash
//! encoding/decoding. The lower-level geohash primitives live in the
//! [`hash`] module.

mod aggregate;
mod filter;
pub mod hash;
mod legacy;
mod parse;
mod ranges;

pub use aggregate::{DistanceArgument, DistanceResult, GeoValue, geodistance};
pub use filter::{GeoDistance, GeoFilter, GeoFilterError};
pub use legacy::{LegacyGeoArgs, LegacyGeoFilter, LegacyGeoParseError};
pub use parse::{Coordinates, ParseGeoError};
pub use ranges::{GEO_RANGE_COUNT, GeoScoreRange, calc_ranges};
