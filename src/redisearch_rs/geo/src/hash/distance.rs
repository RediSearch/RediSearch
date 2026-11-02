/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Haversine great-circle distance calculation.

use decorum::R64;

use super::EARTH_RADIUS_IN_METERS;

/// Calculate the great-circle distance between two WGS-84 points using the
/// haversine formula.
///
/// All coordinates are in degrees. Returns distance in meters.
pub fn haversine_distance(lon1: R64, lat1: R64, lon2: R64, lat2: R64) -> f64 {
    let lat1r = lat1.into_inner().to_radians();
    let lon1r = lon1.into_inner().to_radians();
    let lat2r = lat2.into_inner().to_radians();
    let lon2r = lon2.into_inner().to_radians();

    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2r - lon1r) / 2.0).sin();

    // Clamp before asin: floating-point rounding can push the sqrt result
    // slightly above 1.0 for near-antipodal points, which would yield NaN.
    2.0 * EARTH_RADIUS_IN_METERS
        * (u * u + lat1r.cos() * lat2r.cos() * v * v)
            .sqrt()
            .min(1.0)
            .asin()
}

/// Great-circle distance along a meridian (constant longitude).
///
/// This is a specialization of [`haversine_distance`] where `lon1 == lon2`,
/// eliminating the longitude sine/cosine terms.
pub(crate) fn meridian_distance(lat1: R64, lat2: R64) -> f64 {
    let u = ((lat2.into_inner().to_radians() - lat1.into_inner().to_radians()) / 2.0).sin();
    2.0 * EARTH_RADIUS_IN_METERS * u.abs().min(1.0).asin()
}

/// Great-circle distance along a parallel (constant latitude).
///
/// This is a specialization of [`haversine_distance`] where `lat1 == lat2`,
/// eliminating the latitude-difference term and needing only one cosine.
pub(crate) fn parallel_distance(lat: R64, lon1: R64, lon2: R64) -> f64 {
    let lat_r = lat.into_inner().to_radians();
    let v = ((lon2.into_inner().to_radians() - lon1.into_inner().to_radians()) / 2.0).sin();
    2.0 * EARTH_RADIUS_IN_METERS * (lat_r.cos() * v.abs()).min(1.0).asin()
}
