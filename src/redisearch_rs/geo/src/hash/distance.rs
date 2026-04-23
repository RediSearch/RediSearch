/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Haversine great-circle distance calculation.

use super::EARTH_RADIUS_IN_METERS;

/// Calculate the great-circle distance between two WGS-84 points using the
/// haversine formula.
///
/// All coordinates are in degrees. Returns distance in meters.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1r = lat1.to_radians();
    let lon1r = lon1.to_radians();
    let lat2r = lat2.to_radians();
    let lon2r = lon2.to_radians();

    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2r - lon1r) / 2.0).sin();

    2.0 * EARTH_RADIUS_IN_METERS * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}
