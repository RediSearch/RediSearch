/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Redis-compatible geohash encoding/decoding.
//!
//! This is a port of the C geohash library originally by yinqiwen, Matt
//! Stancliff, and Salvatore Sanfilippo (used in Redis and RediSearch).
//!
//! Coordinates are encoded as 52-bit interleaved hashes (Morton codes) using
//! WGS-84 bounds, suitable for storage as sorted-set scores in Redis.

mod bits;
mod distance;
mod types;

pub use distance::haversine_distance;
pub use types::{GeoHashArea, GeoHashBits, GeoHashNeighbors, GeoHashRadius, GeoHashRange};

/// Maximum geohash step count. 26 steps × 2 bits = 52 bits of precision.
pub const GEO_STEP_MAX: u8 = 26;

/// WGS-84 latitude limits (EPSG:900913).
pub const GEO_LAT_MIN: f64 = -85.05112878;
/// WGS-84 latitude limits (EPSG:900913).
pub const GEO_LAT_MAX: f64 = 85.05112878;
/// WGS-84 longitude limits.
pub const GEO_LONG_MIN: f64 = -180.0;
/// WGS-84 longitude limits.
pub const GEO_LONG_MAX: f64 = 180.0;

/// Mercator projection maximum (meters).
const MERCATOR_MAX: f64 = 20037726.37;

/// Earth's quadratic mean radius for WGS-84 (meters).
const EARTH_RADIUS_IN_METERS: f64 = 6372797.560856;

/// Error returned by [`encode_wgs84`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// The step value is outside the valid range `1..=32`.
    #[error("step must be in 1..=32")]
    InvalidStep,
    /// The longitude or latitude is outside the WGS-84 bounds.
    #[error("coordinates outside WGS-84 bounds")]
    OutOfRange,
}

/// Encode WGS-84 longitude/latitude into a [`GeoHashBits`] with the given
/// step precision.
///
/// # Errors
///
/// Returns [`EncodeError::InvalidStep`] if `step` is 0 or greater than 32.
/// Returns [`EncodeError::OutOfRange`] if `longitude` or `latitude` is
/// outside the WGS-84 bounds.
pub fn encode_wgs84(longitude: f64, latitude: f64, step: u8) -> Result<GeoHashBits, EncodeError> {
    if step == 0 || step > 32 {
        return Err(EncodeError::InvalidStep);
    }
    if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&longitude)
        || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&latitude)
    {
        return Err(EncodeError::OutOfRange);
    }

    let lon_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

    let lat_fixed = (lat_offset * (1u64 << step) as f64) as u32;
    let lon_fixed = (lon_offset * (1u64 << step) as f64) as u32;

    Ok(GeoHashBits {
        bits: bits::interleave64(lat_fixed, lon_fixed),
        step,
    })
}

/// Error returned by [`decode`] and [`decode_to_lon_lat`] when the hash is
/// zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("cannot decode a zero geohash")]
pub struct DecodeError;

/// Decode a [`GeoHashBits`] back into the bounding [`GeoHashArea`].
///
/// # Errors
///
/// Returns [`DecodeError`] if the hash is zero.
pub fn decode(hash: GeoHashBits) -> Result<GeoHashArea, DecodeError> {
    if hash.is_zero() {
        return Err(DecodeError);
    }

    let step = hash.step;
    let (ilat, ilon) = bits::deinterleave64(hash.bits);

    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let lon_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    let step_size = (1u64 << step) as f64;

    Ok(GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: GEO_LAT_MIN + (ilat as f64 / step_size) * lat_scale,
            max: GEO_LAT_MIN + ((ilat as f64 + 1.0) / step_size) * lat_scale,
        },
        longitude: GeoHashRange {
            min: GEO_LONG_MIN + (ilon as f64 / step_size) * lon_scale,
            max: GEO_LONG_MIN + ((ilon as f64 + 1.0) / step_size) * lon_scale,
        },
    })
}

/// Decode a [`GeoHashBits`] to the center `(longitude, latitude)` point.
///
/// # Errors
///
/// Returns [`DecodeError`] if the hash is zero.
pub fn decode_to_lon_lat(hash: GeoHashBits) -> Result<(f64, f64), DecodeError> {
    let area = decode(hash)?;
    let lon = ((area.longitude.min + area.longitude.max) / 2.0).clamp(GEO_LONG_MIN, GEO_LONG_MAX);
    let lat = ((area.latitude.min + area.latitude.max) / 2.0).clamp(GEO_LAT_MIN, GEO_LAT_MAX);
    Ok((lon, lat))
}

/// Left-shift a geohash to fill 52 bits, producing a value suitable for use
/// as a Redis sorted-set score.
pub const fn align_52bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step as u32 * 2)
}

/// Compute the 8 neighboring geohash cells.
pub const fn neighbors(hash: GeoHashBits) -> GeoHashNeighbors {
    let mut east = hash;
    let mut west = hash;
    let mut north = hash;
    let mut south = hash;
    let mut north_east = hash;
    let mut north_west = hash;
    let mut south_east = hash;
    let mut south_west = hash;

    bits::move_x(&mut east, 1);
    bits::move_x(&mut west, -1);
    bits::move_y(&mut north, 1);
    bits::move_y(&mut south, -1);

    bits::move_x(&mut north_east, 1);
    bits::move_y(&mut north_east, 1);

    bits::move_x(&mut north_west, -1);
    bits::move_y(&mut north_west, 1);

    bits::move_x(&mut south_east, 1);
    bits::move_y(&mut south_east, -1);

    bits::move_x(&mut south_west, -1);
    bits::move_y(&mut south_west, -1);

    GeoHashNeighbors {
        north,
        south,
        east,
        west,
        north_east,
        north_west,
        south_east,
        south_west,
    }
}

/// Estimate the precision step for a radius query at the given latitude.
fn estimate_steps_by_radius(range_meters: f64, lat: f64) -> u8 {
    if range_meters == 0.0 {
        // Zero radius means exact point lookup — use finest precision.
        // Also avoids an infinite loop since 0.0 * 2.0 stays 0.0.
        return GEO_STEP_MAX;
    }
    let mut range = range_meters;
    let mut step: i32 = 1;
    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2; // Ensure the range is included in most base cases.

    // Wider range towards the poles.
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }

    step.clamp(1, GEO_STEP_MAX as i32) as u8
}

/// Compute the bounding box `[min_lon, min_lat, max_lon, max_lat]` for a
/// circle centered at `(longitude, latitude)` with the given radius in meters.
fn bounding_box(longitude: f64, latitude: f64, radius_meters: f64) -> [f64; 4] {
    let lat_rad = latitude.to_radians();
    let lon_delta = (radius_meters / EARTH_RADIUS_IN_METERS / lat_rad.cos()).to_degrees();
    let lat_delta = (radius_meters / EARTH_RADIUS_IN_METERS).to_degrees();
    [
        longitude - lon_delta,
        latitude - lat_delta,
        longitude + lon_delta,
        latitude + lat_delta,
    ]
}

/// Return the center hash, bounding area, and 8 neighbors that cover a
/// radius query around `(longitude, latitude)` with `radius_meters`.
///
/// Neighbors that fall outside the bounding box are zeroed out.
pub fn get_areas_by_radius(longitude: f64, latitude: f64, radius_meters: f64) -> GeoHashRadius {
    let bounds = bounding_box(longitude, latitude, radius_meters);
    let [min_lon, min_lat, max_lon, max_lat] = bounds;

    let mut steps = estimate_steps_by_radius(radius_meters, latitude);

    // On encode failure, preserve the step so that neighbors()/move_x()/
    // move_y() don't shift a u64 by 64 (which panics in debug mode).
    let fallback = GeoHashBits {
        bits: 0,
        step: steps,
    };

    let mut hash = encode_wgs84(longitude, latitude, steps).unwrap_or(fallback);
    let mut nbrs = neighbors(hash);
    let mut area = decode(hash).unwrap_or_default();

    // Check if the step is sufficient at the limits of the covered area.
    // Sometimes a neighboring cell is too near to cover everything.
    let mut decrease_step = false;
    if let (Ok(north), Ok(south), Ok(east), Ok(west)) = (
        decode(nbrs.north),
        decode(nbrs.south),
        decode(nbrs.east),
        decode(nbrs.west),
    ) {
        if haversine_distance(longitude, latitude, longitude, north.latitude.max) < radius_meters {
            decrease_step = true;
        }
        if haversine_distance(longitude, latitude, longitude, south.latitude.min) < radius_meters {
            decrease_step = true;
        }
        if haversine_distance(longitude, latitude, east.longitude.max, latitude) < radius_meters {
            decrease_step = true;
        }
        if haversine_distance(longitude, latitude, west.longitude.min, latitude) < radius_meters {
            decrease_step = true;
        }
    }

    if steps > 1 && decrease_step {
        steps -= 1;
        let fallback = GeoHashBits {
            bits: 0,
            step: steps,
        };
        hash = encode_wgs84(longitude, latitude, steps).unwrap_or(fallback);
        nbrs = neighbors(hash);
        area = decode(hash).unwrap_or_default();
    }

    // Exclude neighbor cells that are outside the bounding box.
    if steps >= 2 {
        if area.latitude.min < min_lat {
            nbrs.south = GeoHashBits::ZERO;
            nbrs.south_west = GeoHashBits::ZERO;
            nbrs.south_east = GeoHashBits::ZERO;
        }
        if area.latitude.max > max_lat {
            nbrs.north = GeoHashBits::ZERO;
            nbrs.north_east = GeoHashBits::ZERO;
            nbrs.north_west = GeoHashBits::ZERO;
        }
        if area.longitude.min < min_lon {
            nbrs.west = GeoHashBits::ZERO;
            nbrs.south_west = GeoHashBits::ZERO;
            nbrs.north_west = GeoHashBits::ZERO;
        }
        if area.longitude.max > max_lon {
            nbrs.east = GeoHashBits::ZERO;
            nbrs.south_east = GeoHashBits::ZERO;
            nbrs.north_east = GeoHashBits::ZERO;
        }
    }

    GeoHashRadius {
        hash,
        area,
        neighbors: nbrs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_zero_hash_returns_err() {
        assert_eq!(decode(GeoHashBits::ZERO), Err(DecodeError));
    }

    #[test]
    fn decode_to_lon_lat_zero_hash_returns_err() {
        assert_eq!(decode_to_lon_lat(GeoHashBits::ZERO), Err(DecodeError));
    }

    #[test]
    fn estimate_steps_zero_radius_returns_max() {
        assert_eq!(estimate_steps_by_radius(0.0, 0.0), GEO_STEP_MAX);
    }

    #[test]
    fn estimate_steps_small_radius_gives_high_step() {
        let step = estimate_steps_by_radius(100.0, 45.0);
        // 100m radius should yield a high step count (fine precision).
        assert!(
            step >= 15,
            "expected step >= 15 for 100m radius, got {step}"
        );
    }

    #[test]
    fn estimate_steps_large_radius_gives_low_step() {
        let step = estimate_steps_by_radius(5_000_000.0, 0.0);
        // 5000 km radius should yield a very low step count.
        assert!(
            step <= 5,
            "expected step <= 5 for 5000km radius, got {step}"
        );
    }

    #[test]
    fn estimate_steps_polar_latitude_decrements() {
        let step_equator = estimate_steps_by_radius(1000.0, 0.0);
        let step_polar = estimate_steps_by_radius(1000.0, 70.0);
        let step_extreme_polar = estimate_steps_by_radius(1000.0, 85.0);

        assert!(
            step_polar < step_equator,
            "polar ({step_polar}) should be less than equator ({step_equator})"
        );
        assert!(
            step_extreme_polar < step_polar,
            "extreme polar ({step_extreme_polar}) should be less than polar ({step_polar})"
        );
    }

    #[test]
    fn estimate_steps_negative_polar_latitude_decrements() {
        let step_equator = estimate_steps_by_radius(1000.0, 0.0);
        let step_polar = estimate_steps_by_radius(1000.0, -70.0);

        assert!(
            step_polar < step_equator,
            "negative polar ({step_polar}) should be less than equator ({step_equator})"
        );
    }

    #[test]
    fn estimate_steps_clamps_to_valid_range() {
        // Extremely large radius should clamp to 1.
        let step = estimate_steps_by_radius(f64::MAX, 85.0);
        assert_eq!(step, 1);
    }

    #[test]
    fn bounding_box_symmetric_around_center() {
        let [min_lon, min_lat, max_lon, max_lat] = bounding_box(10.0, 45.0, 1000.0);

        let lon_delta_west = 10.0 - min_lon;
        let lon_delta_east = max_lon - 10.0;
        assert!(
            (lon_delta_west - lon_delta_east).abs() < 1e-10,
            "longitude deltas should be symmetric"
        );

        let lat_delta_south = 45.0 - min_lat;
        let lat_delta_north = max_lat - 45.0;
        assert!(
            (lat_delta_south - lat_delta_north).abs() < 1e-10,
            "latitude deltas should be symmetric"
        );
    }

    #[test]
    fn bounding_box_larger_radius_gives_larger_box() {
        let small = bounding_box(0.0, 45.0, 100.0);
        let large = bounding_box(0.0, 45.0, 10_000.0);

        assert!(
            large[0] < small[0],
            "larger radius should have smaller min_lon"
        );
        assert!(
            large[1] < small[1],
            "larger radius should have smaller min_lat"
        );
        assert!(
            large[2] > small[2],
            "larger radius should have larger max_lon"
        );
        assert!(
            large[3] > small[3],
            "larger radius should have larger max_lat"
        );
    }

    #[test]
    fn bounding_box_wider_at_high_latitude() {
        let equator = bounding_box(0.0, 0.0, 1000.0);
        let high_lat = bounding_box(0.0, 60.0, 1000.0);

        let equator_lon_span = equator[2] - equator[0];
        let high_lat_lon_span = high_lat[2] - high_lat[0];

        // At higher latitudes, the longitude span should be wider because
        // meridians converge towards the poles.
        assert!(
            high_lat_lon_span > equator_lon_span,
            "lon span at 60° ({high_lat_lon_span}) should be wider than at equator ({equator_lon_span})"
        );
    }
}
