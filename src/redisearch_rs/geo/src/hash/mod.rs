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
//! This is a port of the [C geohash library](https://github.com/yinqiwen/ardb/blob/d42503/src/geo/geohash_helper.cpp)
//! originally by yinqiwen, Matt Stancliff, and Salvatore Sanfilippo (used in Redis and RediSearch).
//!
//! Coordinates are encoded as 52-bit interleaved hashes (Morton codes) using
//! WGS-84 bounds, suitable for storage as sorted-set scores in Redis.

use decorum::R64;

mod bits;
mod distance;
mod types;

pub use distance::haversine_distance;
pub use types::{
    GeoHashArea, GeoHashBits, GeoHashNeighbors, GeoHashRadius, GeoHashRange, InvalidPrecisionStep,
    InvalidWGS84Coordinates, PrecisionStep, WGS84Coordinates,
};

/// Maximum geohash step count. 26 steps × 2 bits = 52 bits of precision.
pub const GEO_STEP_MAX: PrecisionStep = match PrecisionStep::new(26) {
    Ok(s) => s,
    Err(_) => unreachable!(),
};

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

/// Encode WGS-84 coordinates into a [`GeoHashBits`] with the given step
/// precision.
pub fn encode_wgs84(coords: WGS84Coordinates, step: PrecisionStep) -> GeoHashBits {
    let lon = coords.longitude().into_inner();
    let lat = coords.latitude().into_inner();

    let step_val = step.as_u8();
    let lon_offset = (lon - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat_offset = (lat - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

    // Clamp to the maximum valid fixed-point value. At exact boundary
    // coordinates (e.g. lon=180, lat=85.05112878) the offset is exactly 1.0,
    // producing `1 << step` which overflows the `step*2`-bit hash range.
    let max_fixed = (1u64 << step_val) - 1;
    let lat_fixed = ((lat_offset * (1u64 << step_val) as f64) as u64).min(max_fixed) as u32;
    let lon_fixed = ((lon_offset * (1u64 << step_val) as f64) as u64).min(max_fixed) as u32;

    GeoHashBits {
        bits: bits::interleave64(lat_fixed, lon_fixed),
        step,
    }
}

/// Decode a [`GeoHashBits`] back into the bounding [`GeoHashArea`].
pub fn decode(hash: GeoHashBits) -> GeoHashArea {
    let step = hash.step.as_u8();
    let (ilat, ilon) = bits::deinterleave64(hash.bits);

    let lat_scale = GEO_LAT_MAX - GEO_LAT_MIN;
    let lon_scale = GEO_LONG_MAX - GEO_LONG_MIN;
    let step_size = (1u64 << step) as f64;

    // All values below are finite: they are bounded arithmetic on integer
    // inputs clamped to `step` bits, so `R64::assert` cannot panic.
    GeoHashArea {
        hash,
        latitude: GeoHashRange {
            min: R64::assert(GEO_LAT_MIN + (ilat as f64 / step_size) * lat_scale),
            max: R64::assert(GEO_LAT_MIN + ((ilat as f64 + 1.0) / step_size) * lat_scale),
        },
        longitude: GeoHashRange {
            min: R64::assert(GEO_LONG_MIN + (ilon as f64 / step_size) * lon_scale),
            max: R64::assert(GEO_LONG_MIN + ((ilon as f64 + 1.0) / step_size) * lon_scale),
        },
    }
}

/// Decode a [`GeoHashBits`] to the center `(longitude, latitude)` point.
pub fn decode_to_lon_lat(hash: GeoHashBits) -> (R64, R64) {
    let area = decode(hash);
    let lon = ((area.longitude.min + area.longitude.max) / 2.0)
        .clamp(R64::assert(GEO_LONG_MIN), R64::assert(GEO_LONG_MAX));
    let lat = ((area.latitude.min + area.latitude.max) / 2.0)
        .clamp(R64::assert(GEO_LAT_MIN), R64::assert(GEO_LAT_MAX));
    (lon, lat)
}

/// Left-shift a geohash to fill 52 bits, producing a value suitable for use
/// as a Redis sorted-set score.
pub const fn align_52bits(hash: GeoHashBits) -> u64 {
    hash.bits << (52 - hash.step.as_u8() as u32 * 2)
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
        north: Some(north),
        south: Some(south),
        east: Some(east),
        west: Some(west),
        north_east: Some(north_east),
        north_west: Some(north_west),
        south_east: Some(south_east),
        south_west: Some(south_west),
    }
}

/// Estimate the precision step for a radius query at the given latitude.
fn estimate_steps_by_radius(range_meters: f64, lat: R64) -> PrecisionStep {
    if range_meters.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
        // Zero, negative, or NaN radius — use finest precision.
        // This also avoids an infinite loop: doubling a non-positive value
        // never reaches MERCATOR_MAX.
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
    let lat = lat.into_inner();
    if !(-66.0..=66.0).contains(&lat) {
        step -= 1;
        if !(-80.0..=80.0).contains(&lat) {
            step -= 1;
        }
    }

    // Clamped to 1..=GEO_STEP_MAX, which is always valid for PrecisionStep.
    PrecisionStep::new(step.clamp(1, GEO_STEP_MAX.as_u8() as i32) as u8).unwrap()
}

/// Compute the bounding box `[min_lon, min_lat, max_lon, max_lat]` for a
/// circle centered at `(longitude, latitude)` with the given radius in meters.
///
/// **Known limitation:** the planar approximation does not behave correctly
/// at very high latitudes with large radii. For example, at coordinates
/// (81.63, 30.56) with a 7 083 km radius the reported `min_lon` is too
/// large, missing points that are actually within range. Because the
/// bounding box is only used as an optimistic filter (candidates are
/// verified with [`haversine_distance`]), the result is over-pruning of
/// neighbor cells, not incorrect final results.
fn bounding_box(coords: WGS84Coordinates, radius_meters: f64) -> [f64; 4] {
    let lon = coords.longitude().into_inner();
    let lat = coords.latitude().into_inner();
    let lat_rad = lat.to_radians();
    let lon_delta = (radius_meters / EARTH_RADIUS_IN_METERS / lat_rad.cos()).to_degrees();
    let lat_delta = (radius_meters / EARTH_RADIUS_IN_METERS).to_degrees();
    [
        lon - lon_delta,
        lat - lat_delta,
        lon + lon_delta,
        lat + lat_delta,
    ]
}

/// Return the center hash, bounding area, and 8 neighbors that cover a
/// radius query around the given coordinates with `radius_meters`.
///
/// Neighbors that fall outside the bounding box are set to [`None`].
pub fn get_areas_by_radius(coords: WGS84Coordinates, radius_meters: f64) -> GeoHashRadius {
    let bounds = bounding_box(coords, radius_meters);
    let [min_lon, min_lat, max_lon, max_lat] = bounds;

    let longitude = coords.longitude();
    let latitude = coords.latitude();

    let mut steps = estimate_steps_by_radius(radius_meters, latitude);

    let mut hash = encode_wgs84(coords, steps);
    let mut nbrs = neighbors(hash);
    let mut area = decode(hash);

    // Check if the step is sufficient at the limits of the covered area.
    // Sometimes a neighboring cell is too near to cover everything.
    let mut decrease_step = false;
    if let (Some(n), Some(s), Some(e), Some(w)) = (nbrs.north, nbrs.south, nbrs.east, nbrs.west) {
        let north = decode(n);
        let south = decode(s);
        let east = decode(e);
        let west = decode(w);
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

    if steps.as_u8() > 1 && decrease_step {
        // steps is at least 2 here, so steps - 1 is at least 1 — always valid.
        steps = PrecisionStep::new(steps.as_u8() - 1).unwrap();
        hash = encode_wgs84(coords, steps);
        nbrs = neighbors(hash);
        area = decode(hash);
    }

    // Exclude neighbor cells that are outside the bounding box.
    if steps.as_u8() >= 2 {
        if area.latitude.min < min_lat {
            nbrs.south = None;
            nbrs.south_west = None;
            nbrs.south_east = None;
        }
        if area.latitude.max > max_lat {
            nbrs.north = None;
            nbrs.north_east = None;
            nbrs.north_west = None;
        }
        if area.longitude.min < min_lon {
            nbrs.west = None;
            nbrs.south_west = None;
            nbrs.north_west = None;
        }
        if area.longitude.max > max_lon {
            nbrs.east = None;
            nbrs.south_east = None;
            nbrs.north_east = None;
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
    fn estimate_steps_zero_radius_returns_max() {
        assert_eq!(
            estimate_steps_by_radius(0.0, R64::assert(0.0)),
            GEO_STEP_MAX
        );
    }

    #[test]
    fn estimate_steps_negative_radius_returns_max() {
        assert_eq!(
            estimate_steps_by_radius(-1.0, R64::assert(0.0)),
            GEO_STEP_MAX
        );
        assert_eq!(
            estimate_steps_by_radius(-1000.0, R64::assert(45.0)),
            GEO_STEP_MAX
        );
        assert_eq!(
            estimate_steps_by_radius(f64::NEG_INFINITY, R64::assert(0.0)),
            GEO_STEP_MAX
        );
    }

    #[test]
    fn estimate_steps_positive_infinity_returns_one() {
        assert_eq!(
            estimate_steps_by_radius(f64::INFINITY, R64::assert(0.0)).as_u8(),
            1
        );
    }

    #[test]
    fn estimate_steps_nan_radius_returns_max() {
        assert_eq!(
            estimate_steps_by_radius(f64::NAN, R64::assert(0.0)),
            GEO_STEP_MAX
        );
    }

    #[test]
    fn estimate_steps_small_radius_gives_high_step() {
        let step = estimate_steps_by_radius(100.0, R64::assert(45.0)).as_u8();
        // 100m radius should yield a high step count (fine precision).
        assert!(
            step >= 15,
            "expected step >= 15 for 100m radius, got {step}"
        );
    }

    #[test]
    fn estimate_steps_large_radius_gives_low_step() {
        let step = estimate_steps_by_radius(5_000_000.0, R64::assert(0.0)).as_u8();
        // 5000 km radius should yield a very low step count.
        assert!(
            step <= 5,
            "expected step <= 5 for 5000km radius, got {step}"
        );
    }

    #[test]
    fn estimate_steps_polar_latitude_decrements() {
        let step_equator = estimate_steps_by_radius(1000.0, R64::assert(0.0)).as_u8();
        let step_polar = estimate_steps_by_radius(1000.0, R64::assert(70.0)).as_u8();
        let step_extreme_polar = estimate_steps_by_radius(1000.0, R64::assert(85.0)).as_u8();

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
        let step_equator = estimate_steps_by_radius(1000.0, R64::assert(0.0)).as_u8();
        let step_polar = estimate_steps_by_radius(1000.0, R64::assert(-70.0)).as_u8();

        assert!(
            step_polar < step_equator,
            "negative polar ({step_polar}) should be less than equator ({step_equator})"
        );
    }

    #[test]
    fn estimate_steps_clamps_to_valid_range() {
        // Extremely large radius should clamp to 1.
        let step = estimate_steps_by_radius(f64::MAX, R64::assert(85.0)).as_u8();
        assert_eq!(step, 1);
    }

    fn coords(lon: f64, lat: f64) -> WGS84Coordinates {
        WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap()
    }

    #[test]
    fn bounding_box_symmetric_around_center() {
        let [min_lon, min_lat, max_lon, max_lat] = bounding_box(coords(10.0, 45.0), 1000.0);

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
        let small = bounding_box(coords(0.0, 45.0), 100.0);
        let large = bounding_box(coords(0.0, 45.0), 10_000.0);

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
        let equator = bounding_box(coords(0.0, 0.0), 1000.0);
        let high_lat = bounding_box(coords(0.0, 60.0), 1000.0);

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
