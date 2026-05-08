/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use geo::hash::{
    GEO_LAT_MAX, GEO_LONG_MAX, GEO_LONG_MIN, WGS84Coordinates, decode, decode_to_lon_lat,
    get_areas_by_radius, haversine_distance,
};

fn coords(lon: f64, lat: f64) -> WGS84Coordinates {
    WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap()
}

#[test]
fn small_radius_returns_valid_center() {
    let result = get_areas_by_radius(coords(2.3522, 48.8566), 1000.0); // Paris, 1km
    assert!(
        result.hash.bits != 0,
        "center hash should have non-zero bits for valid coordinates"
    );
}

#[test]
fn center_hash_contains_query_point() {
    let lon = -73.9857;
    let lat = 40.7484;
    let result = get_areas_by_radius(coords(lon, lat), 500.0);
    let area = result.area;

    assert!(
        area.longitude.min <= lon && area.longitude.max >= lon,
        "query longitude {lon} should be within center cell [{}, {}]",
        area.longitude.min,
        area.longitude.max,
    );
    assert!(
        area.latitude.min <= lat && area.latitude.max >= lat,
        "query latitude {lat} should be within center cell [{}, {}]",
        area.latitude.min,
        area.latitude.max,
    );
}

#[test]
fn radius_covers_nearby_point() {
    // Two points ~500m apart in Eilat, Israel.
    let lon = 29.69465;
    let lat = 34.95126;
    let nearby_lon = 29.69350;
    let nearby_lat = 34.94737;
    let dist = haversine_distance(
        R64::assert(lon),
        R64::assert(lat),
        R64::assert(nearby_lon),
        R64::assert(nearby_lat),
    );

    let result = get_areas_by_radius(coords(lon, lat), dist + 100.0);

    // The nearby point should fall within one of the 9 cells.
    let cells = [
        Some(result.hash),
        result.neighbors.north,
        result.neighbors.south,
        result.neighbors.east,
        result.neighbors.west,
        result.neighbors.north_east,
        result.neighbors.north_west,
        result.neighbors.south_east,
        result.neighbors.south_west,
    ];

    let in_any_cell = cells.iter().any(|cell| {
        let Some(cell) = cell else {
            return false;
        };
        let area = decode(*cell);
        area.longitude.min <= nearby_lon
            && area.longitude.max >= nearby_lon
            && area.latitude.min <= nearby_lat
            && area.latitude.max >= nearby_lat
    });
    assert!(in_any_cell, "nearby point should be in one of the 9 cells");
}

#[test]
fn some_neighbors_are_zeroed_for_small_radius() {
    // A small radius at mid-latitudes: some bounding-box exclusion should
    // zero out neighbors that are outside the search area.
    let result = get_areas_by_radius(coords(0.0, 0.0), 100.0);

    let cells = [
        result.neighbors.north,
        result.neighbors.south,
        result.neighbors.east,
        result.neighbors.west,
        result.neighbors.north_east,
        result.neighbors.north_west,
        result.neighbors.south_east,
        result.neighbors.south_west,
    ];

    let none_count = cells.iter().filter(|c| c.is_none()).count();
    // With a very small radius, at least some neighbors should be excluded.
    assert!(
        none_count > 0,
        "expected some None neighbors for a 100m radius"
    );
}

#[test]
fn large_radius_triggers_step_decrease() {
    // At very high latitude with a large radius, the step should decrease.
    let result_small = get_areas_by_radius(coords(0.0, 85.0), 100.0);
    let result_large = get_areas_by_radius(coords(0.0, 85.0), 500_000.0);

    assert!(
        result_large.hash.step < result_small.hash.step,
        "large radius at pole should use a coarser step ({}) than small radius ({})",
        result_large.hash.step.as_u8(),
        result_small.hash.step.as_u8(),
    );
}

#[test]
fn all_non_zero_neighbors_decode_successfully() {
    let result = get_areas_by_radius(coords(139.6917, 35.6895), 5000.0); // Tokyo, 5km

    let cells = [
        ("center", Some(result.hash)),
        ("north", result.neighbors.north),
        ("south", result.neighbors.south),
        ("east", result.neighbors.east),
        ("west", result.neighbors.west),
        ("north_east", result.neighbors.north_east),
        ("north_west", result.neighbors.north_west),
        ("south_east", result.neighbors.south_east),
        ("south_west", result.neighbors.south_west),
    ];

    for (name, cell) in cells {
        if let Some(cell) = cell {
            // decode is infallible — just verify the area is sane.
            let area = decode(cell);
            assert!(
                area.latitude.min < area.latitude.max,
                "{name} cell should have valid latitude range"
            );
        }
    }
}

#[test]
fn center_decodes_to_point_near_query() {
    let lon = 10.0;
    let lat = 20.0;
    let result = get_areas_by_radius(coords(lon, lat), 1000.0);

    let (decoded_lon, decoded_lat) = decode_to_lon_lat(result.hash);

    // The decoded center of the hash cell should be close to the query point.
    let dist = haversine_distance(R64::assert(lon), R64::assert(lat), decoded_lon, decoded_lat);
    // The cell size at step ~24 is sub-meter; at step ~10 it can be ~km.
    // With a 1km radius the step is high, so the center should be very close.
    assert!(
        dist < 1000.0,
        "decoded center should be within the search radius, got {dist}m"
    );
}

#[test]
fn very_large_radius_does_not_panic() {
    // Radius larger than Earth's circumference — should not panic.
    let _result = get_areas_by_radius(coords(0.0, 0.0), 50_000_000.0);
}

#[test]
fn near_antimeridian_does_not_panic() {
    let _result = get_areas_by_radius(coords(179.9, 0.0), 10_000.0);
    let _result = get_areas_by_radius(coords(-179.9, 0.0), 10_000.0);
}

#[test]
fn near_poles_does_not_panic() {
    let _result = get_areas_by_radius(coords(0.0, GEO_LAT_MAX - 0.01), 10_000.0);
    let _result = get_areas_by_radius(coords(0.0, -GEO_LAT_MAX + 0.01), 10_000.0);
}

#[test]
fn neighbor_exclusion_respects_bounding_box() {
    // Use a moderate radius and verify that non-zero neighbors are within
    // the bounding box (approximately).
    let lon = 0.0;
    let lat = 45.0;
    let radius = 5000.0;
    let result = get_areas_by_radius(coords(lon, lat), radius);

    let cells = [
        result.neighbors.north,
        result.neighbors.south,
        result.neighbors.east,
        result.neighbors.west,
        result.neighbors.north_east,
        result.neighbors.north_west,
        result.neighbors.south_east,
        result.neighbors.south_west,
    ];

    for cell in cells.into_iter().flatten() {
        let area = decode(cell);
        let cell_lon = (area.longitude.min + area.longitude.max) / 2.0;
        let cell_lat = (area.latitude.min + area.latitude.max) / 2.0;
        let dist = haversine_distance(R64::assert(lon), R64::assert(lat), cell_lon, cell_lat);
        // Non-zero neighbors should be reasonably close to the query point.
        // The cell centers should be within a few multiples of the radius.
        assert!(
            dist < radius * 10.0,
            "non-zero neighbor center at ({cell_lon}, {cell_lat}) is {dist}m away, too far"
        );
    }
}

#[test]
fn duplicate_neighbor_suppression_with_huge_radius() {
    // With a very large radius, adjacent neighbors can become the same cell.
    // `get_areas_by_radius` uses a coarse step, so multiple neighbors may
    // share the same hash (the FFI layer in `calcRanges` deduplicates them).
    // Just verify it doesn't panic and returns a valid result.
    let result = get_areas_by_radius(coords(0.0, 0.0), 10_000_000.0);
    assert_eq!(result.hash.step, result.area.hash.step);
}

#[test]
fn boundary_longitudes() {
    for lon in [GEO_LONG_MIN + 0.01, GEO_LONG_MAX - 0.01] {
        let _result = get_areas_by_radius(coords(lon, 0.0), 1000.0);
    }
}

#[test]
fn high_latitude_bounding_box_over_pruning() {
    // The planar bounding-box approximation used by `get_areas_by_radius`
    // does not behave correctly at very high latitudes with large radii.
    // For instance, at lat ≈ 84° with a 50 km radius the longitude delta
    // computed from `radius / (R * cos(lat))` explodes because cos(84°) is
    // small, but the *latitude* delta stays modest, so the box is very wide
    // in longitude yet narrow in latitude. This can cause the south (and
    // south-east / south-west) neighbors to be incorrectly pruned even
    // though points within the radius exist in those cells.
    let result = get_areas_by_radius(coords(0.0, 84.0), 50_000.0);
    let nbr_count = [
        result.neighbors.north,
        result.neighbors.south,
        result.neighbors.east,
        result.neighbors.west,
        result.neighbors.north_east,
        result.neighbors.north_west,
        result.neighbors.south_east,
        result.neighbors.south_west,
    ]
    .iter()
    .filter(|c| c.is_some())
    .count();
    // Current behavior: only 1 of 8 neighbors survives pruning.
    // If the bounding-box logic is improved, update this assertion.
    assert_eq!(nbr_count, 1);
}
