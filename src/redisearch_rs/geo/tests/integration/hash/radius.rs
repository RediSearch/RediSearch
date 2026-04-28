/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{
    GEO_LAT_MAX, GEO_LONG_MAX, GEO_LONG_MIN, decode, decode_to_lon_lat, get_areas_by_radius,
    haversine_distance,
};

#[test]
fn small_radius_returns_non_zero_center() {
    let result = get_areas_by_radius(2.3522, 48.8566, 1000.0); // Paris, 1km
    assert!(
        !result.hash.is_zero(),
        "center hash should be non-zero for valid coordinates"
    );
    assert!(
        !result.area.hash.is_zero(),
        "center area hash should be non-zero"
    );
}

#[test]
fn center_hash_contains_query_point() {
    let lon = -73.9857;
    let lat = 40.7484;
    let result = get_areas_by_radius(lon, lat, 500.0);
    let area = result.area;

    assert!(
        area.longitude.min <= lon && lon <= area.longitude.max,
        "query longitude {lon} should be within center cell [{}, {}]",
        area.longitude.min,
        area.longitude.max,
    );
    assert!(
        area.latitude.min <= lat && lat <= area.latitude.max,
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
    let dist = haversine_distance(lon, lat, nearby_lon, nearby_lat);

    let result = get_areas_by_radius(lon, lat, dist + 100.0);

    // The nearby point should fall within one of the 9 cells.
    let cells = [
        result.hash,
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
        if cell.is_zero() {
            return false;
        }
        let Ok(area) = decode(*cell) else {
            return false;
        };
        area.longitude.min <= nearby_lon
            && nearby_lon <= area.longitude.max
            && area.latitude.min <= nearby_lat
            && nearby_lat <= area.latitude.max
    });
    assert!(in_any_cell, "nearby point should be in one of the 9 cells");
}

#[test]
fn some_neighbors_are_zeroed_for_small_radius() {
    // A small radius at mid-latitudes: some bounding-box exclusion should
    // zero out neighbors that are outside the search area.
    let result = get_areas_by_radius(0.0, 0.0, 100.0);

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

    let zero_count = cells.iter().filter(|c| c.is_zero()).count();
    // With a very small radius, at least some neighbors should be excluded.
    assert!(
        zero_count > 0,
        "expected some zeroed neighbors for a 100m radius"
    );
}

#[test]
fn large_radius_triggers_step_decrease() {
    // At very high latitude with a large radius, the step should decrease.
    // This matches the Python test `testGeoLargeRadiusDecreaseStep`.
    let result_small = get_areas_by_radius(0.0, 85.0, 100.0);
    let result_large = get_areas_by_radius(0.0, 85.0, 500_000.0);

    assert!(
        result_large.hash.step < result_small.hash.step,
        "large radius at pole should use a coarser step ({}) than small radius ({})",
        result_large.hash.step,
        result_small.hash.step,
    );
}

#[test]
fn all_non_zero_neighbors_decode_successfully() {
    let result = get_areas_by_radius(139.6917, 35.6895, 5000.0); // Tokyo, 5km

    let cells = [
        ("center", result.hash),
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
        if !cell.is_zero() {
            assert!(
                decode(cell).is_ok(),
                "non-zero {name} cell should decode successfully"
            );
        }
    }
}

#[test]
fn center_decodes_to_point_near_query() {
    let lon = 10.0;
    let lat = 20.0;
    let result = get_areas_by_radius(lon, lat, 1000.0);

    let (decoded_lon, decoded_lat) = decode_to_lon_lat(result.hash).expect("center should decode");

    // The decoded center of the hash cell should be close to the query point.
    let dist = haversine_distance(lon, lat, decoded_lon, decoded_lat);
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
    let result = get_areas_by_radius(0.0, 0.0, 50_000_000.0);
    assert!(!result.hash.is_zero());
}

#[test]
fn near_antimeridian_does_not_panic() {
    let result = get_areas_by_radius(179.9, 0.0, 10_000.0);
    assert!(!result.hash.is_zero());

    let result = get_areas_by_radius(-179.9, 0.0, 10_000.0);
    assert!(!result.hash.is_zero());
}

#[test]
fn near_poles_does_not_panic() {
    let result = get_areas_by_radius(0.0, GEO_LAT_MAX - 0.01, 10_000.0);
    assert!(!result.hash.is_zero());

    let result = get_areas_by_radius(0.0, -GEO_LAT_MAX + 0.01, 10_000.0);
    assert!(!result.hash.is_zero());
}

#[test]
fn neighbor_exclusion_respects_bounding_box() {
    // Use a moderate radius and verify that non-zero neighbors are within
    // the bounding box (approximately).
    let lon = 0.0;
    let lat = 45.0;
    let radius = 5000.0;
    let result = get_areas_by_radius(lon, lat, radius);

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

    for cell in cells {
        if cell.is_zero() {
            continue;
        }
        let area = decode(cell).unwrap();
        let (cell_lon, cell_lat) = (
            (area.longitude.min + area.longitude.max) / 2.0,
            (area.latitude.min + area.latitude.max) / 2.0,
        );
        let dist = haversine_distance(lon, lat, cell_lon, cell_lat);
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
    let result = get_areas_by_radius(0.0, 0.0, 10_000_000.0);
    assert!(!result.hash.is_zero());
    assert_eq!(result.hash.step, result.area.hash.step);
}

#[test]
fn boundary_longitudes() {
    for lon in [GEO_LONG_MIN + 0.01, GEO_LONG_MAX - 0.01] {
        let result = get_areas_by_radius(lon, 0.0, 1000.0);
        assert!(
            !result.hash.is_zero(),
            "should handle boundary longitude {lon}"
        );
    }
}
