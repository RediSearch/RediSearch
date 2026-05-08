/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::{R64, real::UnaryRealFunction};
use geo::hash::{
    GeoHashBits, GeoHashNeighbors, PrecisionStep, WGS84Coordinates, decode, encode_wgs84, neighbors,
};

fn encode(lon: f64, lat: f64) -> GeoHashBits {
    let coords = WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap();
    encode_wgs84(coords, PrecisionStep::new(26).unwrap())
}

#[test]
fn all_eight_neighbors_are_distinct() {
    let hash = encode(2.3522, 48.8566); // Paris
    let nbrs = neighbors(hash);

    let all = [
        Some(hash),
        nbrs.north,
        nbrs.south,
        nbrs.east,
        nbrs.west,
        nbrs.north_east,
        nbrs.north_west,
        nbrs.south_east,
        nbrs.south_west,
    ];

    // All 9 cells (center + 8 neighbors) should be distinct.
    for (i, a) in all.iter().enumerate() {
        for (j, b) in all.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "cells {i} and {j} should differ");
            }
        }
    }
}

#[test]
fn all_neighbors_have_same_step() {
    let hash = encode(10.0, 20.0);
    let nbrs = neighbors(hash);

    for (name, cell) in named_neighbors(&nbrs) {
        let cell = cell.expect("neighbor should be present");
        assert_eq!(cell.step, hash.step, "{name} step mismatch");
    }
}

#[test]
fn neighbor_of_neighbor_returns_to_original() {
    let hash = encode(-73.9857, 40.7484); // NYC
    let nbrs = neighbors(hash);

    // Going north then south should return to the original.
    let north_nbrs = neighbors(nbrs.north.unwrap());
    assert_eq!(
        north_nbrs.south.unwrap(),
        hash,
        "south-of-north should be the original"
    );

    // Going east then west should return to the original.
    let east_nbrs = neighbors(nbrs.east.unwrap());
    assert_eq!(
        east_nbrs.west.unwrap(),
        hash,
        "west-of-east should be the original"
    );

    // Going south then north should return to the original.
    let south_nbrs = neighbors(nbrs.south.unwrap());
    assert_eq!(
        south_nbrs.north.unwrap(),
        hash,
        "north-of-south should be the original"
    );

    // Going west then east should return to the original.
    let west_nbrs = neighbors(nbrs.west.unwrap());
    assert_eq!(
        west_nbrs.east.unwrap(),
        hash,
        "east-of-west should be the original"
    );
}

#[test]
fn diagonal_neighbors_are_consistent() {
    let hash = encode(139.6917, 35.6895); // Tokyo
    let nbrs = neighbors(hash);

    // north_east should be east-of-north and north-of-east.
    let north_nbrs = neighbors(nbrs.north.unwrap());
    let east_nbrs = neighbors(nbrs.east.unwrap());
    assert_eq!(
        nbrs.north_east, north_nbrs.east,
        "north_east should equal east-of-north"
    );
    assert_eq!(
        nbrs.north_east, east_nbrs.north,
        "north_east should equal north-of-east"
    );
}

#[test]
fn neighbors_decoded_areas_are_adjacent() {
    let hash = encode(0.0, 0.0);
    let nbrs = neighbors(hash);
    let center = decode(hash);

    let north = decode(nbrs.north.unwrap());
    assert!(
        (north.latitude.min - center.latitude.max).abs() < 1e-10,
        "north cell should be adjacent above the center"
    );

    let south = decode(nbrs.south.unwrap());
    assert!(
        (south.latitude.max - center.latitude.min).abs() < 1e-10,
        "south cell should be adjacent below the center"
    );

    let east = decode(nbrs.east.unwrap());
    assert!(
        (east.longitude.min - center.longitude.max).abs() < 1e-10,
        "east cell should be adjacent to the right of center"
    );

    let west = decode(nbrs.west.unwrap());
    assert!(
        (west.longitude.max - center.longitude.min).abs() < 1e-10,
        "west cell should be adjacent to the left of center"
    );
}

#[test]
fn neighbors_at_lower_step_precision() {
    // Verify neighbors work at a lower precision too.
    let coords = WGS84Coordinates::new(R64::assert(10.0), R64::assert(20.0)).unwrap();
    let hash = encode_wgs84(coords, PrecisionStep::new(5).unwrap());
    let nbrs = neighbors(hash);

    let all = [
        Some(hash),
        nbrs.north,
        nbrs.south,
        nbrs.east,
        nbrs.west,
        nbrs.north_east,
        nbrs.north_west,
        nbrs.south_east,
        nbrs.south_west,
    ];

    for (i, a) in all.iter().enumerate() {
        for (j, b) in all.iter().enumerate() {
            if i != j {
                assert_ne!(a, b, "cells {i} and {j} should differ at step=5");
            }
        }
    }
}

#[cfg(not(miri))] // proptest calls getcwd() which is not supported on Miri
mod proptests {
    use decorum::{R64, real::UnaryRealFunction};
    use geo::hash::{
        GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, PrecisionStep, WGS84Coordinates,
        decode, encode_wgs84, neighbors,
    };
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn all_eight_neighbors_are_distinct(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            // At step=1 the grid is 2×2, so east and west wrap to the same
            // cell. Step >= 2 gives at least 4 cells per dimension, enough
            // for a 3×3 neighborhood without aliasing.
            step in 2u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(), step);
            let nbrs = neighbors(hash);

            let all = [
                Some(hash),
                nbrs.north, nbrs.south, nbrs.east, nbrs.west,
                nbrs.north_east, nbrs.north_west, nbrs.south_east, nbrs.south_west,
            ];

            for (i, a) in all.iter().enumerate() {
                for (j, b) in all.iter().enumerate() {
                    if i != j {
                        prop_assert_ne!(a, b, "cells {} and {} should differ at step={}", i, j, step.as_u8());
                    }
                }
            }
        }

        #[test]
        fn all_neighbors_have_same_step(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            step in 1u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(), step);
            let nbrs = neighbors(hash);

            for (name, cell) in super::named_neighbors(&nbrs) {
                let cell = cell.unwrap();
                prop_assert_eq!(
                    cell.step, hash.step,
                    "{} step mismatch at ({}, {}) step={}", name, lon, lat, step.as_u8()
                );
            }
        }

        #[test]
        fn neighbor_of_neighbor_returns_to_original(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            step in 1u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(), step);
            let nbrs = neighbors(hash);

            let north_nbrs = neighbors(nbrs.north.unwrap());
            prop_assert_eq!(
                north_nbrs.south.unwrap(), hash,
                "south-of-north should be original at ({}, {}) step={}", lon, lat, step.as_u8()
            );

            let east_nbrs = neighbors(nbrs.east.unwrap());
            prop_assert_eq!(
                east_nbrs.west.unwrap(), hash,
                "west-of-east should be original at ({}, {}) step={}", lon, lat, step.as_u8()
            );

            let south_nbrs = neighbors(nbrs.south.unwrap());
            prop_assert_eq!(
                south_nbrs.north.unwrap(), hash,
                "north-of-south should be original at ({}, {}) step={}", lon, lat, step.as_u8()
            );

            let west_nbrs = neighbors(nbrs.west.unwrap());
            prop_assert_eq!(
                west_nbrs.east.unwrap(), hash,
                "east-of-west should be original at ({}, {}) step={}", lon, lat, step.as_u8()
            );
        }

        #[test]
        fn diagonal_neighbors_are_consistent(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            step in 1u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(), step);
            let nbrs = neighbors(hash);

            let north_nbrs = neighbors(nbrs.north.unwrap());
            let east_nbrs = neighbors(nbrs.east.unwrap());
            prop_assert_eq!(
                nbrs.north_east, north_nbrs.east,
                "north_east should equal east-of-north at ({}, {}) step={}", lon, lat, step.as_u8()
            );
            prop_assert_eq!(
                nbrs.north_east, east_nbrs.north,
                "north_east should equal north-of-east at ({}, {}) step={}", lon, lat, step.as_u8()
            );
        }

        #[test]
        fn neighbors_decoded_areas_are_adjacent(
            lon in GEO_LONG_MIN..=GEO_LONG_MAX,
            lat in GEO_LAT_MIN..=GEO_LAT_MAX,
            step in 1u8..=26,
        ) {
            let step = PrecisionStep::new(step).unwrap();
            let hash = encode_wgs84(WGS84Coordinates::new(R64::assert(lon), R64::assert(lat)).unwrap(), step);
            let nbrs = neighbors(hash);
            let center = decode(hash);

            // At grid edges, move_x/move_y wrap around so the "neighbor"
            // is on the opposite side of the grid. Only assert geometric
            // adjacency when the neighbor didn't wrap.
            let north = decode(nbrs.north.unwrap());
            if north.latitude.min > center.latitude.min {
                prop_assert!(
                    (north.latitude.min - center.latitude.max).abs() < 1e-10,
                    "north cell should be adjacent above center at ({}, {}) step={}", lon, lat, step.as_u8()
                );
            }

            let south = decode(nbrs.south.unwrap());
            if south.latitude.max < center.latitude.max {
                prop_assert!(
                    (south.latitude.max - center.latitude.min).abs() < 1e-10,
                    "south cell should be adjacent below center at ({}, {}) step={}", lon, lat, step.as_u8()
                );
            }

            let east = decode(nbrs.east.unwrap());
            if east.longitude.min > center.longitude.min {
                prop_assert!(
                    (east.longitude.min - center.longitude.max).abs() < 1e-10,
                    "east cell should be adjacent right of center at ({}, {}) step={}", lon, lat, step.as_u8()
                );
            }

            let west = decode(nbrs.west.unwrap());
            if west.longitude.max < center.longitude.max {
                prop_assert!(
                    (west.longitude.max - center.longitude.min).abs() < 1e-10,
                    "west cell should be adjacent left of center at ({}, {}) step={}", lon, lat, step.as_u8()
                );
            }
        }
    }
}

fn named_neighbors(nbrs: &GeoHashNeighbors) -> [(&str, Option<GeoHashBits>); 8] {
    [
        ("north", nbrs.north),
        ("south", nbrs.south),
        ("east", nbrs.east),
        ("west", nbrs.west),
        ("north_east", nbrs.north_east),
        ("north_west", nbrs.north_west),
        ("south_east", nbrs.south_east),
        ("south_west", nbrs.south_west),
    ]
}
