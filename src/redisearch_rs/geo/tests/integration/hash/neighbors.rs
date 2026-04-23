/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{GEO_STEP_MAX, GeoHashBits, GeoHashNeighbors, decode, encode_wgs84, neighbors};

fn encode(lon: f64, lat: f64) -> GeoHashBits {
    encode_wgs84(lon, lat, GEO_STEP_MAX).unwrap()
}

#[test]
fn all_eight_neighbors_are_distinct() {
    let hash = encode(2.3522, 48.8566); // Paris
    let nbrs = neighbors(hash);

    let all = [
        hash,
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
        assert_eq!(cell.step, hash.step, "{name} step mismatch");
    }
}

#[test]
fn neighbor_of_neighbor_returns_to_original() {
    let hash = encode(-73.9857, 40.7484); // NYC
    let nbrs = neighbors(hash);

    // Going north then south should return to the original.
    let north_nbrs = neighbors(nbrs.north);
    assert_eq!(
        north_nbrs.south, hash,
        "south-of-north should be the original"
    );

    // Going east then west should return to the original.
    let east_nbrs = neighbors(nbrs.east);
    assert_eq!(east_nbrs.west, hash, "west-of-east should be the original");

    // Going south then north should return to the original.
    let south_nbrs = neighbors(nbrs.south);
    assert_eq!(
        south_nbrs.north, hash,
        "north-of-south should be the original"
    );

    // Going west then east should return to the original.
    let west_nbrs = neighbors(nbrs.west);
    assert_eq!(west_nbrs.east, hash, "east-of-west should be the original");
}

#[test]
fn diagonal_neighbors_are_consistent() {
    let hash = encode(139.6917, 35.6895); // Tokyo
    let nbrs = neighbors(hash);

    // north_east should be east-of-north and north-of-east.
    let north_nbrs = neighbors(nbrs.north);
    let east_nbrs = neighbors(nbrs.east);
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
    let center = decode(hash).unwrap();

    let north = decode(nbrs.north).unwrap();
    assert!(
        (north.latitude.min - center.latitude.max).abs() < 1e-10,
        "north cell should be adjacent above the center"
    );

    let south = decode(nbrs.south).unwrap();
    assert!(
        (south.latitude.max - center.latitude.min).abs() < 1e-10,
        "south cell should be adjacent below the center"
    );

    let east = decode(nbrs.east).unwrap();
    assert!(
        (east.longitude.min - center.longitude.max).abs() < 1e-10,
        "east cell should be adjacent to the right of center"
    );

    let west = decode(nbrs.west).unwrap();
    assert!(
        (west.longitude.max - center.longitude.min).abs() < 1e-10,
        "west cell should be adjacent to the left of center"
    );
}

#[test]
fn neighbors_at_lower_step_precision() {
    // Verify neighbors work at a lower precision too.
    let hash = encode_wgs84(10.0, 20.0, 5).unwrap();
    let nbrs = neighbors(hash);

    let all = [
        hash,
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

fn named_neighbors(nbrs: &GeoHashNeighbors) -> [(&str, GeoHashBits); 8] {
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
