/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use geo::hash::haversine_distance;

#[test]
fn same_point_is_zero() {
    assert_eq!(
        haversine_distance(
            R64::assert(0.0),
            R64::assert(0.0),
            R64::assert(0.0),
            R64::assert(0.0)
        ),
        0.0
    );
    assert_eq!(
        haversine_distance(
            R64::assert(10.0),
            R64::assert(20.0),
            R64::assert(10.0),
            R64::assert(20.0)
        ),
        0.0
    );
}

#[test]
fn known_distance() {
    // London (51.5074, -0.1278) to Paris (48.8566, 2.3522)
    // Expected: ~343 km
    let dist = haversine_distance(
        R64::assert(-0.1278),
        R64::assert(51.5074),
        R64::assert(2.3522),
        R64::assert(48.8566),
    );
    assert!((dist - 343_556.0).abs() < 500.0, "got {dist}");
}

#[test]
fn symmetric() {
    let d1 = haversine_distance(
        R64::assert(1.0),
        R64::assert(2.0),
        R64::assert(3.0),
        R64::assert(4.0),
    );
    let d2 = haversine_distance(
        R64::assert(3.0),
        R64::assert(4.0),
        R64::assert(1.0),
        R64::assert(2.0),
    );
    assert!((d1 - d2).abs() < 1e-6);
}
