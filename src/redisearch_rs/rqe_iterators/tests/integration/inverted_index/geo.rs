/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{
    GeoDistance_GEO_DISTANCE_FT, GeoDistance_GEO_DISTANCE_KM, GeoDistance_GEO_DISTANCE_M,
    GeoDistance_GEO_DISTANCE_MI,
};
use geo::{GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN};
use rqe_iterators::{build_geo_numeric_filters, extract_geo_unit_factor};

use crate::inverted_index::numeric::geo_filter_stub;

#[test]
fn unit_factor_meters() {
    assert_eq!(extract_geo_unit_factor(GeoDistance_GEO_DISTANCE_M), 1.0);
}

#[test]
fn unit_factor_kilometers() {
    assert_eq!(extract_geo_unit_factor(GeoDistance_GEO_DISTANCE_KM), 1000.0);
}

#[test]
fn unit_factor_feet() {
    assert_eq!(extract_geo_unit_factor(GeoDistance_GEO_DISTANCE_FT), 0.3048);
}

#[test]
fn unit_factor_miles() {
    assert_eq!(
        extract_geo_unit_factor(GeoDistance_GEO_DISTANCE_MI),
        1609.34
    );
}

// Tests for the five independent validation conditions in `build_geo_numeric_filters`.
// Each test makes exactly one condition true while keeping all preceding conditions false,
// so short-circuit evaluation guarantees only the target branch triggers the error.

#[test]
fn invalid_radius_is_rejected() {
    let mut gf = geo_filter_stub();
    gf.radius = 0.0;
    // SAFETY: radius <= 0.0 triggers the early-return before any pointer is used.
    assert!(unsafe { build_geo_numeric_filters(&mut gf) }.is_err());
}

#[test]
fn invalid_lon_too_high_is_rejected() {
    let mut gf = geo_filter_stub();
    gf.lon = GEO_LONG_MAX + 0.01;
    // SAFETY: lon > GEO_LONG_MAX triggers the early-return before any pointer is used.
    assert!(unsafe { build_geo_numeric_filters(&mut gf) }.is_err());
}

#[test]
fn invalid_lon_too_low_is_rejected() {
    let mut gf = geo_filter_stub();
    gf.lon = GEO_LONG_MIN - 0.01;
    // SAFETY: lon < GEO_LONG_MIN triggers the early-return before any pointer is used.
    assert!(unsafe { build_geo_numeric_filters(&mut gf) }.is_err());
}

#[test]
fn invalid_lat_too_high_is_rejected() {
    let mut gf = geo_filter_stub();
    gf.lat = GEO_LAT_MAX + 0.01;
    // SAFETY: lat > GEO_LAT_MAX triggers the early-return before any pointer is used.
    assert!(unsafe { build_geo_numeric_filters(&mut gf) }.is_err());
}

#[test]
fn invalid_lat_too_low_is_rejected() {
    let mut gf = geo_filter_stub();
    gf.lat = GEO_LAT_MIN - 0.01;
    // SAFETY: lat < GEO_LAT_MIN triggers the early-return before any pointer is used.
    assert!(unsafe { build_geo_numeric_filters(&mut gf) }.is_err());
}
