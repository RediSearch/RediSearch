/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr;

use crate::{FilterGeoReader, IndexReader, NumericFilter};
use ffi::{GeoDistance_GEO_DISTANCE_M, GeoFilter};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::RS_INVALID_FIELD_INDEX;

#[test]
fn reading_filter_based_on_geo_filter() {
    let iter: Vec<_> = [0.005, 0.015, 0.025]
        .into_iter()
        .enumerate()
        .map(|(i, lon)| {
            let point = geo::hash::WGS84Coordinates::from_f64(lon, 0.0).unwrap();
            let score = geo::hash::encode_wgs84(point, geo::hash::GEO_STEP_MAX).bits as f64;
            RSIndexResult::build_numeric(score)
                .doc_id(10 + i as u64)
                .build()
        })
        .collect();

    let geo_filter = GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lat: 0.0,
        lon: 0.0,
        radius: 2000.0,
        unitType: GeoDistance_GEO_DISTANCE_M,
        numericFilters: ptr::null_mut(),
    };

    let filter = NumericFilter {
        min: 0.0,
        max: 0.0,
        min_inclusive: false,
        max_inclusive: false,
        geo_filter: &geo_filter as *const _ as *const _,
        ascending: true,
        ..Default::default()
    };

    let mut reader = FilterGeoReader::new(filter, iter.into_iter());
    let mut result = RSIndexResult::build_numeric(0.0).build();

    // Equatorial arc lengths in meters, with tolerance for geohash quantization.
    for (id, distance) in [(10, 556.1), (11, 1668.4)] {
        assert!(reader.next_record(&mut result).unwrap());
        assert_eq!(result.doc_id, id);
        assert!((result.as_numeric().unwrap() - distance).abs() < 1.0);
    }
    assert!(!reader.next_record(&mut result).unwrap());
}
