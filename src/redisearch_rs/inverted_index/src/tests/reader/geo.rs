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
use geo::{
    GEO_DISTANCE_M, GeoFilter,
    hash::{GEO_STEP_MAX, WGS84Coordinates, encode_wgs84},
};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::RS_INVALID_FIELD_INDEX;

#[test]
fn reading_filter_based_on_geo_filter() {
    let iter = [(10, 0.001), (11, 0.002), (12, 1.0)].map(|(id, lon)| {
        let score =
            encode_wgs84(WGS84Coordinates::from_f64(lon, 0.0).unwrap(), GEO_STEP_MAX).bits as f64;
        RSIndexResult::build_numeric(score).doc_id(id).build()
    });

    let geo_filter = GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lat: 0.0,
        lon: 0.0,
        radius: 300.0,
        unitType: GEO_DISTANCE_M,
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

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result.doc_id, 10);
    assert!((result.as_numeric().unwrap() - 111.2).abs() < 0.5);

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result.doc_id, 11);
    assert!((result.as_numeric().unwrap() - 222.4).abs() < 0.5);
    assert!(!reader.next_record(&mut result).unwrap());
}
