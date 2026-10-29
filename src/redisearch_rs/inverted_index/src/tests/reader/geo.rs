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
use ffi::{GeoDistance_GEO_DISTANCE_KM, GeoFilter};
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::RS_INVALID_FIELD_INDEX;

#[test]
fn reading_filter_based_on_geo_filter() {
    // Independent 26-bit quantization of (12.6, -42.2), (15, -42.25),
    // and (12.55, -42.3). The middle point lies outside the 10 km radius.
    let iter = vec![
        RSIndexResult::build_numeric(2_542_122_275_915_209.0)
            .doc_id(10)
            .build(),
        RSIndexResult::build_numeric(2_542_662_729_649_357.0)
            .doc_id(11)
            .build(),
        RSIndexResult::build_numeric(2_542_120_733_624_659.0)
            .doc_id(12)
            .build(),
    ];

    let geo_filter = GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lat: -42.25,
        lon: 12.5,
        radius: 10.0,
        unitType: GeoDistance_GEO_DISTANCE_KM,
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
    // Separate spherical-distance calculations for the quantized cell centers.
    assert!((result.as_numeric().unwrap() - 9938.194472756348).abs() < 1e-7);

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result.doc_id, 12);
    assert!((result.as_numeric().unwrap() - 6918.225959756095).abs() < 1e-7);
    assert!(!reader.next_record(&mut result).unwrap());
}
