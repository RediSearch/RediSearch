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
use geo::abi::GeoFilter;
use index_result::RSIndexResult;
use pretty_assertions::assert_eq;
use rqe_core::RS_INVALID_FIELD_INDEX;

#[test]
fn reading_filter_based_on_geo_filter() {
    // Make an iterator with three records having different geo distances. The last record will be
    // filtered out based on the geo distance.
    let numeric = RSIndexResult::build_numeric;
    let iter = vec![
        numeric(3377736806566050.0).doc_id(10).build(), // (1°, 0°)
        numeric(3377848064680586.0).doc_id(11).build(), // (2°, 0°)
        numeric(3378251632246912.0).doc_id(12).build(), // (3°, 0°)
    ];

    let geo_filter = GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lat: 0.0,
        lon: 0.0,
        radius: 250_000.0,
        unitType: geo::abi::GEO_DISTANCE_M,
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
    assert!((111_000.0..111_500.0).contains(&result.as_numeric().unwrap()));

    let found = reader.next_record(&mut result).unwrap();
    assert!(found);
    assert_eq!(result.doc_id, 11);
    assert!((222_000.0..223_000.0).contains(&result.as_numeric().unwrap()));
    assert!(!reader.next_record(&mut result).unwrap());
}
