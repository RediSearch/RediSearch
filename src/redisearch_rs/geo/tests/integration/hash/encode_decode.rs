/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::hash::{EncodeError, GEO_STEP_MAX, decode_to_lon_lat, encode_wgs84};

#[test]
fn encode_decode_roundtrip() {
    let cases = [
        (0.0, 0.0),
        (10.0, 20.0),
        (-122.4194, 37.7749),   // San Francisco
        (2.3522, 48.8566),      // Paris
        (139.6917, 35.6895),    // Tokyo
        (-0.1278, 51.5074),     // London
        (180.0, 85.05112878),   // max bounds
        (-180.0, -85.05112878), // min bounds
    ];

    for (lon, lat) in cases {
        let hash = encode_wgs84(lon, lat, GEO_STEP_MAX)
            .unwrap_or_else(|e| panic!("failed to encode ({lon}, {lat}): {e:?}"));
        let (decoded_lon, decoded_lat) = decode_to_lon_lat(hash)
            .unwrap_or_else(|e| panic!("failed to decode ({lon}, {lat}): {e}"));
        // 52-bit precision gives sub-meter accuracy, so 0.001 degree tolerance is generous
        assert!(
            (decoded_lon - lon).abs() < 0.001,
            "lon mismatch for ({lon}, {lat}): got {decoded_lon}"
        );
        assert!(
            (decoded_lat - lat).abs() < 0.001,
            "lat mismatch for ({lon}, {lat}): got {decoded_lat}"
        );
    }
}

#[test]
fn encode_out_of_range() {
    assert_eq!(
        encode_wgs84(181.0, 0.0, GEO_STEP_MAX),
        Err(EncodeError::OutOfRange)
    );
    assert_eq!(
        encode_wgs84(-181.0, 0.0, GEO_STEP_MAX),
        Err(EncodeError::OutOfRange)
    );
    assert_eq!(
        encode_wgs84(0.0, 90.0, GEO_STEP_MAX),
        Err(EncodeError::OutOfRange)
    );
    assert_eq!(
        encode_wgs84(0.0, -90.0, GEO_STEP_MAX),
        Err(EncodeError::OutOfRange)
    );
}

#[test]
fn encode_invalid_step() {
    assert_eq!(encode_wgs84(0.0, 0.0, 0), Err(EncodeError::InvalidStep));
    assert_eq!(encode_wgs84(0.0, 0.0, 33), Err(EncodeError::InvalidStep));
}

#[test]
fn nearby_points_have_similar_hashes() {
    let h1 = encode_wgs84(29.69465, 34.95126, GEO_STEP_MAX).unwrap();
    let h2 = encode_wgs84(29.69350, 34.94737, GEO_STEP_MAX).unwrap();
    // Nearby points should share high-order bits
    let aligned1 = geo::hash::align_52bits(h1);
    let aligned2 = geo::hash::align_52bits(h2);
    // Top 30 bits should match for points ~500m apart
    assert_eq!(aligned1 >> 22, aligned2 >> 22);
}
