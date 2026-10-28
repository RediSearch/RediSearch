/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use decorum::R64;
use geo::{DistanceUnit, FilterError, FilterParams, hash};
use proptest::prelude::*;

#[test]
fn units_preserve_names_factors_and_discriminants() {
    for (unit, name, factor, discriminant) in [
        (DistanceUnit::Kilometers, c"km", 1000.0, 0),
        (DistanceUnit::Meters, c"m", 1.0, 1),
        (DistanceUnit::Feet, c"ft", 0.3048, 2),
        (DistanceUnit::Miles, c"mi", 1609.34, 3),
    ] {
        assert_eq!(unit.as_c_str(), name);
        assert_eq!(unit.meters(), factor);
        assert_eq!(unit as i32, discriminant);
        assert_eq!(DistanceUnit::parse(name.to_bytes()), Some(unit));
        assert_eq!(
            DistanceUnit::parse(&name.to_bytes().to_ascii_uppercase()),
            Some(unit)
        );
    }
    assert_eq!(DistanceUnit::default(), DistanceUnit::Kilometers);
    assert_eq!(DistanceUnit::parse(b"fT"), Some(DistanceUnit::Feet));
    assert_eq!(DistanceUnit::parse(b"Mi"), Some(DistanceUnit::Miles));
    for bytes in [b"".as_slice(), b"meters", b" km", b"km ", b"\xff", b"km\0"] {
        assert_eq!(DistanceUnit::parse(bytes), None);
    }
}

#[test]
fn unit_buffers_preserve_nul_and_length_rules() {
    assert_eq!(
        DistanceUnit::parse_buffer(b"km\0garbage"),
        Some(DistanceUnit::Kilometers)
    );
    assert_eq!(
        DistanceUnit::parse_buffer(b"km\0xxxxxxxxxxxx"),
        Some(DistanceUnit::Kilometers)
    );
    assert_eq!(DistanceUnit::parse_buffer(b"km\0xxxxxxxxxxxxx"), None);
    assert_eq!(DistanceUnit::parse_buffer(b""), None);
    assert_eq!(DistanceUnit::parse_buffer(b"\0km"), None);
}

const fn filter() -> FilterParams {
    FilterParams {
        lon: 12.0,
        lat: -33.25,
        radius: 40.0,
        unit: Some(DistanceUnit::Kilometers),
    }
}

#[test]
fn validation_preserves_bounds_precedence_and_messages() {
    let mut f = filter();
    for lat in [-90.0, 90.0] {
        for lon in [-180.0, 180.0] {
            f.lat = lat;
            f.lon = lon;
            assert_eq!(f.validate(), Ok(()));
        }
    }
    for (lat, lon) in [
        (-90.0_f64.next_up(), 0.0),
        (90.0_f64.next_up(), 0.0),
        (0.0, -180.0_f64.next_up()),
        (0.0, 180.0_f64.next_up()),
        (f64::NEG_INFINITY, 0.0),
        (0.0, f64::INFINITY),
    ] {
        f.lat = lat;
        f.lon = lon;
        assert_eq!(f.validate(), Err(FilterError::Coordinates));
    }
    f.radius = 0.0;
    f.unit = None;
    assert_eq!(f.validate(), Err(FilterError::Unit));
    assert_eq!(FilterError::Unit.to_string(), "Invalid GeoFilter unit");
    f.unit = Some(DistanceUnit::Meters);
    assert_eq!(f.validate(), Err(FilterError::Coordinates));
    assert_eq!(
        FilterError::Coordinates.to_string(),
        "Invalid GeoFilter lat/lon"
    );
    f.lon = 0.0;
    f.lat = 0.0;
    for radius in [0.0, -0.0, -1.0, f64::NEG_INFINITY] {
        f.radius = radius;
        assert_eq!(f.validate(), Err(FilterError::Radius));
    }
    assert_eq!(FilterError::Radius.to_string(), "Invalid GeoFilter radius");
    f.radius = f64::MIN_POSITIVE;
    assert_eq!(f.validate(), Ok(()));
}

#[test]
fn validation_is_not_a_finiteness_check() {
    let mut f = filter();
    f.radius = f64::INFINITY;
    assert_eq!(f.validate(), Ok(()));
    for field in 0..3 {
        let mut f = filter();
        match field {
            0 => f.lon = f64::NAN,
            1 => f.lat = f64::NAN,
            _ => f.radius = f64::NAN,
        }
        assert_eq!(f.validate(), Ok(()));
    }
    f.lon = f64::NAN;
    f.lat = f64::NAN;
    f.radius = f64::NAN;
    assert_eq!(f.validate(), Ok(()));
    f.unit = None;
    assert_eq!(f.validate(), Err(FilterError::Unit));
}

#[test]
fn radius_is_inclusive_and_returns_meters_on_misses() {
    let hash = hash::encode_wgs84(
        hash::WGS84Coordinates::from_f64(12.25, -33.5).unwrap(),
        hash::GEO_STEP_MAX,
    );
    let score = hash::align_52bits(hash) as f64;
    let (lon, lat) = hash::decode_to_lon_lat(hash);
    let distance = hash::haversine_distance(R64::assert(12.0), R64::assert(-33.25), lon, lat);
    let mut f = filter();
    f.unit = Some(DistanceUnit::Meters);
    // Repeated libm calls may differ by a few ULPs (notably under Miri).
    // Zero distance gives an exact boundary without depending on that rounding.
    f.lon = lon.into_inner();
    f.lat = lat.into_inner();
    f.radius = 0.0;
    assert_eq!(f.within_radius(score), (true, 0.0));
    f.radius = -f64::MIN_POSITIVE;
    assert_eq!(f.within_radius(score), (false, 0.0));
    f.lon = 12.0;
    f.lat = -33.25;
    for (unit, factor) in [
        (DistanceUnit::Meters, 1.0),
        (DistanceUnit::Kilometers, 1000.0),
        (DistanceUnit::Feet, 0.3048),
        (DistanceUnit::Miles, 1609.34),
    ] {
        f.unit = Some(unit);
        f.radius = (distance + 1.0) / factor;
        let (found, measured) = f.within_radius(score);
        assert!(found);
        assert!((measured - distance).abs() < 1e-7);
        f.radius = (distance - 1.0) / factor;
        let (found, measured) = f.within_radius(score);
        assert!(!found);
        assert!((measured - distance).abs() < 1e-7);
    }
    f.radius = f64::INFINITY;
    assert!(f.within_radius(score).0);
    f.radius = f64::NAN;
    assert!(!f.within_radius(score).0);
}

#[test]
fn radius_requires_a_unit_and_finite_center() {
    for f in [
        FilterParams {
            unit: None,
            ..filter()
        },
        FilterParams {
            lon: f64::NAN,
            ..filter()
        },
        FilterParams {
            lat: f64::INFINITY,
            ..filter()
        },
    ] {
        assert!(std::panic::catch_unwind(|| f.within_radius(0.0)).is_err());
    }
}

proptest! {
    #![proptest_config({
        let mut config = ProptestConfig::default();
        if cfg!(miri) {
            // Miri's isolated runner cannot access regression files on disk.
            config.failure_persistence = None;
            config.cases = 32;
        }
        config
    })]

    #[test]
    fn buffer_units_match_a_case_folded_nul_terminated_reference(bytes in prop_oneof![
        prop::collection::vec(any::<u8>(), 0..24),
        (prop::sample::select(vec![b"km".as_slice(), b"m", b"ft", b"mi"]),
         any::<bool>(), prop::collection::vec(any::<u8>(), 0..20))
            .prop_map(|(unit, uppercase, suffix)| {
                let mut bytes = if uppercase { unit.to_ascii_uppercase() } else { unit.to_vec() };
                bytes.push(0);
                bytes.extend(suffix);
                bytes
            }),
    ]) {
        let prefix: Vec<_> = bytes.iter().copied().take_while(|&b| b != 0).collect();
        let expected = if bytes.len() >= 16 { None } else {
            match prefix.to_ascii_lowercase().as_slice() {
                b"km" => Some(DistanceUnit::Kilometers),
                b"m" => Some(DistanceUnit::Meters),
                b"ft" => Some(DistanceUnit::Feet),
                b"mi" => Some(DistanceUnit::Miles),
                _ => None,
            }
        };
        prop_assert_eq!(DistanceUnit::parse_buffer(&bytes), expected);
    }

    #[test]
    fn radius_matches_an_independent_spherical_distance(
        lon in -170.0..170.0f64, lat in -75.0..75.0f64,
        target_lon in -170.0..170.0f64, target_lat in -75.0..75.0f64,
        unit_index in 0usize..4,
    ) {
        let hash = hash::encode_wgs84(hash::WGS84Coordinates::from_f64(target_lon, target_lat).unwrap(), hash::GEO_STEP_MAX);
        let score = hash::align_52bits(hash) as f64;
        let (target_lon, target_lat) = hash::decode_to_lon_lat(hash);
        // Spherical law of cosines, independent of the implementation's haversine formula.
        let a = lat.to_radians();
        let b = target_lat.into_inner().to_radians();
        let delta = (lon - target_lon.into_inner()).to_radians();
        let meters = (a.sin() * b.sin() + a.cos() * b.cos() * delta.cos()).clamp(-1.0, 1.0).acos() * 6_372_797.560_856;
        let (unit, factor) = [(DistanceUnit::Kilometers, 1000.0), (DistanceUnit::Meters, 1.0),
                             (DistanceUnit::Feet, 0.3048), (DistanceUnit::Miles, 1609.34)][unit_index];
        let mut f = FilterParams {lon, lat, radius: (meters + 1.0) / factor, unit: Some(unit)};
        let (found, distance) = f.within_radius(score);
        prop_assert!(found);
        prop_assert!((distance - meters).abs() < 0.2);
        f.radius = (meters - 1.0) / factor;
        prop_assert!(!f.within_radius(score).0);
    }
}
