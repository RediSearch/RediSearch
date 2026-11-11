/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;

use crate::{DistanceUnit, RadiusFilter};
use rqe_core::{FieldIndex, RS_INVALID_FIELD_INDEX};

/// Integer unit slot, including unparsed or invalid values, in the C ABI.
#[cheadergen::config(export)]
pub type GeoDistance = i32;
/// Unrecognized or unparsed unit.
#[cheadergen::config(export)]
pub const GEO_DISTANCE_INVALID: GeoDistance = -1;
/// Kilometers in the C ABI.
#[cheadergen::config(export)]
pub const GEO_DISTANCE_KM: GeoDistance = DistanceUnit::Kilometers as i32;
/// Meters in the C ABI.
#[cheadergen::config(export)]
pub const GEO_DISTANCE_M: GeoDistance = DistanceUnit::Meters as i32;
/// Feet in the C ABI.
#[cheadergen::config(export)]
pub const GEO_DISTANCE_FT: GeoDistance = DistanceUnit::Feet as i32;
/// Miles in the C ABI.
#[cheadergen::config(export)]
pub const GEO_DISTANCE_MI: GeoDistance = DistanceUnit::Miles as i32;

/// Query filter layout shared with C. Pointer fields are owned by the query,
/// not by copies of this value used by readers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
#[expect(non_snake_case, reason = "C ABI field names")]
#[cheadergen::config(export)]
pub struct GeoFilter {
    /// Stable field index; field storage may move while a query is suspended.
    pub fieldIndex: FieldIndex,
    /// Latitude in degrees.
    pub lat: f64,
    /// Longitude in degrees.
    pub lon: f64,
    /// Radius in [`Self::unitType`].
    pub radius: f64,
    /// Parsed unit, or [`GEO_DISTANCE_INVALID`].
    pub unitType: GeoDistance,
    /// Opaque Rust-owned array of per-range numeric filters.
    pub numericFilters: *mut c_void,
}

impl GeoFilter {
    /// Construct a filter before field resolution and range allocation.
    pub const fn new(lon: f64, lat: f64, radius: f64, unit: Option<DistanceUnit>) -> Self {
        Self {
            fieldIndex: RS_INVALID_FIELD_INDEX,
            lat,
            lon,
            radius,
            unitType: match unit {
                Some(unit) => unit as i32,
                None => GEO_DISTANCE_INVALID,
            },
            numericFilters: std::ptr::null_mut(),
        }
    }

    /// Validate the parsed fields and construct a query circle.
    pub fn validate(&self) -> Result<RadiusFilter, &'static str> {
        RadiusFilter::new(
            self.lon,
            self.lat,
            self.radius,
            DistanceUnit::from_raw(self.unitType),
        )
    }
}

/// Legacy filter, borrowing its hidden field name until field resolution.
#[repr(C)]
#[cheadergen::config(export)]
pub struct LegacyGeoFilter {
    /// Query parameters and owned numeric ranges.
    pub base: GeoFilter,
    /// Borrowed-name wrapper allocated by `NewHiddenString`.
    pub field: *mut c_void,
}
