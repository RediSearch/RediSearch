/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Missing`], [`Numeric`], [`Term`], and [`Wildcard`].

mod core;
mod geo;
mod missing;
mod numeric;
mod tag;
mod term;
mod wildcard;

pub use core::InvIndIterator;
pub use geo::{
    GeoRangeError, InvalidGeoInput, build_geo_numeric_filters, build_geo_range_iterator,
    extract_geo_unit_factor, new_geo_range_iterator,
};
pub use missing::{Missing, new_missing_iterator};
pub use numeric::{
    Numeric, NumericIteratorVariant, build_numeric_filter_iterator, open_numeric_or_geo_index,
};

pub use tag::{Tag, TagLookup};
pub use term::{Term, TermIndexReader, build_term_iterator};
pub use wildcard::Wildcard;
