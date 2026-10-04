/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use enumflags2::{BitFlags, bitflags};

pub type RSResultKindMask = BitFlags<RSResultKind, u8>;

/// A C-style discriminant for [`super::result_data::RSResultData`].
///
/// # Implementation notes
///
/// We need a standalone C-style discriminant to get `bitflags` to generate a
/// dedicated bitmask type. Unfortunately, we can't apply `#[bitflags]` directly
/// on [`super::result_data::RSResultData`] since `bitflags` doesn't support enum with data in
/// their variants, nor lifetime parameters.
///
/// The discriminant values must match *exactly* the ones specified
/// on [`super::result_data::RSResultData`].
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RSResultKind {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

impl std::fmt::Display for RSResultKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let k = match self {
            RSResultKind::Union => "Union",
            RSResultKind::Intersection => "Intersection",
            RSResultKind::Term => "Term",
            RSResultKind::Virtual => "Virtual",
            RSResultKind::Numeric => "Numeric",
            RSResultKind::Metric => "Metric",
            RSResultKind::HybridMetric => "HybridMetric",
        };
        write!(f, "{k}")
    }
}
