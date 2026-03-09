/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::aggregate::RSAggregateResult;
use super::kind::RSResultKind;
use super::term_record::RSTermRecord;

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`super::kind::RSResultKindMask`]
///
/// The `'index` lifetime is linked to the [`crate::IndexBlock`] when decoding borrows from the block.
#[repr(u8)]
#[derive(Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultData<'index> {
    Union(RSAggregateResult<'index>) = 1,
    Intersection(RSAggregateResult<'index>) = 2,
    Term(RSTermRecord<'index>) = 4,
    Virtual = 8,
    Numeric(f64) = 16,
    Metric(f64) = 32,
    HybridMetric(RSAggregateResult<'index>) = 64,
}

impl RSResultData<'_> {
    pub const fn kind(&self) -> RSResultKind {
        match self {
            RSResultData::Union(_) => RSResultKind::Union,
            RSResultData::Intersection(_) => RSResultKind::Intersection,
            RSResultData::Term(_) => RSResultKind::Term,
            RSResultData::Virtual => RSResultKind::Virtual,
            RSResultData::Numeric(_) => RSResultKind::Numeric,
            RSResultData::Metric(_) => RSResultKind::Metric,
            RSResultData::HybridMetric(_) => RSResultKind::HybridMetric,
        }
    }

    /// Create an owned copy of this result data, allocating new memory for the contained data.
    ///
    /// The returned data may borrow the term from the original data.
    pub fn to_owned<'a>(&'a self) -> RSResultData<'a> {
        match self {
            Self::Union(agg) => RSResultData::Union(agg.to_owned()),
            Self::Intersection(agg) => RSResultData::Intersection(agg.to_owned()),
            Self::Term(term) => RSResultData::Term(term.to_owned()),
            Self::Virtual => RSResultData::Virtual,
            Self::Numeric(num) => RSResultData::Numeric(*num),
            Self::Metric(num) => RSResultData::Metric(*num),
            Self::HybridMetric(agg) => RSResultData::HybridMetric(agg.to_owned()),
        }
    }
}
