/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ref_mode::{Active, Ref};

use super::aggregate::RawAggregateResult;
use super::kind::RSResultKind;
use super::term_record::RawTermRecord;

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`super::kind::RSResultKindMask`]
///
/// The `R: Ref` parameter selects between [`Active<'index>`] mode (data references are valid for
/// `'index`) and [`ref_mode::Suspended`] mode (data references are inert raw pointers).
#[cheadergen::config(prefix_with_name)]
#[repr(u8)]
pub enum RawResultData<R: Ref> {
    Union(RawAggregateResult<R>) = 1,
    Intersection(RawAggregateResult<R>) = 2,
    Term(RawTermRecord<R>) = 4,
    Virtual = 8,
    Numeric(f64) = 16,
    Metric(f64) = 32,
    HybridMetric(RawAggregateResult<R>) = 64,
}

/// The [`Active`] instantiation of [`RawResultData`].
pub type RSResultData<'a> = RawResultData<Active<'a>>;

impl<'a> std::fmt::Debug for RSResultData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawResultData::Union(agg) => f.debug_tuple("Union").field(agg).finish(),
            RawResultData::Intersection(agg) => f.debug_tuple("Intersection").field(agg).finish(),
            RawResultData::Term(t) => f.debug_tuple("Term").field(t).finish(),
            RawResultData::Virtual => write!(f, "Virtual"),
            RawResultData::Numeric(n) => f.debug_tuple("Numeric").field(n).finish(),
            RawResultData::Metric(n) => f.debug_tuple("Metric").field(n).finish(),
            RawResultData::HybridMetric(agg) => f.debug_tuple("HybridMetric").field(agg).finish(),
        }
    }
}

impl<'a> PartialEq for RSResultData<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RawResultData::Union(a), RawResultData::Union(b)) => a == b,
            (RawResultData::Intersection(a), RawResultData::Intersection(b)) => a == b,
            (RawResultData::Term(a), RawResultData::Term(b)) => a == b,
            (RawResultData::Virtual, RawResultData::Virtual) => true,
            (RawResultData::Numeric(a), RawResultData::Numeric(b)) => a == b,
            (RawResultData::Metric(a), RawResultData::Metric(b)) => a == b,
            (RawResultData::HybridMetric(a), RawResultData::HybridMetric(b)) => a == b,
            _ => false,
        }
    }
}

impl<R: Ref> RawResultData<R> {
    pub const fn kind(&self) -> RSResultKind {
        match self {
            RawResultData::Union(_) => RSResultKind::Union,
            RawResultData::Intersection(_) => RSResultKind::Intersection,
            RawResultData::Term(_) => RSResultKind::Term,
            RawResultData::Virtual => RSResultKind::Virtual,
            RawResultData::Numeric(_) => RSResultKind::Numeric,
            RawResultData::Metric(_) => RSResultKind::Metric,
            RawResultData::HybridMetric(_) => RSResultKind::HybridMetric,
        }
    }
}

impl<'a> RSResultData<'a> {
    /// Create an owned copy of this result data, allocating new memory for the contained data.
    ///
    /// The returned data may borrow the term from the original data.
    pub fn to_owned(&'a self) -> RSResultData<'a> {
        match self {
            Self::Union(agg) => RawResultData::Union(agg.to_owned()),
            Self::Intersection(agg) => RawResultData::Intersection(agg.to_owned()),
            Self::Term(term) => RawResultData::Term(term.to_owned()),
            Self::Virtual => RawResultData::Virtual,
            Self::Numeric(num) => RawResultData::Numeric(*num),
            Self::Metric(num) => RawResultData::Metric(*num),
            Self::HybridMetric(agg) => RawResultData::HybridMetric(agg.to_owned()),
        }
    }
}
