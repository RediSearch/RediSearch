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
#[derive(Debug)]
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
#[cheadergen::config(export)]
pub type RSResultData<'a> = RawResultData<Active<'a>>;

impl<'a> PartialEq for RSResultData<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Union(a), Self::Union(b)) => a == b,
            (Self::Intersection(a), Self::Intersection(b)) => a == b,
            (Self::Term(a), Self::Term(b)) => a == b,
            (Self::Virtual, Self::Virtual) => true,
            (Self::Numeric(a), Self::Numeric(b)) => a == b,
            (Self::Metric(a), Self::Metric(b)) => a == b,
            (Self::HybridMetric(a), Self::HybridMetric(b)) => a == b,
            _ => false,
        }
    }
}

impl<R: Ref> RawResultData<R> {
    pub const fn kind(&self) -> RSResultKind {
        match self {
            Self::Union(_) => RSResultKind::Union,
            Self::Intersection(_) => RSResultKind::Intersection,
            Self::Term(_) => RSResultKind::Term,
            Self::Virtual => RSResultKind::Virtual,
            Self::Numeric(_) => RSResultKind::Numeric,
            Self::Metric(_) => RSResultKind::Metric,
            Self::HybridMetric(_) => RSResultKind::HybridMetric,
        }
    }
}

impl<'a> RSResultData<'a> {
    /// Create an owned copy of this result data, allocating new memory for the contained data.
    ///
    /// The returned data may borrow the term from the original data.
    pub fn to_owned(&'a self) -> RSResultData<'a> {
        match self {
            Self::Union(agg) => Self::Union(agg.to_owned()),
            Self::Intersection(agg) => Self::Intersection(agg.to_owned()),
            Self::Term(term) => Self::Term(term.to_owned()),
            Self::Virtual => Self::Virtual,
            Self::Numeric(num) => Self::Numeric(*num),
            Self::Metric(num) => Self::Metric(*num),
            Self::HybridMetric(agg) => Self::HybridMetric(agg.to_owned()),
        }
    }
}
