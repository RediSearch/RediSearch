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
pub enum RawResultData<'query, R: Ref> {
    Union(RawAggregateResult<'query, R>) = 1,
    Intersection(RawAggregateResult<'query, R>) = 2,
    Term(RawTermRecord<'query, R>) = 4,
    Virtual = 8,
    Numeric(f64) = 16,
    Metric(f64) = 32,
    HybridMetric(RawAggregateResult<'query, R>) = 64,
}

/// The [`Active`] instantiation of [`RawResultData`].
#[cheadergen::config(export)]
pub type RSResultData<'a> = RawResultData<'a, Active<'a>>;

// Compile-time proof that the `Active` and `Suspended` instantiations of
// `RawResultData` are layout-identical. Only `size_of`/`align_of` are checked:
// `offset_of!` cannot address `#[repr(u8)]` enum variant fields. The variant
// payloads that carry `R` are guarded one level deeper by the
// `RawAggregateResult` (`aggregate.rs`) and `RawTermRecord` (`term_record.rs`)
// blocks. Part of the recursive net backing the conversions on
// `RawIndexResult` (see `core/mod.rs`).
const _: () = {
    use ref_mode::Suspended;
    use std::mem::{align_of, size_of};
    type A = RawResultData<'static, Active<'static>>;
    type S = RawResultData<'static, Suspended>;
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

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

impl<'query, R: Ref> RawResultData<'query, R> {
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
