/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Score orderings for [`TopKHeap`](crate::TopKHeap).

use std::cmp::Ordering;

use crate::heap::ScoredResult;

/// Which score the top-k treats as better, and so which end of the range it
/// keeps.
///
/// Implementers are [`Clone`]; simple direction-only types are also [`Copy`]
/// and carry nothing but the direction, so [`compare`](Self::compare) inlines
/// into the heap's sift loops.
pub trait ScoreOrdering: Clone {
    /// Orders `a` against `b`, returning [`Ordering::Less`] when `a` is the
    /// better score.
    fn compare(&self, a: f64, b: f64) -> Ordering;

    /// Orders two [`ScoredResult`]s, returning [`Ordering::Less`] when `a` is
    /// better than `b`.
    ///
    /// The default compares scores via [`compare`](Self::compare) and breaks
    /// ties by ascending [`ScoredResult::doc_id`]. Multi-key orderings can
    /// override this to break score ties using additional sort keys and
    /// direction-aware document-id tie-breaking.
    #[inline]
    fn compare_results(&self, a: ScoredResult, b: ScoredResult) -> Ordering {
        match self.compare(a.score, b.score) {
            Ordering::Equal => a.doc_id.cmp(&b.doc_id),
            ord => ord,
        }
    }
}

/// Lower score is better, as for a vector distance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ascending;

impl ScoreOrdering for Ascending {
    #[inline]
    fn compare(&self, a: f64, b: f64) -> Ordering {
        a.total_cmp(&b)
    }
}

/// Higher score is better.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Descending;

impl ScoreOrdering for Descending {
    #[inline]
    fn compare(&self, a: f64, b: f64) -> Ordering {
        b.total_cmp(&a)
    }
}

/// A direction settled at run time, for queries whose sort order is not known
/// until the request is parsed.
///
/// Costs one well-predicted branch per comparison — [`Ascending`] and
/// [`Descending`] cost none — and still inlines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeOrder {
    ascending: bool,
}

impl RuntimeOrder {
    /// Orders ascending when `ascending` is set, descending otherwise.
    pub const fn new(ascending: bool) -> Self {
        Self { ascending }
    }
}

impl ScoreOrdering for RuntimeOrder {
    #[inline]
    fn compare(&self, a: f64, b: f64) -> Ordering {
        if self.ascending {
            a.total_cmp(&b)
        } else {
            b.total_cmp(&a)
        }
    }
}
