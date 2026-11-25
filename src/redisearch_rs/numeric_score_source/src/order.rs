/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Multi-key and direction-aware tie-break ordering for [`NumericTopKIterator`](crate::NumericTopKIterator).

use std::cmp::Ordering;

use rqe_core::DocId;
use top_k::{ScoreOrdering, ScoredResult};

/// Breaks ties between two documents that share the same primary numeric sort
/// value (or both lack one), returning [`Ordering::Less`] when `a` ranks before
/// `b`.
pub trait TieBreakComparator: Clone {
    /// Compare `a` and `b` on the sort keys after the primary numeric key.
    fn cmp_tie_break(&self, a: DocId, b: DocId) -> Ordering;
}

/// No secondary sort keys: ties fall straight through to the document-id
/// tie-break.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NoTieBreak;

impl TieBreakComparator for NoTieBreak {
    #[inline(always)]
    fn cmp_tie_break(&self, _a: DocId, _b: DocId) -> Ordering {
        Ordering::Equal
    }
}

/// Orders `(doc_id, score)` results the way the pipeline sorter
/// (`SearchResult_CmpByFields`) ranks them:
///
/// 1. Finite numeric values rank before missing values (`f64::NAN`) in either
///    direction.
/// 2. Finite numeric values are ordered by `ascending`.
/// 3. Ties are broken by [`TieBreakComparator::cmp_tie_break`].
/// 4. Remaining ties are broken by [`DocId`] in `last_ascending`'s direction
///    (ascending doc ID when the last sort key is `ASC`, descending doc ID when
///    the last sort key is `DESC`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumericOrder<T: TieBreakComparator = NoTieBreak> {
    ascending: bool,
    last_ascending: bool,
    tie_break: T,
}

impl NumericOrder<NoTieBreak> {
    /// Single-key numeric ordering where the document-id tie-break follows
    /// `ascending`.
    pub const fn single_key(ascending: bool) -> Self {
        Self {
            ascending,
            last_ascending: ascending,
            tie_break: NoTieBreak,
        }
    }
}

impl<T: TieBreakComparator> NumericOrder<T> {
    /// Multi-key numeric ordering with a custom [`TieBreakComparator`] and
    /// explicit direction for the final document-id tie-break (`last_ascending`
    /// is the direction of the last sort key).
    pub const fn with_tie_break(ascending: bool, last_ascending: bool, tie_break: T) -> Self {
        Self {
            ascending,
            last_ascending,
            tie_break,
        }
    }
}

impl<T: TieBreakComparator> ScoreOrdering for NumericOrder<T> {
    #[inline]
    fn compare(&self, a: f64, b: f64) -> Ordering {
        match (a.is_nan(), b.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                if self.ascending {
                    a.total_cmp(&b)
                } else {
                    b.total_cmp(&a)
                }
            }
        }
    }

    #[inline]
    fn compare_results(&self, a: ScoredResult, b: ScoredResult) -> Ordering {
        let ord = self.compare(a.score, b.score);
        if ord != Ordering::Equal {
            return ord;
        }
        let tie = self.tie_break.cmp_tie_break(a.doc_id, b.doc_id);
        if tie != Ordering::Equal {
            return tie;
        }
        if self.last_ascending {
            a.doc_id.cmp(&b.doc_id)
        } else {
            b.doc_id.cmp(&a.doc_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, num::NonZeroUsize};

    use top_k::TopKHeap;

    use super::*;

    #[derive(Clone)]
    struct MapTieBreak {
        ranks: HashMap<DocId, i32>,
        asc: bool,
    }

    impl TieBreakComparator for MapTieBreak {
        fn cmp_tie_break(&self, a: DocId, b: DocId) -> Ordering {
            match (self.ranks.get(&a), self.ranks.get(&b)) {
                (None, None) => Ordering::Equal,
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (Some(va), Some(vb)) => {
                    let ord = va.cmp(vb);
                    if self.asc { ord } else { ord.reverse() }
                }
            }
        }
    }

    #[test]
    fn single_key_desc_breaks_ties_by_descending_doc_id() {
        let mut heap = TopKHeap::new(
            NonZeroUsize::new(2).unwrap(),
            NumericOrder::single_key(false),
        );
        for id in [1, 5, 3, 10] {
            heap.push(id, 42.0);
        }
        let ids: Vec<DocId> = heap
            .drain_sorted()
            .into_iter()
            .map(|r| r.scored.doc_id)
            .collect();
        assert_eq!(ids, vec![10, 5]);
    }

    #[test]
    fn missing_scores_rank_last_in_either_direction() {
        for asc in [true, false] {
            let mut heap =
                TopKHeap::new(NonZeroUsize::new(3).unwrap(), NumericOrder::single_key(asc));
            heap.push(1, f64::NAN);
            heap.push(2, 10.0);
            heap.push(3, f64::NAN);
            heap.push(4, 20.0);
            let ids: Vec<DocId> = heap
                .drain_sorted()
                .into_iter()
                .map(|r| r.scored.doc_id)
                .collect();
            let expected = if asc { vec![2, 4, 1] } else { vec![4, 2, 3] };
            assert_eq!(ids, expected, "asc={asc}");
        }
    }

    #[test]
    fn multi_key_tie_break_respects_direction_and_missing_values() {
        let tie = MapTieBreak {
            ranks: HashMap::from([(1, 20), (2, 10), (3, 20)]), // doc 4 missing
            asc: false,                                        // tie-break DESC -> doc_id DESC
        };
        let order = NumericOrder::with_tie_break(true, false, tie);
        let mut heap = TopKHeap::new(NonZeroUsize::new(4).unwrap(), order);
        for id in [4, 1, 2, 3] {
            heap.push(id, 5.0);
        }
        let ids: Vec<DocId> = heap
            .drain_sorted()
            .into_iter()
            .map(|r| r.scored.doc_id)
            .collect();
        // rank 20 (doc 3 > doc 1 because last key is DESC), then rank 10 (doc 2), then missing (doc 4).
        assert_eq!(ids, vec![3, 1, 2, 4]);
    }
}
