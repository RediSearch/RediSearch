/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Proximity checks: test whether matched term positions in an aggregate result
//! satisfy `max_slop` / `in_order` constraints.

use std::io::Cursor;

use super::super::kind::{RSResultKind, RSResultKindMask};
use super::super::result_data::RSResultData;
use super::RSIndexResult;

/// A lazy iterator over the term-position offsets stored inside an [`RSIndexResult`].
///
/// - [`OffsetIter::Empty`]: EOF immediately (virtual / numeric / metric results).
/// - [`OffsetIter::Term`]: reads varint delta-encoded `u32` positions from the raw bytes.
/// - [`OffsetIter::Merge`]: k-way merge of child iterators (used when a child of the
///   intersection is itself a union, e.g. for stemmed or synonym expansions).
enum OffsetIter<'a> {
    Empty,
    Term {
        /// Reader over the raw varint-encoded offset bytes for this term.
        cursor: Cursor<&'a [u8]>,
        /// Accumulated position; each varint read is a delta added to this.
        last: u32,
    },
    Merge {
        /// One iterator per child result (e.g. each variant of a union/synonym expansion).
        children: Vec<OffsetIter<'a>>,
        /// One look-ahead position per child: `Some(pos)` = next unconsumed
        /// offset from that child, `None` = child has reached EOF.
        positions: Vec<Option<u32>>,
    },
}

impl OffsetIter<'_> {
    /// Returns the next position in ascending order, or `None` at EOF.
    fn next_offset(&mut self) -> Option<u32> {
        match self {
            OffsetIter::Empty => None,
            OffsetIter::Term { cursor, last } => {
                // Each stored value is a delta; accumulate to recover the absolute position.
                let delta: u32 = varint::read(cursor).ok()?;
                *last = last.wrapping_add(delta);
                Some(*last)
            }
            OffsetIter::Merge {
                children,
                positions,
            } => {
                // Find the child whose look-ahead position is smallest.
                let (min_idx, min_val) = positions
                    .iter()
                    .enumerate()
                    .filter_map(|(i, opt)| opt.map(|v| (i, v)))
                    .min_by_key(|&(_, v)| v)?;
                // Advance that child and refresh the look-ahead slot.
                positions[min_idx] = children[min_idx].next_offset();
                Some(min_val)
            }
        }
    }
}

/// Returns `true` if `result` contributes meaningful term-position offsets.
fn has_offsets(result: &RSIndexResult<'_>) -> bool {
    match &result.data {
        RSResultData::Term(rec) => !rec.offsets().is_empty(),
        RSResultData::Intersection(agg) | RSResultData::Union(agg) => {
            // Skip aggregates that consist only of virtual or purely numeric
            // (Numeric | Metric) results, as neither carries offset data.
            let mask = agg.kind_mask();
            let virtual_only: RSResultKindMask = RSResultKind::Virtual.into();
            let numeric_only: RSResultKindMask = RSResultKind::Numeric | RSResultKind::Metric;
            mask != virtual_only && mask != numeric_only
        }
        RSResultData::Virtual
        | RSResultData::Numeric(_)
        | RSResultData::Metric(_)
        | RSResultData::HybridMetric(_) => false,
    }
}

/// Creates an [`OffsetIter`] that yields every position recorded for `result`.
///
/// - Term → varint delta decoder over the raw offset bytes.
/// - Intersection / Union with 1 child → recurse into that child directly.
/// - Intersection / Union with N children → k-way merge of child iterators.
/// - Everything else → [`OffsetIter::Empty`].
fn iterate_offsets<'a>(result: &'a RSIndexResult<'_>) -> OffsetIter<'a> {
    match &result.data {
        RSResultData::Term(rec) => OffsetIter::Term {
            cursor: Cursor::new(rec.offsets()),
            last: 0,
        },
        RSResultData::Virtual
        | RSResultData::Numeric(_)
        | RSResultData::Metric(_)
        | RSResultData::HybridMetric(_) => OffsetIter::Empty,
        RSResultData::Intersection(agg) | RSResultData::Union(agg) => {
            let n = agg.len();
            if n == 1 {
                // optimisation: single child → delegate directly.
                return match agg.get(0) {
                    Some(child) => iterate_offsets(child),
                    None => OffsetIter::Empty,
                };
            }
            // Eagerly advance each child iterator so the first offset is ready
            // before any comparison begins.
            let mut children: Vec<OffsetIter<'a>> = (0..n)
                .filter_map(|i| agg.get(i))
                .map(iterate_offsets)
                .collect();
            let positions: Vec<Option<u32>> =
                children.iter_mut().map(|c| c.next_offset()).collect();
            OffsetIter::Merge {
                children,
                positions,
            }
        }
    }
}

/// Checks whether all `n` offset streams contain positions that appear in the same
/// relative order as the child iterators, with no more than `max_slop` non-matching
/// token slots between consecutive terms.
fn within_range_in_order(iters: &mut [OffsetIter<'_>], max_slop: u32) -> bool {
    let n = iters.len();
    // `positions[i]` holds the most recently read position for child `i` (or 0 initially).
    // Child 0 is always re-advanced at the start of each outer iteration.
    let mut positions = vec![0u32; n];

    loop {
        let mut span: i32 = 0;
        let mut over_slop = false;

        for i in 0..n {
            // Always advance child 0; reuse the stored position for the others.
            let mut pos = if i == 0 {
                match iters[0].next_offset() {
                    Some(p) => p,
                    None => return false,
                }
            } else {
                positions[i]
            };
            let last_pos = if i == 0 { 0u32 } else { positions[i - 1] };

            // Advance child i until its position is ≥ last_pos (enforce ordering).
            while pos < last_pos {
                match iters[i].next_offset() {
                    Some(p) => pos = p,
                    None => return false,
                }
            }
            positions[i] = pos;

            if i > 0 {
                span += (pos as i32) - (last_pos as i32) - 1;
                // A negative span means terms are densely packed — never over slop.
                if span > 0 && span as u32 > max_slop {
                    over_slop = true;
                    break;
                }
            }
        }

        if !over_slop {
            return true;
        }
        // span > max_slop — advance child 0 further in the next outer iteration.
    }
}

/// Checks whether all `n` offset streams contain positions within `max_slop` of each
/// other (in any order).
fn within_range_unordered(iters: &mut [OffsetIter<'_>], max_slop: u32) -> bool {
    let n = iters.len();

    // Prime: read the first position from each iterator.
    // If any iterator starts at EOF, no within-range match is possible.
    let Some(mut positions): Option<Vec<u32>> =
        iters.iter_mut().map(|it| it.next_offset()).collect()
    else {
        return false;
    };

    let (mut max_pos, _) = array_max(&positions);

    loop {
        let (min_pos, min_idx) = array_min(&positions);

        if min_pos != max_pos {
            // span = max - min - (num_terms - 1): the number of non-matched slots.
            // Can be negative when terms overlap; a negative span is always within range.
            let span = (max_pos as i32) - (min_pos as i32) - (n as i32 - 1);
            if span < 0 || span as u32 <= max_slop {
                return true;
            }
        }

        // Advance the iterator at the minimum position.
        let Some(new_pos) = iters[min_idx].next_offset() else {
            break; // One iterator reached EOF; no more candidates.
        };
        positions[min_idx] = new_pos;
        if new_pos > max_pos {
            max_pos = new_pos;
        }
    }

    false
}

/// Returns `(min_value, min_index)`. Returns `(u32::MAX, 0)` on an empty slice.
#[inline]
fn array_min(arr: &[u32]) -> (u32, usize) {
    arr.iter()
        .enumerate()
        .min_by_key(|(_, v)| *v)
        .map(|(i, v)| (*v, i))
        .unwrap_or((u32::MAX, 0))
}

/// Returns `(max_value, max_index)`. On equal values the last index wins.
#[inline]
fn array_max(arr: &[u32]) -> (u32, usize) {
    arr.iter()
        .enumerate()
        .fold((0u32, 0usize), |(max_v, max_i), (i, &v)| {
            if v >= max_v { (v, i) } else { (max_v, max_i) }
        })
}

/// Returns `true` when the term positions recorded in `ir` satisfy the given
/// proximity constraints.
///
/// # Parameters
///
/// - `max_slop`: maximum allowed number of non-matched token slots between
///   consecutive terms.  `None` disables the slop check (any gap is permitted).
/// - `in_order`: when `true`, terms must appear in the same order as the child
///   iterators.
///
/// Returns `true` when `ir` is not an aggregate, has ≤ 1 child, or ≤ 1 child
/// has meaningful offsets — all degenerate cases where the constraint is
/// trivially satisfied.
///
/// # Preconditions
///
/// At least one of `max_slop` or `in_order` must impose a constraint:
/// `max_slop.is_some() || in_order` must hold.  If neither is set, the result
/// is trivially `true` for every input and the call is pointless; callers are
/// expected to short-circuit that case before invoking this function.
pub(super) fn is_within_range<'a>(
    ir: &'a RSIndexResult<'_>,
    max_slop: Option<u32>,
    in_order: bool,
) -> bool {
    debug_assert!(
        max_slop.is_some() || in_order,
        "is_within_range called with no slop constraint and no ordering requirement; \
        callers should short-circuit this case as it trivially returns `true`"
    );

    let agg = match &ir.data {
        RSResultData::Intersection(agg) | RSResultData::Union(agg) => agg,
        _ => return true,
    };

    if agg.len() <= 1 {
        return true;
    }

    let mut iters: Vec<OffsetIter<'a>> = (0..agg.len())
        .filter_map(|i| agg.get(i))
        .filter(|child| has_offsets(child))
        .map(iterate_offsets)
        .collect();

    if iters.len() <= 1 {
        return true;
    }

    let max_slop = max_slop.unwrap_or(u32::MAX);

    if in_order {
        within_range_in_order(&mut iters, max_slop)
    } else {
        within_range_unordered(&mut iters, max_slop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Offset-iterator helpers ───────────────────────────────────────────────

    /// Build a `OffsetIter::Term` backed by a `'static` byte slice.
    ///
    /// The caller supplies the raw varint-delta bytes directly.
    fn static_term_iter(bytes: &'static [u8]) -> OffsetIter<'static> {
        OffsetIter::Term {
            cursor: Cursor::new(bytes),
            last: 0,
        }
    }

    // Since all values < 128, varint bytes equal the delta values.
    static VW1_BYTES: [u8; 5] = [1, 8, 4, 3, 6];
    static VW2_BYTES: [u8; 3] = [4, 3, 25];

    fn make_vw_iters() -> [OffsetIter<'static>; 2] {
        [static_term_iter(&VW1_BYTES), static_term_iter(&VW2_BYTES)]
    }

    // ── within_range_in_order ─────────────────────────────────────────────────

    #[test]
    fn in_order() {
        // slop=0: no window of size 1+0 exists  → false
        assert!(!within_range_in_order(&mut make_vw_iters(), 0));
        // slop=1: need gap of ≤1 → false
        assert!(!within_range_in_order(&mut make_vw_iters(), 1));
        // slop=2: {1,4} gap=2 → true
        assert!(within_range_in_order(&mut make_vw_iters(), 2));
        assert!(within_range_in_order(&mut make_vw_iters(), 3));
        assert!(within_range_in_order(&mut make_vw_iters(), 4));
        assert!(within_range_in_order(&mut make_vw_iters(), 5));
    }

    #[test]
    fn in_order_exact_consecutive() {
        // span = 4-3-1 = 0 ≤ 0 → true at slop 0
        static A: [u8; 1] = [3];
        static B: [u8; 1] = [4];
        let mut iters = [static_term_iter(&A), static_term_iter(&B)];
        assert!(within_range_in_order(&mut iters, 0));
    }

    #[test]
    fn in_order_out_of_order_terms() {
        // iter0 is at position 10, iter1 is at position 5
        // iter1 must advance past 10, but it's already at EOF after 5 → false
        static A: [u8; 1] = [10]; // pos 10
        static B: [u8; 1] = [5]; // pos 5 — behind 10, no more data → EOF
        let mut iters = [static_term_iter(&A), static_term_iter(&B)];
        assert!(!within_range_in_order(&mut iters, 100));
    }

    #[test]
    fn in_order_empty_first_iter() {
        static EMPTY: [u8; 0] = [];
        static B: [u8; 1] = [5];
        let mut iters = [static_term_iter(&EMPTY), static_term_iter(&B)];
        assert!(!within_range_in_order(&mut iters, 100));
    }

    // ── within_range_unordered ────────────────────────────────────────────────

    /// slop=1 returns true because the pair (vw1=9, vw2=7) has span = 9-7-1 = 1 ≤ 1.
    #[test]
    fn unordered() {
        assert!(!within_range_unordered(&mut make_vw_iters(), 0));
        assert!(within_range_unordered(&mut make_vw_iters(), 1));
        assert!(within_range_unordered(&mut make_vw_iters(), 2));
        assert!(within_range_unordered(&mut make_vw_iters(), 3));
        assert!(within_range_unordered(&mut make_vw_iters(), 4));
    }

    #[test]
    fn unordered_reversed_order_ok() {
        // iter0 pos 10, iter1 pos 5 → span = 10-5-1 = 4
        static A: [u8; 1] = [10];
        static B: [u8; 1] = [5];
        let mut iters = [static_term_iter(&A), static_term_iter(&B)];
        assert!(!within_range_unordered(&mut iters, 3));
        let mut iters = [static_term_iter(&A), static_term_iter(&B)];
        assert!(within_range_unordered(&mut iters, 4));
    }

    #[test]
    fn unordered_empty_iter_returns_false() {
        static EMPTY: [u8; 0] = [];
        static B: [u8; 1] = [5];
        let mut iters = [static_term_iter(&EMPTY), static_term_iter(&B)];
        assert!(!within_range_unordered(&mut iters, 100));
    }

    // ── OffsetIter::Merge ─────────────────────────────────────────────────────

    fn make_merge(children: Vec<OffsetIter<'static>>) -> OffsetIter<'static> {
        let mut children = children;
        let positions: Vec<Option<u32>> = children.iter_mut().map(|c| c.next_offset()).collect();
        OffsetIter::Merge {
            children,
            positions,
        }
    }

    #[test]
    fn merge_two_children_yields_sorted_order() {
        // child0: deltas [2,3,4] → positions [2, 5, 9]
        // child1: deltas [1,3,3] → positions [1, 4, 7]
        // expected k-way merge:           1, 2, 4, 5, 7, 9
        static C0: [u8; 3] = [2, 3, 4];
        static C1: [u8; 3] = [1, 3, 3];
        let mut merge = make_merge(vec![static_term_iter(&C0), static_term_iter(&C1)]);

        assert_eq!(merge.next_offset(), Some(1));
        assert_eq!(merge.next_offset(), Some(2));
        assert_eq!(merge.next_offset(), Some(4));
        assert_eq!(merge.next_offset(), Some(5));
        assert_eq!(merge.next_offset(), Some(7));
        assert_eq!(merge.next_offset(), Some(9));
        assert_eq!(merge.next_offset(), None);
    }

    #[test]
    fn merge_one_child_exhausts_early() {
        // child0 ends after position 3; child1 still has positions [6, 10]
        static C0: [u8; 1] = [3];
        static C1: [u8; 2] = [6, 4]; // deltas → positions 6, 10
        let mut merge = make_merge(vec![static_term_iter(&C0), static_term_iter(&C1)]);

        assert_eq!(merge.next_offset(), Some(3));
        assert_eq!(merge.next_offset(), Some(6));
        assert_eq!(merge.next_offset(), Some(10));
        assert_eq!(merge.next_offset(), None);
    }

    #[test]
    fn merge_three_children_yields_sorted_order() {
        // child0: deltas [5]     → positions [5]
        // child1: deltas [2, 6]  → positions [2, 8]
        // child2: deltas [1, 3]  → positions [1, 4]
        // expected merge: 1, 2, 4, 5, 8
        static C0: [u8; 1] = [5];
        static C1: [u8; 2] = [2, 6];
        static C2: [u8; 2] = [1, 3];
        let mut merge = make_merge(vec![
            static_term_iter(&C0),
            static_term_iter(&C1),
            static_term_iter(&C2),
        ]);

        assert_eq!(merge.next_offset(), Some(1));
        assert_eq!(merge.next_offset(), Some(2));
        assert_eq!(merge.next_offset(), Some(4));
        assert_eq!(merge.next_offset(), Some(5));
        assert_eq!(merge.next_offset(), Some(8));
        assert_eq!(merge.next_offset(), None);
    }

    #[test]
    fn merge_all_children_empty_returns_none() {
        static EMPTY: [u8; 0] = [];
        let mut merge = make_merge(vec![static_term_iter(&EMPTY), static_term_iter(&EMPTY)]);
        assert_eq!(merge.next_offset(), None);
    }

    // ── iterate_offsets: single-child shortcut ────────────────────────────────

    #[test]
    fn single_child_union_delegates_to_child_iter() {
        use crate::{RSAggregateResult, RSIndexResult, RSOffsetSlice, RSResultData};
        use std::ptr;

        // delta bytes [2, 3, 5] → cumulative positions [2, 5, 10]
        static OFFSETS: [u8; 3] = [2, 3, 5];

        let term = RSIndexResult::build_term()
            .borrowed_record(None, RSOffsetSlice::from_slice(&OFFSETS))
            .build();

        let mut agg = RSAggregateResult::owned_with_capacity(1);
        agg.push_boxed(Box::new(term));

        // Construct the Union directly; `data` is private to `core` but
        // visible here since this test module is nested within `core`.
        let union_ir = RSIndexResult {
            doc_id: 0,
            dmd: ptr::null(),
            field_mask: 0,
            freq: 0,
            data: RSResultData::Union(agg),
            metrics: ptr::null_mut(),
            weight: 0.0,
        };

        // With n == 1, iterate_offsets delegates directly to the child,
        // so positions must match OFFSETS decoded as varint deltas.
        let mut it = iterate_offsets(&union_ir);
        assert_eq!(it.next_offset(), Some(2));
        assert_eq!(it.next_offset(), Some(5));
        assert_eq!(it.next_offset(), Some(10));
        assert_eq!(it.next_offset(), None);
    }
}
