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
//!
//! This is the Rust equivalent of `IndexResult_IsWithinRange` and its helpers
//! from `src/index_result/index_result.c`.

use std::io::Cursor;

use super::kind::RSResultKind;
use super::result_data::RSResultData;
use crate::RSIndexResult;

// ─────────────────────────────────────────────────────────────────────────────
// Offset iterator
// ─────────────────────────────────────────────────────────────────────────────

/// A lazy iterator over the term-position offsets stored inside an [`RSIndexResult`].
///
/// - [`OffsetIter::Empty`]: EOF immediately (virtual / numeric / metric results).
/// - [`OffsetIter::Term`]: reads varint delta-encoded `u32` positions from the raw bytes.
/// - [`OffsetIter::Merge`]: k-way merge of child iterators (used when a child of the
///   intersection is itself a union, e.g. for stemmed or synonym expansions).
enum OffsetIter<'a> {
    Empty,
    Term {
        cursor: Cursor<&'a [u8]>,
        /// Accumulated position; each varint read is a delta added to this.
        last: u32,
    },
    Merge {
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

// ─────────────────────────────────────────────────────────────────────────────
// Building offset iterators from index results
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `true` if `result` contributes meaningful term-position offsets.
///
/// Mirrors `RSIndexResult_HasOffsets` from `src/index_result/index_result.c`.
fn has_offsets(result: &RSIndexResult<'_>) -> bool {
    match &result.data {
        RSResultData::Term(rec) => !rec.offsets().is_empty(),
        RSResultData::Intersection(agg) | RSResultData::Union(agg) => {
            // Mirrors the C check:
            //   `mask != RSResultData_Virtual && mask != RS_RESULT_NUMERIC`
            // where RS_RESULT_NUMERIC = Numeric | Metric.
            let mask = agg.kind_mask();
            let virtual_only: super::kind::RSResultKindMask = RSResultKind::Virtual.into();
            let numeric_only: super::kind::RSResultKindMask =
                RSResultKind::Numeric | RSResultKind::Metric;
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
/// Mirrors `RSIndexResult_IterateOffsets` from `src/offset_vector.c`:
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
                // Mirror the C optimisation: single child → delegate directly.
                return match agg.get(0) {
                    Some(child) => iterate_offsets(child),
                    None => OffsetIter::Empty,
                };
            }
            // Eagerly prime the look-ahead for each child (mirrors `_aggregateResult_iterate`
            // in offset_vector.c which calls `.Next()` once per child on construction).
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

// ─────────────────────────────────────────────────────────────────────────────
// Within-range algorithms
// ─────────────────────────────────────────────────────────────────────────────

/// Checks whether all `n` offset streams contain positions that appear in the same
/// relative order as the child iterators, with no more than `max_slop` non-matching
/// token slots between consecutive terms.
///
/// Direct port of `__indexResult_withinRangeInOrder` from
/// `src/index_result/index_result.c`.
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
///
/// Direct port of `__indexResult_withinRangeUnordered` from
/// `src/index_result/index_result.c`.
fn within_range_unordered(iters: &mut [OffsetIter<'_>], max_slop: u32) -> bool {
    let n = iters.len();

    // Prime: read the first position from each iterator.
    // `u32::MAX` is the EOF sentinel (mirrors `RS_OFFSETVECTOR_EOF = UINT32_MAX`).
    let mut positions: Vec<u32> = iters
        .iter_mut()
        .map(|it| it.next_offset().unwrap_or(u32::MAX))
        .collect();

    // If any iterator starts at EOF, no within-range match is possible.
    if positions.contains(&u32::MAX) {
        return false;
    }

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
        let new_pos = iters[min_idx].next_offset().unwrap_or(u32::MAX);
        positions[min_idx] = new_pos;
        if new_pos == u32::MAX {
            break; // One iterator reached EOF; no more candidates.
        }
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

/// Returns `(max_value, max_index)`. On equal values the last index wins (mirrors C).
#[inline]
fn array_max(arr: &[u32]) -> (u32, usize) {
    arr.iter()
        .enumerate()
        .fold((0u32, 0usize), |(max_v, max_i), (i, &v)| {
            if v >= max_v { (v, i) } else { (max_v, max_i) }
        })
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Returns `true` when the term positions recorded in `ir` satisfy the given
/// proximity constraints.
///
/// This is the Rust port of `IndexResult_IsWithinRange` from
/// `src/index_result/index_result.c`.
///
/// # Parameters
///
/// - `max_slop`: maximum allowed number of non-matched token slots between
///   consecutive terms.  `None` disables the check entirely (equivalent to
///   passing `u32::MAX`, matching the C constructor normalisation).
/// - `in_order`: when `true`, terms must appear in the same order as the child
///   iterators.
///
/// Returns `true` when `ir` is not an aggregate, has ≤ 1 child, or ≤ 1 child
/// has meaningful offsets — all degenerate cases where the constraint is
/// trivially satisfied.
pub fn is_within_range<'a>(
    ir: &'a RSIndexResult<'_>,
    max_slop: Option<u32>,
    in_order: bool,
) -> bool {
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
    use crate::{RSIndexResult, RSOffsetSlice};

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

    // For the C++ test mirror:
    //   vw1 = {1, 9, 13, 16, 22} → deltas [1, 8, 4, 3, 6]
    //   vw2 = {4, 7, 32}          → deltas [4, 3, 25]
    // Since all values < 128, varint bytes equal the delta values.
    static VW1_BYTES: [u8; 5] = [1, 8, 4, 3, 6];
    static VW2_BYTES: [u8; 3] = [4, 3, 25];

    fn make_vw_iters() -> [OffsetIter<'static>; 2] {
        [static_term_iter(&VW1_BYTES), static_term_iter(&VW2_BYTES)]
    }

    // ── within_range_in_order ─────────────────────────────────────────────────

    /// Mirrors the C++ `testDistance` assertions for in-order checks.
    #[test]
    fn in_order_mirrors_cpp_test() {
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
        // positions: [3], [4] → span = 4-3-1 = 0 ≤ 0 → true at slop 0
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

    /// Mirrors the C++ `testDistance` assertions for unordered checks.
    ///
    /// slop=1 returns true because the pair (vw1=9, vw2=7) has span = 9-7-1 = 1 ≤ 1.
    #[test]
    fn unordered_mirrors_cpp_test() {
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

    // ── is_within_range — trivial paths ──────────────────────────────────────

    #[test]
    fn non_aggregate_always_true() {
        // A term result (not an aggregate) → trivially within range.
        static BYTES: [u8; 1] = [5];
        let ir = RSIndexResult::build_term()
            .borrowed_record(None, RSOffsetSlice::from_slice(&BYTES))
            .doc_id(1)
            .build();
        assert!(is_within_range(&ir, Some(0), false));
        assert!(is_within_range(&ir, Some(0), true));
    }

    #[test]
    fn single_child_aggregate_always_true() {
        // An intersection with a single numeric child — no proximity check needed.
        let child = RSIndexResult::build_numeric(1.0).doc_id(1).build();
        let mut ir = RSIndexResult::build_intersect(1).build();
        ir.push_borrowed(&child);
        assert!(is_within_range(&ir, Some(0), false));
        assert!(is_within_range(&ir, Some(0), true));
    }

    #[test]
    fn no_constraint_always_true() {
        // max_slop = None → always true regardless of positions.
        let child1 = RSIndexResult::build_numeric(1.0).doc_id(1).build();
        let child2 = RSIndexResult::build_numeric(2.0).doc_id(1).build();
        let mut ir = RSIndexResult::build_intersect(2).build();
        ir.push_borrowed(&child1);
        ir.push_borrowed(&child2);
        // Numeric children have no offsets → trivially true.
        assert!(is_within_range(&ir, None, false));
        assert!(is_within_range(&ir, None, true));
    }

    #[test]
    fn purely_numeric_children_always_true() {
        // An intersection of two numeric results has no offsets → trivially within range.
        let child1 = RSIndexResult::build_numeric(1.0).doc_id(1).build();
        let child2 = RSIndexResult::build_numeric(2.0).doc_id(1).build();
        let mut ir = RSIndexResult::build_intersect(2).build();
        ir.push_borrowed(&child1);
        ir.push_borrowed(&child2);
        assert!(is_within_range(&ir, Some(0), false));
        assert!(is_within_range(&ir, Some(0), true));
    }

    // ── is_within_range — full integration via 'static term results ──────────
    /// Mirrors the C++ `testDistance` test using the full `is_within_range` entry point.
    ///
    /// vw1 = {1, 9, 13, 16, 22}, vw2 = {4, 7, 32}
    #[test]
    fn full_test_mirrors_cpp_testdistance() {
        let t1: RSIndexResult<'static> = RSIndexResult::build_term()
            .borrowed_record(None, RSOffsetSlice::from_slice(&VW1_BYTES))
            .doc_id(1)
            .build();
        let t2: RSIndexResult<'static> = RSIndexResult::build_term()
            .borrowed_record(None, RSOffsetSlice::from_slice(&VW2_BYTES))
            .doc_id(1)
            .build();

        let mut ir = RSIndexResult::build_intersect(2).build();
        ir.push_borrowed(&t1);
        ir.push_borrowed(&t2);

        // Unordered: slop=1 is true because (vw1=9, vw2=7) has span=1.
        assert!(!is_within_range(&ir, Some(0), false));
        assert!(is_within_range(&ir, Some(1), false));
        assert!(is_within_range(&ir, Some(2), false));
        assert!(is_within_range(&ir, Some(3), false));
        assert!(is_within_range(&ir, Some(4), false));

        // In-order:
        assert!(!is_within_range(&ir, Some(0), true));
        assert!(!is_within_range(&ir, Some(1), true));
        assert!(is_within_range(&ir, Some(2), true));
        assert!(is_within_range(&ir, Some(3), true));
        assert!(is_within_range(&ir, Some(4), true));
        assert!(is_within_range(&ir, Some(5), true));
    }
}
