/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Window skipping in [`RQEIterator::read_with_threshold`] of the composites, driven over
//! [`Blocked`] leaves.

use std::{cell::RefCell, rc::Rc};

use proptest::prelude::*;
use query_types::QueryNodeType;
use rqe_core::DocId;
use rqe_iterators::{
    Empty, Intersection, RQEIterator, UnionFullFlat, UnionFullHeap, UnionQuickFlat, UnionQuickHeap,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    score_bound::{BlockBound, MinCompetitiveScore, can_drop},
    union_opaque::build_union,
};

use crate::utils::blocked::{Block, Blocked, BlockedLog, Tree, drain_above, leaf_spec, tree};

type Log = Rc<RefCell<BlockedLog>>;
type DynIterator = Box<dyn RQEIterator<'static>>;

/// The threshold the score sorter passes down for heap minimum `theta`: the minimum itself.
fn min_competitive(theta: f64) -> MinCompetitiveScore {
    MinCompetitiveScore::new(theta).expect("a positive heap minimum")
}

fn leaf(blocks: Vec<Block>) -> (Blocked<'static>, Log) {
    let leaf = Blocked::new(blocks);
    let log = leaf.log();
    (leaf, log)
}

fn visited(log: &Log) -> Vec<DocId> {
    log.borrow().visited.clone()
}

/// The documents a plain [`RQEIterator::read`] yields whose bound reaches
/// `min_competitive_score`: with exact leaf bounds, the ones a thresholded read must keep.
fn plain_survivors<'index>(
    it: &mut impl RQEIterator<'index>,
    min_competitive_score: MinCompetitiveScore,
) -> Vec<DocId> {
    let mut docs = Vec::new();
    while let Some(doc) = it.read().expect("read failed").map(|r| r.doc_id) {
        if !can_drop(it.current_max_score(), min_competitive_score) {
            docs.push(doc);
        }
    }
    docs
}

fn ids(range: std::ops::RangeInclusive<DocId>) -> Vec<DocId> {
    range.collect()
}

/****************************** intersection ******************************/

/// `a` has a weak block then a strong one; `b` one weak block over both.
fn and_children() -> [(Blocked<'static>, Log); 2] {
    [
        leaf(vec![
            Block::uniform(1..=10, 1.0),
            Block::uniform(11..=20, 3.0),
        ]),
        leaf(vec![Block::uniform(1..=20, 1.0)]),
    ]
}

#[test]
fn intersection_skips_a_window_whose_block_sum_is_below_the_threshold() {
    let [(a, a_log), (b, _)] = and_children();
    let mut it = Intersection::new(vec![a, b], 1.0, false);

    assert_eq!(drain_above(&mut it, min_competitive(3.0)), ids(11..=20));
    // The first two documents are read before the window is known; the rest of the weak block
    // is jumped.
    let mut expected = vec![1, 2];
    expected.extend(11..=20);
    assert_eq!(visited(&a_log), expected);
    assert_eq!(a_log.borrow().skip_tos, 1);
}

#[test]
fn intersection_without_a_block_boundary_visits_every_document() {
    let [(a, a_log), (b, _)] = and_children();
    let mut it = Intersection::new(vec![a, b.without_block_bounds()], 1.0, false);

    assert_eq!(drain_above(&mut it, min_competitive(3.0)), ids(11..=20));
    assert_eq!(visited(&a_log), ids(1..=20));
    assert_eq!(a_log.borrow().skip_tos, 0);
    // One read per document and one that finds the end.
    assert_eq!(a_log.borrow().reads, 21);
}

#[test]
fn intersection_follows_misaligned_block_boundaries() {
    let (a, a_log) = leaf(vec![
        Block::uniform(1..=4, 1.0),
        Block::uniform(5..=12, 3.0),
    ]);
    let (b, b_log) = leaf(vec![
        Block::uniform(1..=8, 1.0),
        Block::uniform(9..=12, 3.0),
    ]);
    let mut it = Intersection::new(vec![a, b], 1.0, false);

    // Every window ends at the earlier of the two block ends: [2, 4] (sum 2), then [5, 8]
    // (sum 4), both below 5.
    assert_eq!(drain_above(&mut it, min_competitive(5.0)), ids(9..=12));
    assert_eq!(visited(&a_log), [1, 2, 5, 9, 10, 11, 12]);
    assert_eq!(visited(&b_log), [1, 9, 10, 11, 12]);
}

#[test]
fn intersection_drops_agreed_documents_below_the_threshold() {
    let docs: Vec<_> = (1..=6).map(|id| (id, id as f64)).collect();
    let (a, _) = leaf(vec![Block::tight(&docs)]);
    let (b, _) = leaf(vec![Block::uniform(1..=6, 0.0)]);
    let mut it = Intersection::new(vec![a, b], 1.0, false);

    assert_eq!(drain_above(&mut it, min_competitive(4.0)), [4, 5, 6]);
}

#[test]
fn intersection_passes_over_a_target_its_aligned_children_rule_out() {
    // Together the blocks reach 4.0, so no window is skipped; but at 4 and 8 the lead `a` scores
    // only 1.0, and with `b`'s block maximum the target cannot reach 3.5.
    let (a, _) = leaf(vec![Block::tight(&[
        (2, 1.0),
        (4, 1.0),
        (6, 3.0),
        (8, 1.0),
    ])]);
    let (b, b_log) = leaf(vec![Block::uniform(1..=10, 1.0)]);
    let mut it = Intersection::new(vec![a, b], 1.0, false);

    assert_eq!(drain_above(&mut it, min_competitive(3.5)), [6]);
    // `b` is moved onto 2, before it stands in a block, and onto 6; never onto 4 or 8.
    assert_eq!(visited(&b_log), [2, 6]);
    assert_eq!(b_log.borrow().skip_tos, 2);
}

/// An exact-phrase intersection (`max_slop = 0`, `in_order`) over `a` then `b`, each a single
/// block of `docs` with the matching scores and term positions.
fn phrase(
    a: (&[(DocId, f64)], &[u8]),
    b: (&[(DocId, f64)], &[u8]),
) -> Intersection<'static, Blocked<'static>> {
    let a = Blocked::new(vec![Block::tight(a.0)]).with_positions(a.1);
    let b = Blocked::new(vec![Block::tight(b.0)]).with_positions(b.1);
    Intersection::new_with_slop_order(vec![a, b], 1.0, false, Some(0), true)
}

#[test]
fn phrase_intersection_keeps_only_relevant_documents() {
    let docs: Vec<_> = (1..=5).map(|id| (id, 2.0)).collect();
    // Doc 1 and 4 are adjacent and in order; 2 and 5 are too far apart; 3 is out of order.
    let a_positions = [1, 1, 3, 1, 1];
    let b_positions = [2, 4, 2, 2, 3];
    let build = || phrase((&docs, &a_positions), (&docs, &b_positions));
    let threshold = min_competitive(1.0);

    assert_eq!(plain_survivors(&mut build(), threshold), [1, 4]);
    assert_eq!(drain_above(&mut build(), threshold), [1, 4]);
}

#[test]
fn phrase_intersection_drops_by_bound_and_by_relevancy() {
    // Docs 1 to 3 cannot reach the threshold; of those that can, 5 fails the phrase check.
    let a: Vec<_> = (1..=6)
        .map(|id| (id, if id <= 3 { 1.0 } else { 3.0 }))
        .collect();
    let b: Vec<_> = (1..=6).map(|id| (id, 1.0)).collect();
    let a_positions = [1, 1, 1, 1, 1, 1];
    // Doc 3 fails both checks.
    let b_positions = [2, 2, 5, 2, 5, 2];
    let build = || phrase((&a, &a_positions), (&b, &b_positions));
    let threshold = min_competitive(3.5);

    assert_eq!(plain_survivors(&mut build(), threshold), [4, 6]);
    assert_eq!(drain_above(&mut build(), threshold), [4, 6]);
}

/****************************** unions ******************************/

/// Builds a union over leaves, under the name of its variant.
struct UnionCase {
    name: &'static str,
    /// Whether the union returns on the first child that matches a document rather than
    /// aggregating every one.
    quick_exit: bool,
    /// Builds the union over the leaves, its score scaled by the weight.
    build_weighted: fn(Vec<Blocked<'static>>, f64) -> DynIterator,
}

impl UnionCase {
    fn build(&self, children: Vec<Blocked<'static>>) -> DynIterator {
        (self.build_weighted)(children, 1.0)
    }
}

const UNIONS: &[UnionCase] = &[
    UnionCase {
        name: "full flat",
        quick_exit: false,
        build_weighted: |children, weight| weighted(UnionFullFlat::new(children), weight),
    },
    UnionCase {
        name: "full heap",
        quick_exit: false,
        build_weighted: |children, weight| weighted(UnionFullHeap::new(children), weight),
    },
    UnionCase {
        name: "quick flat",
        quick_exit: true,
        build_weighted: |children, weight| weighted(UnionQuickFlat::new(children), weight),
    },
    UnionCase {
        name: "quick heap",
        quick_exit: true,
        build_weighted: |children, weight| weighted(UnionQuickHeap::new(children), weight),
    },
    UnionCase {
        name: "opaque flat, through the C ABI",
        quick_exit: false,
        build_weighted: |children, weight| opaque_union(children, usize::MAX, weight),
    },
    UnionCase {
        name: "opaque heap, through the C ABI",
        quick_exit: false,
        build_weighted: |children, weight| opaque_union(children, 0, weight),
    },
];

/// `union` with its score scaled by `weight`, which a Rust union takes from its result, as the
/// query builder sets it.
fn weighted<U: RQEIterator<'static> + 'static>(mut union: U, weight: f64) -> DynIterator {
    union
        .current()
        .expect("an unread union over children holds a result")
        .weight = weight;
    Box::new(union)
}

/// The union a query builds: an opaque union over C-wrapped children, itself reached through
/// the C vtable.
fn opaque_union(
    children: Vec<Blocked<'static>>,
    min_union_iter_heap: usize,
    weight: f64,
) -> DynIterator {
    let children = children
        .into_iter()
        .map(CRQEIterator::from_rust_leaf)
        .collect();
    let header = build_union(
        children,
        false,
        min_union_iter_heap,
        QueryNodeType::Union,
        weight,
    );
    // SAFETY: `build_union` returns a uniquely-owned header with every required callback set.
    Box::new(unsafe { CRQEIterator::new(header) })
}

/// `a` has two weak blocks then a strong one; `b` sits far ahead in a block of its own, so every
/// variant, a quick-exit one included, positions both children on its first read.
fn union_children() -> [(Blocked<'static>, Log); 2] {
    [
        leaf(vec![Block::uniform([40], 0.0)]),
        leaf(vec![
            Block::uniform(1..=4, 1.0),
            Block::uniform(5..=8, 1.0),
            Block::uniform(9..=12, 4.0),
        ]),
    ]
}

#[test]
fn unions_skip_windows_whose_block_sum_is_below_the_threshold() {
    for case in UNIONS {
        let [(b, _), (a, a_log)] = union_children();
        let mut it = case.build(vec![b, a]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        // The window from each dropped document runs to the end of its block.
        assert_eq!(visited(&a_log), [1, 5, 9, 10, 11, 12], "{}", case.name);
    }
}

#[test]
fn unions_skip_up_to_a_child_without_a_block_boundary() {
    for case in UNIONS {
        // `b` reports no block, but it stands at 40 and matches nothing before it, so it does not
        // hold back the skips through `a`.
        let [(b, _), (a, a_log)] = union_children();
        let mut it = case.build(vec![b.without_block_bounds(), a]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        assert_eq!(visited(&a_log), [1, 5, 9, 10, 11, 12], "{}", case.name);
    }
}

#[test]
fn full_unions_do_not_skip_past_a_child_without_a_block_boundary() {
    for case in full_unions() {
        let [_, (a, a_log)] = union_children();
        let (c, _) = leaf(vec![Block::uniform([3], 0.0)]);
        let mut it = case.build(vec![c.without_block_bounds(), a]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        // `c` could score anything at 3, so the union stops there, and reads on until `c` runs
        // out. At 4 `a`'s block ends, so it is unknown from 5 and is read once more.
        assert_eq!(
            visited(&a_log),
            [1, 3, 4, 5, 9, 10, 11, 12],
            "{}",
            case.name
        );
    }
}

#[test]
fn full_unions_follow_misaligned_block_boundaries() {
    let build = || {
        [
            leaf(vec![
                Block::uniform(1..=4, 1.0),
                Block::uniform(5..=8, 1.0),
                Block::uniform(9..=12, 4.0),
            ]),
            leaf(vec![
                Block::uniform([2, 4, 6], 1.0),
                Block::uniform([10, 12], 4.0),
            ]),
        ]
    };
    for case in full_unions() {
        let [(a, a_log), (b, b_log)] = build();
        let mut it = case.build(vec![a, b]);

        // Windows: [2, 4] and [6, 6] both sum to 2. From 7, `a` alone covers 8, since `b` stands
        // at 10, past `a`'s block end.
        assert_eq!(
            drain_above(&mut it, min_competitive(4.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        assert_eq!(visited(&a_log), [1, 5, 7, 9, 10, 11, 12], "{}", case.name);
        assert_eq!(visited(&b_log), [2, 6, 10, 12], "{}", case.name);
    }
}

/// The union variants that aggregate every child on a document. A quick-exit union returns on
/// the first child that matches, so which children it has positioned, and so where it can pivot,
/// depends on their order.
fn full_unions() -> impl Iterator<Item = &'static UnionCase> {
    UNIONS.iter().filter(|case| !case.quick_exit)
}

#[test]
fn full_unions_pivot_past_documents_only_earlier_children_match() {
    for case in full_unions() {
        // Together the blocks reach 3, so the union's own block bound never lets it skip; but
        // before 10 only `a` can match, and `a` alone cannot.
        let (a, a_log) = leaf(vec![Block::uniform(1..=20, 1.0)]);
        let (b, b_log) = leaf(vec![Block::uniform(10..=20, 2.5)]);
        let mut it = case.build(vec![a, b]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(10..=20),
            "{}",
            case.name
        );
        let mut expected = vec![1];
        expected.extend(10..=20);
        assert_eq!(visited(&a_log), expected, "{}", case.name);
        assert_eq!(visited(&b_log), ids(10..=20), "{}", case.name);
    }
}

#[test]
fn full_unions_do_not_pivot_past_the_end_of_an_earlier_block() {
    for case in full_unions() {
        // The weak block of `a` ends at 5, before `b` starts: beyond it `a` is unknown, and its
        // next block alone reaches the threshold.
        let (a, a_log) = leaf(vec![
            Block::uniform(1..=5, 1.0),
            Block::uniform(6..=20, 3.0),
        ]);
        let (b, _) = leaf(vec![Block::uniform(12..=20, 2.5)]);
        let mut it = case.build(vec![a, b]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(6..=20),
            "{}",
            case.name
        );
        let mut expected = vec![1];
        expected.extend(6..=20);
        assert_eq!(visited(&a_log), expected, "{}", case.name);
    }
}

#[test]
fn quick_flat_union_never_pivots_past_an_unpositioned_child() {
    // `a` comes first and matches every document, so the quick flat union returns on it each time
    // and never positions `b`: `b` has no block, and every candidate stays reachable. (The quick
    // heap union positions every child on its first skip, so it pivots here.)
    let case = UNIONS
        .iter()
        .find(|case| case.name == "quick flat")
        .expect("a quick flat case");
    let (a, a_log) = leaf(vec![Block::uniform(1..=20, 1.0)]);
    let (b, b_log) = leaf(vec![Block::uniform(10..=20, 2.5)]);
    let mut it = case.build(vec![a, b]);

    let mut plain = case.build(vec![
        Blocked::new(vec![Block::uniform(1..=20, 1.0)]),
        Blocked::new(vec![Block::uniform(10..=20, 2.5)]),
    ]);
    let threshold = min_competitive(3.0);
    assert_eq!(
        drain_above(&mut it, threshold),
        plain_survivors(&mut plain, threshold)
    );
    assert_eq!(visited(&a_log), ids(1..=20));
    assert!(visited(&b_log).is_empty());
}

/****************************** threshold edges ******************************/

/// A window bound around threshold `4`, and whether the window under it is skipped.
struct EdgeCase {
    name: &'static str,
    window_bound: f64,
    skipped: bool,
}

fn edge_cases() -> [EdgeCase; 2] {
    [
        EdgeCase {
            name: "equal to the threshold",
            window_bound: 4.0,
            skipped: false,
        },
        EdgeCase {
            name: "one ulp below the threshold",
            window_bound: 4.0_f64.next_down(),
            skipped: true,
        },
    ]
}

/// A leaf whose first block, `[1, 5]`, scores `window_bound`, followed by a strong block. Adding
/// a zero-scored partner keeps the window sum exactly `window_bound`.
fn edge_leaf(window_bound: f64) -> (Blocked<'static>, Log) {
    leaf(vec![
        Block::uniform(1..=5, window_bound),
        Block::uniform(6..=10, 10.0),
    ])
}

/// The window is yielded exactly when it is kept, and only `visited_first` of it is visited when
/// it is skipped.
fn assert_edge(
    case: &EdgeCase,
    family: &str,
    visited_first: &[DocId],
    yielded: Vec<DocId>,
    log: &Log,
) {
    let (expected_yield, expected_visit) = if case.skipped {
        (ids(6..=10), [visited_first.to_vec(), ids(6..=10)].concat())
    } else {
        (ids(1..=10), ids(1..=10))
    };
    assert_eq!(yielded, expected_yield, "{family}: {}", case.name);
    assert_eq!(visited(log), expected_visit, "{family}: {}", case.name);
}

#[test]
fn intersection_keeps_windows_that_reach_the_threshold() {
    for case in edge_cases() {
        let (a, a_log) = edge_leaf(case.window_bound);
        let (b, _) = leaf(vec![Block::uniform(1..=10, 0.0)]);
        let mut it = Intersection::new(vec![a, b], 1.0, false);
        let yielded = drain_above(&mut it, min_competitive(4.0));
        // The leading child moves on from a dropped document before the window is checked.
        assert_edge(&case, "intersection", &[1, 2], yielded, &a_log);
    }
}

#[test]
fn unions_keep_windows_that_reach_the_threshold() {
    for union in UNIONS {
        for case in edge_cases() {
            let (a, a_log) = edge_leaf(case.window_bound);
            let (b, _) = leaf(vec![Block::uniform([40], 0.0)]);
            let mut it = union.build(vec![b, a]);
            let mut yielded = drain_above(&mut it, min_competitive(4.0));
            // Doc 40 scores 0 and is dropped either way.
            yielded.retain(|&doc| doc != 40);
            assert_edge(&case, union.name, &[1], yielded, &a_log);
        }
    }
}

/****************************** weight ******************************/

/// Weights for which the composites' skip bounds are scaled up (`true`), or left unscaled
/// (`false`): a weight below 1 must skip exactly as weight 1 does.
const WEIGHTS: [(f64, bool); 3] = [(2.0, true), (1.0, false), (0.5, false)];

/// Every block sums to 2, below the threshold of 3 unless the weight scales it up: the window
/// check, the target check and the drop of agreed documents must all apply the weight.
#[test]
fn intersection_scales_its_skip_bounds_by_weight_but_never_down() {
    for (weight, scaled_up) in WEIGHTS {
        let (a, a_log) = leaf(vec![Block::uniform(1..=10, 1.0)]);
        let (b, _) = leaf(vec![Block::uniform(1..=10, 1.0)]);
        let mut it = Intersection::new(vec![a, b], weight, false);

        let (kept, a_visits) = if scaled_up {
            (ids(1..=10), ids(1..=10))
        } else {
            // The first two documents are read before the window is known.
            (vec![], vec![1, 2])
        };
        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            kept,
            "weight {weight}"
        );
        assert_eq!(visited(&a_log), a_visits, "weight {weight}");
    }
}

/// `a`'s block maximum of 1 is below the threshold of 1.5 unless the weight scales it up: the
/// pivot and the drop of landed documents must both apply the weight.
#[test]
fn unions_scale_their_skip_bounds_by_weight_but_never_down() {
    for case in UNIONS {
        for (weight, scaled_up) in WEIGHTS {
            // `b` stands far ahead, as in `union_children`, so every variant positions both.
            let (b, _) = leaf(vec![Block::uniform([40], 0.0)]);
            let (a, a_log) = leaf(vec![Block::uniform(1..=8, 1.0)]);
            let mut it = (case.build_weighted)(vec![b, a], weight);

            let (kept, a_visits) = if scaled_up {
                (ids(1..=8), ids(1..=8))
            } else {
                (vec![], vec![1])
            };
            assert_eq!(
                drain_above(&mut it, min_competitive(1.5)),
                kept,
                "{} at weight {weight}",
                case.name
            );
            assert_eq!(
                visited(&a_log),
                a_visits,
                "{} at weight {weight}",
                case.name
            );
        }
    }
}

/****************************** composition ******************************/

#[test]
fn composites_report_the_earliest_block_end_and_the_summed_maximum() {
    let children = || {
        vec![
            Blocked::new(vec![
                Block::loose(&[(1, 1.0)], 1.5),
                Block::uniform([9], 1.0),
            ]),
            Blocked::new(vec![Block::loose(&[(1, 2.0), (5, 2.0)], 2.5)]),
        ]
    };
    let expected = Some(BlockBound {
        last_doc_id: 1,
        max_score: 4.0,
    });

    let mut and = Intersection::new(children(), 1.0, false);
    assert_eq!(and.current_block_bound(), None, "unread");
    and.read().expect("read failed");
    assert_eq!(and.current_block_bound(), expected);

    let mut heavy = Intersection::new(children(), 2.0, false);
    heavy.read().expect("read failed");
    assert_eq!(heavy.current_block_bound().map(|b| b.max_score), Some(8.0));

    for union in UNIONS {
        let mut it = union.build(children());
        assert_eq!(it.current_block_bound(), None, "{}: unread", union.name);
        it.read().expect("read failed");
        if union.quick_exit {
            // A quick-exit union leaves its second child unread.
            continue;
        }
        assert_eq!(it.current_block_bound(), expected, "{}", union.name);
    }
}

#[test]
fn a_composite_with_a_child_without_blocks_reports_none() {
    let mut and = Intersection::new(
        vec![
            Blocked::new(vec![Block::uniform([1], 1.0)]),
            Blocked::new(vec![Block::uniform([1], 1.0)]).without_block_bounds(),
        ],
        1.0,
        false,
    );
    and.read().expect("read failed");
    assert_eq!(and.current_block_bound(), None);
}

#[test]
fn composites_do_not_pass_the_threshold_to_their_children() {
    let [(a, a_log), (b, b_log)] = and_children();
    drain_above(
        &mut Intersection::new(vec![a, b], 1.0, false),
        min_competitive(3.0),
    );
    for case in UNIONS {
        let [(c, c_log), (d, d_log)] = union_children();
        drain_above(&mut case.build(vec![c, d]), min_competitive(3.0));
        for log in [&c_log, &d_log] {
            assert_eq!(log.borrow().thresholded_reads, 0, "{}", case.name);
        }
    }
    for log in [&a_log, &b_log] {
        assert_eq!(log.borrow().thresholded_reads, 0, "intersection");
    }
}

/// An intersection of a union and a leaf, and a union of an intersection and a leaf, each skip
/// through the block bounds their composite child reports.
#[test]
fn nested_composites_skip_through_their_composite_children() {
    struct Nested {
        name: &'static str,
        build: fn([Blocked<'static>; 3]) -> DynIterator,
    }
    let shapes = [
        Nested {
            name: "AND(OR(a, b), c)",
            build: |[a, b, c]| {
                let or: DynIterator = Box::new(UnionFullFlat::new(vec![a, b]));
                Box::new(Intersection::new(vec![or, Box::new(c)], 1.0, false))
            },
        },
        Nested {
            name: "OR(AND(a, b), c)",
            build: |[a, b, c]| {
                let and: DynIterator = Box::new(Intersection::new(vec![a, b], 1.0, false));
                Box::new(UnionFullHeap::new(vec![and, Box::new(c)]))
            },
        },
    ];
    let leaves = || {
        [
            leaf(vec![
                Block::uniform(1..=20, 1.0),
                Block::uniform(21..=30, 3.0),
            ]),
            leaf(vec![
                Block::uniform((2..=20).step_by(2), 1.0),
                Block::uniform((22..=30).step_by(2), 3.0),
            ]),
            leaf(vec![Block::uniform(1..=30, 0.5)]),
        ]
    };
    for shape in shapes {
        let min = min_competitive(3.0);
        let expected = plain_survivors(&mut (shape.build)(leaves().map(|(l, _)| l)), min);
        assert!(!expected.is_empty(), "{}: a vacuous case", shape.name);

        let [(a, a_log), (b, _), (c, _)] = leaves();
        let mut it = (shape.build)([a, b, c]);
        assert_eq!(drain_above(&mut it, min), expected, "{}", shape.name);
        assert!(
            !visited(&a_log).iter().any(|doc| (3..=20).contains(doc)),
            "{}: a's weak block was visited: {:?}",
            shape.name,
            visited(&a_log),
        );
    }
}

/// A Rust intersection reached through the C vtable reports its block bound there, and skips.
#[test]
fn intersection_through_the_c_abi_skips_windows() {
    let [(a, a_log), (b, _)] = and_children();
    let children = vec![
        CRQEIterator::from_rust_leaf(a),
        CRQEIterator::from_rust_leaf(b),
    ];
    let header = RQEIteratorWrapper::boxed_new_compound(Intersection::new(children, 1.0, false));
    // SAFETY: `boxed_new_compound` returns a uniquely-owned header with every required callback
    // set.
    let mut it = unsafe { CRQEIterator::new(header) };

    assert_eq!(it.current_block_bound(), None, "unread");
    assert_eq!(drain_above(&mut it, min_competitive(3.0)), ids(11..=20));
    assert_eq!(visited(&a_log), [vec![1, 2], ids(11..=20)].concat());
}

/****************************** equivalence ******************************/

/// Thresholds for successive reads, rising like the minimum of a filling heap. The last one holds
/// for every later read.
fn thresholds() -> impl Strategy<Value = Vec<MinCompetitiveScore>> {
    proptest::collection::vec(1u8..=32, 1..=6).prop_map(|mut steps| {
        steps.sort_unstable();
        steps
            .into_iter()
            .map(|s| MinCompetitiveScore::new(f64::from(s) * 0.25).expect("positive"))
            .collect()
    })
}

/// Checks that thresholded reads of `tree` keep every document a plain read yields that reaches
/// the threshold in force while it is passed, and yield only real matches, in increasing order,
/// then EOF. Only the root may receive the threshold.
fn check_thresholded_reads(
    tree: &Tree,
    thresholds: &[MinCompetitiveScore],
) -> Result<(), TestCaseError> {
    let scores = tree.scores();
    let mut logs = Vec::new();
    let mut it = tree.build(&mut logs);
    let is_leaf = matches!(tree, Tree::Leaf(_));

    let mut previous: DocId = 0;
    for call in 0.. {
        let min = thresholds[call.min(thresholds.len() - 1)];
        let yielded = it
            .read_with_threshold(min)
            .expect("read failed")
            .map(|r| r.doc_id);
        // Every document passed during this call that reaches `min` must be the one returned.
        let passed_to = yielded.unwrap_or(DocId::MAX);
        for (&doc, &score) in scores.range(previous + 1..=passed_to) {
            if !can_drop(score, min) {
                prop_assert_eq!(
                    Some(doc),
                    yielded,
                    "doc {} scoring {} skipped under {:?}",
                    doc,
                    score,
                    min
                );
            }
        }
        let Some(doc) = yielded else { break };
        prop_assert!(doc > previous, "doc {} after {}", doc, previous);
        prop_assert!(scores.contains_key(&doc), "doc {} is not a match", doc);
        prop_assert!(
            !can_drop(it.current_max_score(), min),
            "doc {} below {:?}",
            doc,
            min
        );
        previous = doc;
    }
    prop_assert!(it.read().expect("read failed").is_none(), "EOF is terminal");

    let thresholded = logs
        .iter()
        .filter(|log| log.borrow().thresholded_reads > 0)
        .count();
    prop_assert!(thresholded <= usize::from(is_leaf));
    Ok(())
}

proptest! {
    // Competitive documents inside skippable-looking windows are rare in random trees, so it
    // takes more cases than the default to reach them reliably.
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// [`check_thresholded_reads`] over random trees, quick-exit unions included.
    #[test]
    #[cfg_attr(miri, ignore = "Thousands of random trees: too slow under Miri")]
    fn thresholded_reads_keep_every_competitive_document(
        tree in tree(40, 30, true),
        thresholds in thresholds(),
    ) {
        check_thresholded_reads(&tree, &thresholds)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// [`check_thresholded_reads`] over wide quick-exit unions. Only a quick-exit union leaves
    /// children behind it, and a wide one is where the heap variant walks its heap for the pivot.
    #[test]
    #[cfg_attr(miri, ignore = "Hundreds of random unions: too slow under Miri")]
    fn wide_quick_exit_unions_keep_every_competitive_document(
        leaves in proptest::collection::vec(leaf_spec(60, 30), 2..=24),
        heap in any::<bool>(),
        thresholds in thresholds(),
    ) {
        let tree = Tree::Or { children: leaves, heap, quick_exit: true };
        check_thresholded_reads(&tree, &thresholds)?;
    }
}

/// What a union read under `thresholds` returns, and how it moved each of its leaves: the
/// documents a leaf positioned on, and how many reads and skips it took. The union is rewound
/// once, before read `rewind_at`, if it gets that far: the heap union then pivots by rescanning
/// until its heap is populated again.
fn trace_union(
    leaves: &[Tree],
    heap: bool,
    thresholds: &[MinCompetitiveScore],
    rewind_at: usize,
) -> (Vec<DocId>, Vec<(Vec<DocId>, usize, usize)>) {
    let tree = Tree::Or {
        children: leaves.to_vec(),
        heap,
        quick_exit: false,
    };
    let mut logs = Vec::new();
    let mut it = tree.build(&mut logs);
    let mut docs = Vec::new();
    for call in 0.. {
        if call == rewind_at {
            it.rewind();
        }
        let min = thresholds[call.min(thresholds.len() - 1)];
        match it.read_with_threshold(min).expect("read failed") {
            Some(r) => docs.push(r.doc_id),
            None => break,
        }
    }
    let moves = logs
        .iter()
        .map(|log| {
            let log = log.borrow();
            (log.visited.clone(), log.reads, log.skip_tos)
        })
        .collect();
    (docs, moves)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// The heap union finds its pivot by walking its heap rather than rescanning its children,
    /// and must reach the same decisions: the same documents come back, and every leaf is read
    /// and skipped the same way as under the flat union. Full unions only: the quick-exit
    /// variants position their children differently by design (see
    /// `quick_flat_union_never_pivots_past_an_unpositioned_child`).
    #[test]
    #[cfg_attr(miri, ignore = "Hundreds of random unions: too slow under Miri")]
    fn heap_and_flat_unions_pivot_alike(
        leaves in proptest::collection::vec(leaf_spec(60, 30), 2..=24),
        thresholds in thresholds(),
        rewind_at in 1..=8usize,
    ) {
        prop_assert_eq!(
            trace_union(&leaves, true, &thresholds, rewind_at),
            trace_union(&leaves, false, &thresholds, rewind_at),
        );
    }
}

/// A quick-exit heap union walks its heap only while the heap holds every child it counts as
/// active, so a child already at EOF on the first read must not stay counted.
#[test]
fn quick_heap_union_does_not_count_a_child_already_at_eof() {
    let (a, _) = leaf(vec![Block::uniform(1..=4, 1.0)]);
    let (b, _) = leaf(vec![Block::uniform(3..=6, 1.0)]);
    let children: Vec<DynIterator> = vec![Box::new(Empty), Box::new(a), Box::new(b)];
    let mut it = UnionQuickHeap::new(children);

    it.read_with_threshold(min_competitive(0.5))
        .expect("read failed");
    assert_eq!(it.num_children_active(), 2, "after the first read");

    it.rewind();
    it.read_with_threshold(min_competitive(0.5))
        .expect("read failed");
    assert_eq!(it.num_children_active(), 2, "after a rewind");
}
