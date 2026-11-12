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

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use proptest::prelude::*;
use query_types::QueryNodeType;
use rqe_core::DocId;
use rqe_iterators::{
    Intersection, RQEIterator, UnionFullFlat, UnionFullHeap, UnionQuickFlat, UnionQuickHeap,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    score_bound::{BlockBound, MinCompetitiveScore, can_drop},
    union_opaque::build_union,
};

use crate::utils::blocked::{Block, Blocked, BlockedLog};

type Log = Rc<RefCell<BlockedLog>>;
type DynIterator = Box<dyn RQEIterator<'static>>;

/// The threshold the score sorter passes down for heap minimum `theta`, as
/// `ScoreBound_MinCompetitive` computes it.
fn min_competitive(theta: f64) -> MinCompetitiveScore {
    MinCompetitiveScore::new(theta * (1.0 - ffi::SCORE_BOUND_REL_EPSILON))
        .expect("a positive heap minimum")
}

fn leaf(blocks: Vec<Block>) -> (Blocked, Log) {
    let leaf = Blocked::new(blocks);
    let log = leaf.log();
    (leaf, log)
}

fn visited(log: &Log) -> Vec<DocId> {
    log.borrow().visited.clone()
}

/// Drains `it` under `min_competitive_score`, checking that nothing comes back below it.
fn drain_above<'index>(
    it: &mut impl RQEIterator<'index>,
    min_competitive_score: MinCompetitiveScore,
) -> Vec<DocId> {
    let mut docs = Vec::new();
    while let Some(doc) = it
        .read_with_threshold(min_competitive_score)
        .expect("read failed")
        .map(|r| r.doc_id)
    {
        assert!(
            !can_drop(it.current_max_score(), min_competitive_score),
            "doc {doc} came back with bound {} below {min_competitive_score:?}",
            it.current_max_score(),
        );
        docs.push(doc);
    }
    docs
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
fn and_children() -> [(Blocked, Log); 2] {
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

/****************************** unions ******************************/

/// Builds a union over leaves, under the name of its variant.
struct UnionCase {
    name: &'static str,
    build: fn(Vec<Blocked>) -> DynIterator,
}

const UNIONS: &[UnionCase] = &[
    UnionCase {
        name: "full flat",
        build: |children| Box::new(UnionFullFlat::new(children)),
    },
    UnionCase {
        name: "full heap",
        build: |children| Box::new(UnionFullHeap::new(children)),
    },
    UnionCase {
        name: "quick flat",
        build: |children| Box::new(UnionQuickFlat::new(children)),
    },
    UnionCase {
        name: "quick heap",
        build: |children| Box::new(UnionQuickHeap::new(children)),
    },
    UnionCase {
        name: "opaque flat, through the C ABI",
        build: |children| opaque_union(children, usize::MAX),
    },
    UnionCase {
        name: "opaque heap, through the C ABI",
        build: |children| opaque_union(children, 0),
    },
];

/// The union a query builds: an opaque union over C-wrapped children, itself reached through
/// the C vtable.
fn opaque_union(children: Vec<Blocked>, min_union_iter_heap: usize) -> DynIterator {
    let children = children
        .into_iter()
        .map(CRQEIterator::from_rust_leaf)
        .collect();
    let header = build_union(
        children,
        false,
        min_union_iter_heap,
        QueryNodeType::Union,
        1.0,
    );
    // SAFETY: `build_union` returns a uniquely-owned header with every required callback set.
    Box::new(unsafe { CRQEIterator::new(header) })
}

/// `a` has two weak blocks then a strong one; `b` sits far ahead in a block of its own, so every
/// variant, a quick-exit one included, positions both children on its first read.
fn union_children() -> [(Blocked, Log); 2] {
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
        let mut it = (case.build)(vec![b, a]);

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
fn unions_without_a_block_boundary_visit_every_document() {
    for case in UNIONS {
        let [(b, _), (a, a_log)] = union_children();
        let mut it = (case.build)(vec![b.without_block_bounds(), a]);

        assert_eq!(
            drain_above(&mut it, min_competitive(3.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        assert_eq!(visited(&a_log), ids(1..=12), "{}", case.name);
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
    for case in &UNIONS[..2] {
        let [(a, a_log), (b, b_log)] = build();
        let mut it = (case.build)(vec![a, b]);

        // Windows: [2, 4] and [6, 6] both sum to 2; from 7 the window reaches b's strong block.
        assert_eq!(
            drain_above(&mut it, min_competitive(4.0)),
            ids(9..=12),
            "{}",
            case.name
        );
        assert_eq!(
            visited(&a_log),
            [1, 5, 7, 8, 9, 10, 11, 12],
            "{}",
            case.name
        );
        assert_eq!(visited(&b_log), [2, 6, 10, 12], "{}", case.name);
    }
}

/****************************** threshold edges ******************************/

/// A window bound around threshold `4`, and whether the window under it is skipped.
struct EdgeCase {
    name: &'static str,
    window_bound: f64,
    skipped: bool,
}

fn edge_cases() -> [EdgeCase; 4] {
    let at_margin = min_competitive(4.0);
    [
        EdgeCase {
            name: "equal to the threshold",
            window_bound: 4.0,
            skipped: false,
        },
        EdgeCase {
            name: "inside the margin",
            window_bound: 4.0 * (1.0 - ffi::SCORE_BOUND_REL_EPSILON / 2.0),
            skipped: false,
        },
        EdgeCase {
            name: "at the margin",
            window_bound: at_margin.get(),
            skipped: false,
        },
        EdgeCase {
            name: "just below the margin",
            window_bound: at_margin.get().next_down(),
            skipped: true,
        },
    ]
}

/// A leaf whose first block, `[1, 5]`, scores `window_bound`, followed by a strong block. Adding
/// a zero-scored partner keeps the window sum exactly `window_bound`.
fn edge_leaf(window_bound: f64) -> (Blocked, Log) {
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
fn intersection_keeps_windows_that_reach_the_margin() {
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
fn unions_keep_windows_that_reach_the_margin() {
    for union in UNIONS {
        for case in edge_cases() {
            let (a, a_log) = edge_leaf(case.window_bound);
            let (b, _) = leaf(vec![Block::uniform([40], 0.0)]);
            let mut it = (union.build)(vec![b, a]);
            let mut yielded = drain_above(&mut it, min_competitive(4.0));
            // Doc 40 scores 0 and is dropped either way.
            yielded.retain(|&doc| doc != 40);
            assert_edge(&case, union.name, &[1], yielded, &a_log);
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
        let mut it = (union.build)(children());
        assert_eq!(it.current_block_bound(), None, "{}: unread", union.name);
        it.read().expect("read failed");
        if union.name.starts_with("quick") {
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
        drain_above(&mut (case.build)(vec![c, d]), min_competitive(3.0));
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
        build: fn([Blocked; 3]) -> DynIterator,
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

/// A leaf to build: its blocks, and whether it reports their boundaries.
#[derive(Debug, Clone)]
struct LeafSpec {
    blocks: Vec<Block>,
    reports_blocks: bool,
}

#[derive(Debug, Clone)]
enum Tree {
    Leaf(LeafSpec),
    And(Vec<Tree>),
    Or { children: Vec<Tree>, heap: bool },
}

impl Tree {
    /// Builds the iterator, collecting the logs of its leaves.
    fn build(&self, logs: &mut Vec<Log>) -> DynIterator {
        match self {
            Self::Leaf(spec) => {
                let leaf = Blocked::new(spec.blocks.clone());
                logs.push(leaf.log());
                if spec.reports_blocks {
                    Box::new(leaf)
                } else {
                    Box::new(leaf.without_block_bounds())
                }
            }
            Self::And(children) => Box::new(Intersection::new(
                children.iter().map(|child| child.build(logs)).collect(),
                1.0,
                false,
            )),
            Self::Or { children, heap } => {
                let children: Vec<_> = children.iter().map(|child| child.build(logs)).collect();
                if *heap {
                    Box::new(UnionFullHeap::new(children))
                } else {
                    Box::new(UnionFullFlat::new(children))
                }
            }
        }
    }

    /// The score of every matching document, as the composites define it at weight 1: an
    /// intersection sums all its children, a union those that match.
    fn scores(&self) -> BTreeMap<DocId, f64> {
        match self {
            Self::Leaf(spec) => spec
                .blocks
                .iter()
                .flat_map(|block| block.docs.iter().copied())
                .collect(),
            Self::And(children) => {
                let mut children = children.iter().map(Self::scores);
                let first = children.next().expect("an intersection has children");
                children.fold(first, |acc, child| {
                    acc.into_iter()
                        .filter_map(|(doc, score)| child.get(&doc).map(|s| (doc, score + s)))
                        .collect()
                })
            }
            Self::Or { children, .. } => {
                let mut scores = BTreeMap::new();
                for child in children {
                    for (doc, score) in child.scores() {
                        *scores.entry(doc).or_insert(0.0) += score;
                    }
                }
                scores
            }
        }
    }
}

fn leaf_spec() -> impl Strategy<Value = Tree> {
    let docs = proptest::collection::btree_map(1u64..=40, 0u8..=8, 1..=30);
    (docs, any::<u64>(), proptest::bool::weighted(0.85)).prop_map(
        |(docs, splits, reports_blocks)| {
            let mut blocks: Vec<Vec<(DocId, f64)>> = vec![Vec::new()];
            for (i, (doc, score)) in docs.into_iter().enumerate() {
                if splits & (1 << (i % 64)) != 0 && !blocks.last().expect("non-empty").is_empty() {
                    blocks.push(Vec::new());
                }
                blocks
                    .last_mut()
                    .expect("non-empty")
                    .push((doc, f64::from(score) * 0.5));
            }
            let blocks = blocks
                .iter()
                .enumerate()
                .map(|(i, docs)| {
                    let tight = Block::tight(docs);
                    // Every third block is quantized up.
                    let slack = if i % 3 == 2 { 0.75 } else { 0.0 };
                    Block::loose(docs, tight.max_score + slack)
                })
                .collect();
            Tree::Leaf(LeafSpec {
                blocks,
                reports_blocks,
            })
        },
    )
}

fn tree() -> impl Strategy<Value = Tree> {
    leaf_spec().prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 2..=3).prop_map(Tree::And),
            (proptest::collection::vec(inner, 2..=3), any::<bool>())
                .prop_map(|(children, heap)| Tree::Or { children, heap }),
        ]
    })
}

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

proptest! {
    // Competitive documents inside skippable-looking windows are rare in random trees, so it
    // takes more cases than the default to reach them reliably.
    #![proptest_config(ProptestConfig::with_cases(2048))]

    /// Thresholded reads keep every document a plain read yields that reaches the threshold in
    /// force while it is passed, and yield only real matches, in increasing order, then EOF.
    #[test]
    fn thresholded_reads_keep_every_competitive_document(tree in tree(), thresholds in thresholds()) {
        let scores = tree.scores();
        let mut logs = Vec::new();
        let mut it = tree.build(&mut logs);
        let is_leaf = matches!(tree, Tree::Leaf(_));

        let mut previous: DocId = 0;
        for call in 0.. {
            let min = thresholds[call.min(thresholds.len() - 1)];
            let yielded = it.read_with_threshold(min).expect("read failed").map(|r| r.doc_id);
            // Every document passed during this call that reaches `min` must be the one returned.
            let passed_to = yielded.unwrap_or(DocId::MAX);
            for (&doc, &score) in scores.range(previous + 1..=passed_to) {
                if !can_drop(score, min) {
                    prop_assert_eq!(Some(doc), yielded, "doc {} scoring {} skipped under {:?}", doc, score, min);
                }
            }
            let Some(doc) = yielded else { break };
            prop_assert!(doc > previous, "doc {} after {}", doc, previous);
            prop_assert!(scores.contains_key(&doc), "doc {} is not a match", doc);
            prop_assert!(!can_drop(it.current_max_score(), min), "doc {} below {:?}", doc, min);
            previous = doc;
        }
        prop_assert!(it.read().expect("read failed").is_none(), "EOF is terminal");

        // Only the root may receive the threshold.
        let thresholded = logs.iter().filter(|log| log.borrow().thresholded_reads > 0).count();
        prop_assert!(thresholded <= usize::from(is_leaf));
    }
}
