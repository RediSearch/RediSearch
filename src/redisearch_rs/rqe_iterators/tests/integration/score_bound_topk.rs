/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A top-K score sorter driven over random trees of [`Blocked`] leaves, once with plain
//! [`RQEIterator::read`]s and once the way the sorter drives [`RQEIterator::read_with_threshold`]:
//! plain reads until its heap is full, then the heap minimum as the threshold. Both runs must accept
//! the same candidates into the heap, in the same order, and end with the same top K.
//!
//! The cases come from a seeded generator, so a failure names the seed that reproduces it:
//! `SCORE_BOUND_SEED=<seed>` reruns only that case, and `SCORE_BOUND_CASES=<n>` changes how many
//! cases run.

use std::collections::BTreeMap;

use rqe_core::DocId;
use rqe_iterators::{
    Intersection, RQEIterator, UnionFullFlat, UnionFullHeap,
    score_bound::{MinCompetitiveScore, can_drop},
};

use crate::utils::blocked::{Block, Blocked, BlockedLog};

type Log = std::rc::Rc<std::cell::RefCell<BlockedLog>>;
type DynIterator = Box<dyn RQEIterator<'static>>;

/// Seed of the first case; case `i` runs with `BASE_SEED + i`.
const BASE_SEED: u64 = 0x5C0B_E0B0_0D5E_ED00;
/// Cases run when `SCORE_BOUND_CASES` is unset.
const DEFAULT_CASES: u64 = 400;
/// Scores are multiples of this, so sums are exact in `f64` and ties cannot be broken differently
/// by rounding in the two runs.
const SCORE_STEP: f64 = 1.0 / 16.0;

/// SplitMix64: small, deterministic, and enough to spread the cases.
struct Rng(u64);

impl Rng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi`.
    const fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }

    /// True with probability `p`.
    fn chance(&mut self, p: f64) -> bool {
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < p
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.range(0, items.len() as u64 - 1) as usize]
    }
}

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

    /// The score of every matching document at weight 1: an intersection sums all its children,
    /// a union those that match.
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

    fn leaves(&self) -> usize {
        match self {
            Self::Leaf(_) => 1,
            Self::And(children) | Self::Or { children, .. } => {
                children.iter().map(Self::leaves).sum()
            }
        }
    }
}

/// A random leaf over `1..=universe`: a frequent and weak term, a rare and strong one, or in
/// between, cut into blocks of varying size. Each block draws its own score cap, so a weak block
/// can be followed by a much stronger one, as where a term clusters in a run of documents; some
/// blocks report a maximum looser than their scores.
fn random_leaf(rng: &mut Rng, universe: u64) -> Tree {
    let density = rng.pick(&[0.005, 0.02, 0.05, 0.15, 0.4, 0.8]);
    // Rare terms score high, frequent ones low, as with IDF.
    let strength = if density < 0.03 {
        rng.range(24, 64)
    } else if density < 0.2 {
        rng.range(8, 32)
    } else {
        rng.range(2, 16)
    };
    let mut ids: Vec<DocId> = (1..=universe).filter(|_| rng.chance(density)).collect();
    if ids.is_empty() {
        ids.push(rng.range(1, universe));
    }
    let fixed_size = rng.chance(0.5).then(|| rng.range(1, 32) as usize);
    let mut blocks = Vec::new();
    let mut rest = ids.as_slice();
    while !rest.is_empty() {
        let size = fixed_size
            .unwrap_or_else(|| rng.range(1, 48) as usize)
            .min(rest.len());
        let (block_ids, tail) = rest.split_at(size);
        let cap = match rng.range(0, 3) {
            0 => strength / 4,
            1 | 2 => strength / 2,
            _ => strength,
        };
        let block: Vec<(DocId, f64)> = block_ids
            .iter()
            .map(|&doc| (doc, rng.range(0, cap) as f64 * SCORE_STEP))
            .collect();
        let tight = Block::tight(&block).max_score;
        let slack = if rng.chance(0.25) {
            rng.range(1, 16) as f64 * SCORE_STEP
        } else {
            0.0
        };
        blocks.push(Block::loose(&block, tight + slack));
        rest = tail;
    }
    Tree::Leaf(LeafSpec {
        blocks,
        reports_blocks: rng.chance(0.9),
    })
}

/// A random tree up to `depth` composite levels deep.
fn random_tree(rng: &mut Rng, universe: u64, depth: u32) -> Tree {
    if depth == 0 || rng.chance(0.3) {
        return random_leaf(rng, universe);
    }
    if rng.chance(0.4) {
        let children = (0..rng.range(2, 3))
            .map(|_| random_tree(rng, universe, depth - 1))
            .collect();
        Tree::And(children)
    } else {
        let children = (0..rng.range(2, 6))
            .map(|_| random_tree(rng, universe, depth - 1))
            .collect();
        Tree::Or {
            children,
            heap: rng.chance(0.5),
        }
    }
}

/// A heap of the best `k` `(doc, score)` pairs, ordered as the score sorter orders them: higher
/// score first, and between equal scores the lower doc id. Documents arrive in increasing id
/// order, so a candidate that only ties the worst entry never displaces it.
struct TopK {
    k: usize,
    entries: Vec<(DocId, f64)>,
}

impl TopK {
    const fn new(k: usize) -> Self {
        Self {
            k,
            entries: Vec::new(),
        }
    }

    /// The heap minimum once the heap is full.
    fn theta(&self) -> Option<f64> {
        (self.entries.len() == self.k).then(|| {
            self.entries
                .iter()
                .map(|&(_, score)| score)
                .fold(f64::INFINITY, f64::min)
        })
    }

    /// Offers a candidate, returning whether it entered the heap.
    fn offer(&mut self, doc: DocId, score: f64) -> bool {
        if self.entries.len() < self.k {
            self.entries.push((doc, score));
            return true;
        }
        let (worst, &(worst_doc, worst_score)) = self
            .entries
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
            .expect("a full heap is not empty");
        if score > worst_score || (score == worst_score && doc < worst_doc) {
            self.entries[worst] = (doc, score);
            return true;
        }
        false
    }

    fn sorted(&self) -> Vec<(DocId, f64)> {
        let mut entries = self.entries.clone();
        entries.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        entries
    }
}

/// The threshold the sorter passes down for heap minimum `theta`, as `ScoreBound_MinCompetitive`
/// computes it: `None` when it could not drop anything.
fn min_competitive(theta: f64) -> Option<MinCompetitiveScore> {
    MinCompetitiveScore::new(theta * (1.0 - ffi::SCORE_BOUND_REL_EPSILON))
}

/// What one sorter run saw.
struct Run {
    /// Candidates the heap accepted, in order.
    accepted: Vec<(DocId, f64)>,
    top: Vec<(DocId, f64)>,
    /// Entries the leaves decoded.
    decoded: usize,
}

/// Runs the sorter with plain reads only.
fn plain_run(tree: &Tree, k: usize) -> Run {
    let mut logs = Vec::new();
    let mut it = tree.build(&mut logs);
    let mut heap = TopK::new(k);
    let mut accepted = Vec::new();
    while let Some(doc) = it.read().expect("read failed").map(|r| r.doc_id) {
        let score = it.current_max_score();
        if heap.offer(doc, score) {
            accepted.push((doc, score));
        }
    }
    Run {
        accepted,
        top: heap.sorted(),
        decoded: decoded(&logs),
    }
}

/// Runs the sorter the way `pullCandidate` does, checking every read against the exact scores.
fn thresholded_run(tree: &Tree, k: usize, scores: &BTreeMap<DocId, f64>, seed: u64) -> Run {
    let mut logs = Vec::new();
    let mut it = tree.build(&mut logs);
    let mut heap = TopK::new(k);
    let mut accepted = Vec::new();
    let mut previous: DocId = 0;
    loop {
        let threshold = heap.theta().and_then(min_competitive);
        let yielded = match threshold {
            Some(min) => it.read_with_threshold(min),
            None => it.read(),
        }
        .expect("read failed")
        .map(|r| r.doc_id);

        // Every document passed over in this call scores below the threshold in force.
        let passed_to = yielded.unwrap_or(DocId::MAX);
        for (&doc, &score) in scores.range(previous + 1..passed_to) {
            let skippable = threshold.is_some_and(|min| can_drop(score, min));
            assert!(
                skippable,
                "seed {seed:#x}: doc {doc} scoring {score} passed over under {threshold:?} \
                 (rerun with SCORE_BOUND_SEED={seed})",
            );
        }
        let Some(doc) = yielded else { break };
        assert!(doc > previous, "seed {seed:#x}: doc {doc} after {previous}");
        let score = it.current_max_score();
        assert_eq!(
            scores.get(&doc).copied(),
            Some(score),
            "seed {seed:#x}: doc {doc} is not a match with that score",
        );
        if let Some(min) = threshold {
            assert!(
                !can_drop(score, min),
                "seed {seed:#x}: doc {doc} scoring {score} returned under {min:?}",
            );
        }
        if heap.offer(doc, score) {
            accepted.push((doc, score));
        }
        previous = doc;
    }
    assert!(
        it.read().expect("read failed").is_none(),
        "seed {seed:#x}: EOF is terminal",
    );
    let is_leaf = matches!(tree, Tree::Leaf(_));
    let thresholded = logs
        .iter()
        .filter(|log| log.borrow().thresholded_reads > 0)
        .count();
    assert!(
        thresholded <= usize::from(is_leaf),
        "seed {seed:#x}: only the root may receive the threshold",
    );
    Run {
        accepted,
        top: heap.sorted(),
        decoded: decoded(&logs),
    }
}

fn decoded(logs: &[Log]) -> usize {
    logs.iter().map(|log| log.borrow().visited.len()).sum()
}

/// Builds the case for `seed` and checks the two runs against each other.
fn check_case(seed: u64) -> (usize, usize) {
    let mut rng = Rng::new(seed);
    let universe = rng.pick(&[50, 200, 1000, 3000]);
    let tree = random_tree(&mut rng, universe, 3);
    let k = rng.pick(&[1, 2, 3, 5, 10, 50]) as usize;
    let scores = tree.scores();

    let plain = plain_run(&tree, k);
    let thresholded = thresholded_run(&tree, k, &scores, seed);

    // The oracle's top K, independent of either run.
    let mut expected: Vec<_> = scores.iter().map(|(&doc, &score)| (doc, score)).collect();
    expected.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    expected.truncate(k);

    assert_eq!(
        plain.top, expected,
        "seed {seed:#x}: the plain run's top {k} differs from the exact scores",
    );
    assert_eq!(
        thresholded.accepted,
        plain.accepted,
        "seed {seed:#x}: the heap accepted different candidates, {} leaves, top {k} \
         (rerun with SCORE_BOUND_SEED={seed})",
        tree.leaves(),
    );
    assert_eq!(
        thresholded.top, plain.top,
        "seed {seed:#x}: different top {k} (rerun with SCORE_BOUND_SEED={seed})",
    );
    // A case may decode a few more entries with the threshold than without it: a skip moves an
    // intersection's first child to a new target, and the children behind it then land on
    // different entries than the plain run's chain of `skip_to`s does. Only the totals are checked.
    (plain.decoded, thresholded.decoded)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().map(|value| {
        let value = value.trim();
        value
            .strip_prefix("0x")
            .map_or_else(|| value.parse(), |hex| u64::from_str_radix(hex, 16))
            .unwrap_or_else(|_| panic!("{name} must be a decimal or 0x-prefixed number"))
    })
}

#[test]
#[cfg_attr(
    miri,
    ignore = "Hundreds of trees over thousands of documents: too slow under Miri"
)]
fn thresholded_top_k_matches_plain_top_k_over_random_trees() {
    if let Some(seed) = env_u64("SCORE_BOUND_SEED") {
        check_case(seed);
        return;
    }
    let cases = env_u64("SCORE_BOUND_CASES").unwrap_or(DEFAULT_CASES);
    let (mut plain, mut thresholded) = (0, 0);
    let (mut cases_with_skips, mut cases_with_overhead) = (0, 0);
    for i in 0..cases {
        let (p, t) = check_case(BASE_SEED.wrapping_add(i));
        plain += p;
        thresholded += t;
        cases_with_skips += usize::from(t < p);
        cases_with_overhead += usize::from(t > p);
    }
    // Not vacuous: the threshold must actually spare decoding in a good share of the cases.
    assert!(
        cases_with_skips * 4 >= cases as usize,
        "only {cases_with_skips} of {cases} cases decoded fewer entries with the threshold",
    );
    assert!(
        thresholded < plain,
        "no entry was spared over {cases} cases"
    );
    println!(
        "{cases} cases: {thresholded} of {plain} entries decoded with the threshold; \
         {cases_with_skips} cases spared some, {cases_with_overhead} decoded a few more",
    );
}
