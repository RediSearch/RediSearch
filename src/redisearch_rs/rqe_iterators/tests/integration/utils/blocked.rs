/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A leaf laid out in storage blocks with per-block score maxima, standing in for a disk term
//! leaf when testing score-bound window skipping.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use index_result::{RSIndexResult, RSOffsetSlice};
use index_spec::IndexSpecReadGuard;
use proptest::prelude::*;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
use rqe_iterators::{
    Intersection, IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError,
    RQESuspendedIterator, RQEValidateStatus, ResumeOutcome, SkipToOutcome, UnionFullFlat,
    UnionFullHeap,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, MinCompetitiveScore, can_drop},
};

use super::past_end_cursor;

/// One storage block of a [`Blocked`] leaf.
#[derive(Debug, Clone)]
pub(crate) struct Block {
    /// Documents in increasing id order, each with its exact score.
    pub(crate) docs: Vec<(DocId, f64)>,
    /// At least every score in [`docs`](Self::docs); looser than the largest one when the
    /// storage quantizes it.
    pub(crate) max_score: f64,
}

impl Block {
    /// A block whose maximum is exactly its largest score.
    pub(crate) fn tight(docs: &[(DocId, f64)]) -> Self {
        let max_score = docs.iter().map(|&(_, score)| score).fold(0.0, f64::max);
        Self::loose(docs, max_score)
    }

    /// A block whose maximum may exceed its largest score.
    pub(crate) fn loose(docs: &[(DocId, f64)], max_score: f64) -> Self {
        assert!(!docs.is_empty(), "a block holds at least one document");
        assert!(
            docs.iter().all(|&(_, score)| score <= max_score),
            "the block maximum must bound every score in it",
        );
        Self {
            docs: docs.to_vec(),
            max_score,
        }
    }

    /// The same score for every id in `ids`.
    pub(crate) fn uniform(ids: impl IntoIterator<Item = DocId>, score: f64) -> Self {
        let docs: Vec<_> = ids.into_iter().map(|id| (id, score)).collect();
        Self::tight(&docs)
    }

    fn last_doc_id(&self) -> DocId {
        self.docs.last().expect("blocks are not empty").0
    }
}

/// What a [`Blocked`] leaf was asked to do, shared with the test that built it.
#[derive(Debug, Default)]
pub(crate) struct BlockedLog {
    pub(crate) reads: usize,
    pub(crate) thresholded_reads: usize,
    /// The threshold of the latest [`read_with_threshold`](RQEIterator::read_with_threshold), or
    /// `None` if the leaf never received one.
    pub(crate) last_threshold: Option<MinCompetitiveScore>,
    pub(crate) skip_tos: usize,
    /// Every document the leaf positioned on, in order. A document in a block that a skip jumped
    /// over, or that a skip inside a block stepped over, is not visited.
    pub(crate) visited: Vec<DocId>,
}

/// A leaf over [`Block`]s that implements every score-bound hook: its document bound is the exact
/// score, its block bound the [`Block::max_score`] of the block it stands in, and its
/// [`read_with_threshold`](RQEIterator::read_with_threshold) jumps over whole blocks whose maximum
/// cannot reach the threshold, then steps over the documents of a kept block that cannot.
///
/// `#[repr(C)]` so the layout matches [`BlockedSuspended`] for the `suspend`/`resume` pointer
/// relabel.
#[repr(C)]
pub(crate) struct Blocked<'index> {
    blocks: Vec<Block>,
    /// `(block index, index in block)` of every document, in id order.
    entries: Vec<(usize, usize)>,
    /// Index into `entries` of the next document to position on, or [`past_end_cursor`].
    next: usize,
    reports_blocks: bool,
    /// One term position per document, in id order, when the leaf yields term results.
    positions: Option<Vec<u8>>,
    result: RSIndexResult<'index>,
    log: Rc<RefCell<BlockedLog>>,
}

impl Blocked<'_> {
    pub(crate) fn new(blocks: Vec<Block>) -> Self {
        let entries: Vec<_> = blocks
            .iter()
            .enumerate()
            .flat_map(|(b, block)| (0..block.docs.len()).map(move |i| (b, i)))
            .collect();
        let ids: Vec<DocId> = entries.iter().map(|&(b, i)| blocks[b].docs[i].0).collect();
        assert!(
            ids.windows(2).all(|pair| pair[0] < pair[1]),
            "doc ids must strictly ascend across blocks: {ids:?}",
        );
        assert!(ids.first().is_none_or(|&id| id > 0), "doc id 0 is reserved");
        Self {
            blocks,
            entries,
            next: 0,
            reports_blocks: true,
            positions: None,
            result: RSIndexResult::build_virt().build(),
            log: Rc::default(),
        }
    }

    /// A leaf of one block holding `ids`, every one bounded by `bound`.
    pub(crate) fn single(ids: impl IntoIterator<Item = DocId>, bound: f64) -> Self {
        Self::new(vec![Block::uniform(ids, bound)])
    }

    /// The same leaf, but reporting no block boundary, as a leaf without block metadata does.
    pub(crate) const fn without_block_bounds(mut self) -> Self {
        self.reports_blocks = false;
        self
    }

    /// The same leaf, but yielding a term result carrying `positions[i]` as the only offset of
    /// its `i`-th document, so that a slop or in-order intersection can check it.
    pub(crate) fn with_positions(mut self, positions: &[u8]) -> Self {
        assert_eq!(
            positions.len(),
            self.entries.len(),
            "one position per document"
        );
        assert!(
            positions.iter().all(|&p| (1..=127).contains(&p)),
            "positions must fit a single-byte varint",
        );
        self.positions = Some(positions.to_vec());
        self
    }

    pub(crate) fn log(&self) -> Rc<RefCell<BlockedLog>> {
        Rc::clone(&self.log)
    }

    fn past_end(&self) -> bool {
        self.next == past_end_cursor(self.entries.len())
    }

    /// The entry the leaf stands on, if it is positioned.
    fn position(&self) -> Option<(usize, usize)> {
        (!self.past_end() && self.next > 0).then(|| self.entries[self.next - 1])
    }

    /// Index into `entries` of the first document of the block after the one holding
    /// `entries[from]`.
    fn next_block_start(&self, from: usize) -> usize {
        let block = self.entries[from].0;
        from + self.entries[from..]
            .iter()
            .take_while(|&&(b, _)| b == block)
            .count()
    }

    /// Positions on `entries[at]`, or past the end when `at` runs off it.
    fn land(&mut self, at: usize) -> Option<DocId> {
        if at >= self.entries.len() {
            self.next = past_end_cursor(self.entries.len());
            return None;
        }
        let (b, i) = self.entries[at];
        let doc_id = self.blocks[b].docs[i].0;
        self.next = at + 1;
        if let Some(positions) = &self.positions {
            let offsets = RSOffsetSlice::from_slice(&positions[at..=at]).to_owned();
            self.result = RSIndexResult::build_term()
                .doc_id(doc_id)
                .weight(1.)
                .field_mask(RS_FIELDMASK_ALL)
                .owned_record(None, offsets)
                .build();
        }
        self.result.doc_id = doc_id;
        self.log.borrow_mut().visited.push(doc_id);
        Some(doc_id)
    }
}

impl<'index> RQEIterator<'index> for Blocked<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.position().map(|_| &mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.log.borrow_mut().reads += 1;
        if self.past_end() {
            return Ok(None);
        }
        Ok(self.land(self.next).map(|_| &mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.log.borrow_mut().skip_tos += 1;
        assert!(doc_id > self.result.doc_id, "skip_to must move forward");
        if self.past_end() {
            return Ok(None);
        }
        let mut at = self.next;
        // Whole blocks ending before the target are jumped without being visited.
        while at < self.entries.len() && self.blocks[self.entries[at].0].last_doc_id() < doc_id {
            at = self.next_block_start(at);
        }
        while at < self.entries.len() {
            let (b, i) = self.entries[at];
            if self.blocks[b].docs[i].0 >= doc_id {
                break;
            }
            at += 1;
        }
        Ok(self.land(at).map(|landed| {
            if landed == doc_id {
                SkipToOutcome::Found(&mut self.result)
            } else {
                SkipToOutcome::NotFound(&mut self.result)
            }
        }))
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: MinCompetitiveScore,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        {
            let mut log = self.log.borrow_mut();
            log.thresholded_reads += 1;
            log.last_threshold = Some(min_competitive_score);
        }
        if self.past_end() {
            return Ok(None);
        }
        let mut at = self.next;
        while at < self.entries.len() {
            let (b, i) = self.entries[at];
            if can_drop(self.blocks[b].max_score, min_competitive_score) {
                at = self.next_block_start(at);
            } else if can_drop(self.blocks[b].docs[i].1, min_competitive_score) {
                at += 1;
            } else {
                break;
            }
        }
        Ok(self.land(at).map(|_| &mut self.result))
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    fn rewind(&mut self) {
        self.next = 0;
        self.result.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        self.entries.len()
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.past_end()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }

    fn reports_score_bounds(&self) -> bool {
        true
    }

    fn current_max_score(&self) -> f64 {
        self.position()
            .map_or(f64::INFINITY, |(b, i)| self.blocks[b].docs[i].1)
    }

    fn current_block_bound(&self) -> Option<BlockBound> {
        if !self.reports_blocks {
            return None;
        }
        self.position().map(|(b, _)| BlockBound {
            last_doc_id: self.blocks[b].last_doc_id(),
            max_score: self.blocks[b].max_score,
        })
    }
}

/// Suspended counterpart of [`Blocked`], layout-identical to it: the leaf holds no index data, so
/// the lifetime only ever names a result over owned data, virtual or an owned term record.
#[repr(C)]
pub(crate) struct BlockedSuspended {
    blocks: Vec<Block>,
    entries: Vec<(usize, usize)>,
    next: usize,
    reports_blocks: bool,
    positions: Option<Vec<u8>>,
    result: RSIndexResult<'static>,
    log: Rc<RefCell<BlockedLog>>,
}

// Compile-time proof that `Blocked` and `BlockedSuspended` are layout-identical.
const _: () = {
    use std::mem::offset_of;

    type A<'a> = Blocked<'a>;
    type S = BlockedSuspended;

    assert!(offset_of!(A, blocks) == offset_of!(S, blocks));
    assert!(offset_of!(A, entries) == offset_of!(S, entries));
    assert!(offset_of!(A, next) == offset_of!(S, next));
    assert!(offset_of!(A, reports_blocks) == offset_of!(S, reports_blocks));
    assert!(offset_of!(A, positions) == offset_of!(S, positions));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, log) == offset_of!(S, log));

    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index> RQEIteratorBoxed<'index> for Blocked<'index> {
    type Suspended = BlockedSuspended;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `Blocked` and `BlockedSuspended` are layout-identical, and the result, virtual
        // or an owned term record without a query term, borrows nothing for `'index`.
        unsafe { Box::from_raw(raw.cast::<BlockedSuspended>()) }
    }
}

impl<'query> RQESuspendedIterator<'query> for BlockedSuspended {
    type Resumed<'index>
        = Blocked<'index>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let raw = Box::into_raw(self);
        // SAFETY: As in `suspend`.
        Ok(ResumeOutcome::Ok(unsafe {
            Box::from_raw(raw.cast::<Blocked<'index>>())
        }))
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.entries.len()
    }
}

impl ProfilePrint for BlockedSuspended {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, _ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}

impl ProfilePrint for Blocked<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, _ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}

/// Drains `it` under `min_competitive_score`, as the index result processor reads a query root,
/// checking that nothing comes back below it.
pub(crate) fn drain_above<'index>(
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

/****************************** random trees ******************************/

/// A leaf to build: its blocks, and whether it reports their boundaries.
#[derive(Debug, Clone)]
pub(crate) struct LeafSpec {
    pub(crate) blocks: Vec<Block>,
    pub(crate) reports_blocks: bool,
}

/// A query tree over [`Blocked`] leaves, from which both the iterator and the exact score of
/// every match are derived.
#[derive(Debug, Clone)]
pub(crate) enum Tree {
    Leaf(LeafSpec),
    And(Vec<Tree>),
    Or { children: Vec<Tree>, heap: bool },
}

impl Tree {
    /// Builds the iterator, collecting the logs of its leaves.
    pub(crate) fn build(
        &self,
        logs: &mut Vec<Rc<RefCell<BlockedLog>>>,
    ) -> Box<dyn RQEIterator<'static>> {
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
    /// intersection sums all its children, a union those that match. This is the oracle a
    /// correct result is checked against.
    pub(crate) fn scores(&self) -> BTreeMap<DocId, f64> {
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

/// A random leaf of up to `max_docs` documents among `1..=max_doc_id`, cut into blocks at random.
/// Scores are multiples of a power of two, so sums are exact in `f64` and no tie is broken
/// differently by rounding in two runs over the same tree. Every third block reports a maximum
/// looser than its scores, as a quantized one does.
pub(crate) fn leaf_spec(max_doc_id: DocId, max_docs: usize) -> impl Strategy<Value = Tree> {
    let docs = proptest::collection::btree_map(1..=max_doc_id, 0u8..=8, 1..=max_docs);
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

/// A random tree of intersections and full unions, up to two composite levels deep, over leaves
/// drawn by [`leaf_spec`].
pub(crate) fn tree(max_doc_id: DocId, max_docs: usize) -> impl Strategy<Value = Tree> {
    leaf_spec(max_doc_id, max_docs).prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 2..=3).prop_map(Tree::And),
            (proptest::collection::vec(inner, 2..=3), any::<bool>())
                .prop_map(|(children, heap)| Tree::Or { children, heap }),
        ]
    })
}
