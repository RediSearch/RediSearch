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

use std::{cell::RefCell, rc::Rc};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, can_drop},
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
    pub(crate) skip_tos: usize,
    /// Every document the leaf positioned on, in order. A document in a block that a skip jumped
    /// over, or that a skip inside a block stepped over, is not visited.
    pub(crate) visited: Vec<DocId>,
}

/// A leaf over [`Block`]s that implements every score-bound hook: its document bound is the exact
/// score, its block bound the [`Block::max_score`] of the block it stands in, and its
/// [`read_with_threshold`](RQEIterator::read_with_threshold) jumps over whole blocks whose maximum
/// cannot reach the threshold.
pub(crate) struct Blocked {
    blocks: Vec<Block>,
    /// `(block index, index in block)` of every document, in id order.
    entries: Vec<(usize, usize)>,
    /// Index into `entries` of the next document to position on, or [`past_end_cursor`].
    next: usize,
    reports_blocks: bool,
    result: RSIndexResult<'static>,
    log: Rc<RefCell<BlockedLog>>,
}

impl Blocked {
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
            result: RSIndexResult::build_virt().build(),
            log: Rc::default(),
        }
    }

    /// The same leaf, but reporting no block boundary, as a leaf without block metadata does.
    pub(crate) const fn without_block_bounds(mut self) -> Self {
        self.reports_blocks = false;
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
        self.result.doc_id = doc_id;
        self.log.borrow_mut().visited.push(doc_id);
        Some(doc_id)
    }
}

impl RQEIterator<'static> for Blocked {
    fn current(&mut self) -> Option<&mut RSIndexResult<'static>> {
        self.position().map(|_| &mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'static>>, RQEIteratorError> {
        self.log.borrow_mut().reads += 1;
        if self.past_end() {
            return Ok(None);
        }
        Ok(self.land(self.next).map(|_| &mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'static>>, RQEIteratorError> {
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
        min_competitive_score: f64,
    ) -> Result<Option<&mut RSIndexResult<'static>>, RQEIteratorError> {
        self.log.borrow_mut().thresholded_reads += 1;
        if self.past_end() {
            return Ok(None);
        }
        let mut at = self.next;
        while at < self.entries.len()
            && can_drop(
                self.blocks[self.entries[at].0].max_score,
                min_competitive_score,
            )
        {
            at = self.next_block_start(at);
        }
        Ok(self.land(at).map(|_| &mut self.result))
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'static>, RQEIteratorError> {
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

impl ProfilePrint for Blocked {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, _ctx: &mut ProfilePrintCtx<'_>) {
        map.kv_simple_string(c"Type", c"MOCK");
    }
}
