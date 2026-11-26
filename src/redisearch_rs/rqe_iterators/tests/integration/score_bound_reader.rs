/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! An [`InvIndIterator`] reports the score bounds of its reader and reads with a threshold
//! through it.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use ffi::IndexFlags;
use index_result::RSIndexResult;
use inverted_index::{FilterMaskReader, IndexReader};
use proptest::prelude::*;
use rqe_core::{DocId, FieldMask, RS_FIELDMASK_ALL};
use rqe_iterators::{
    NoOpChecker, RQEIterator,
    inverted_index::InvIndIterator,
    score_bound::{BlockBound, MinCompetitiveScore, can_drop},
};
use rqe_iterators_test_utils::ContractChecker;

use crate::utils::blocked::Block;

/// What a [`BlockedReader`] did, shared with the test that built it.
#[derive(Debug, Default)]
struct ReaderLog {
    /// Every record decoded, in order.
    decoded: Vec<DocId>,
    /// Blocks passed over from their bound alone.
    blocks_skipped: usize,
}

/// A reader over [`Block`]s whose record bound is the record's exact score.
struct BlockedReader {
    blocks: Vec<Block>,
    masks: BTreeMap<DocId, FieldMask>,
    /// Whether [`next_record_with_threshold`](IndexReader::next_record_with_threshold) passes
    /// over blocks from their bound, rather than dropping record by record.
    skips_blocks: bool,
    /// `(block, index)` of the record last read.
    last: Option<(usize, usize)>,
    /// `(block, index)` of the record the next read decodes.
    next: (usize, usize),
    log: Rc<RefCell<ReaderLog>>,
}

impl BlockedReader {
    fn new(blocks: Vec<Block>) -> Self {
        Self {
            blocks,
            masks: BTreeMap::new(),
            skips_blocks: true,
            last: None,
            next: (0, 0),
            log: Rc::default(),
        }
    }

    const fn decoding_every_record(mut self) -> Self {
        self.skips_blocks = false;
        self
    }

    fn with_masks(mut self, masks: impl IntoIterator<Item = (DocId, FieldMask)>) -> Self {
        self.masks = masks.into_iter().collect();
        self
    }

    fn log(&self) -> Rc<RefCell<ReaderLog>> {
        Rc::clone(&self.log)
    }

    fn block_last_doc_id(&self, block: usize) -> DocId {
        self.blocks[block]
            .docs
            .last()
            .expect("blocks are not empty")
            .0
    }

    /// Moves [`next`](Self::next) to the first block that can hold `doc_id`, without decoding.
    fn jump_to_block_of(&mut self, doc_id: DocId) {
        while self.next.0 < self.blocks.len() && self.block_last_doc_id(self.next.0) < doc_id {
            self.next = (self.next.0 + 1, 0);
        }
    }
}

impl IndexReader<'static> for BlockedReader {
    fn next_record(&mut self, result: &mut RSIndexResult<'static>) -> std::io::Result<bool> {
        let (block, index) = self.next;
        if block >= self.blocks.len() {
            return Ok(false);
        }
        let doc_id = self.blocks[block].docs[index].0;
        self.log.borrow_mut().decoded.push(doc_id);
        result.doc_id = doc_id;
        result.field_mask = self.masks.get(&doc_id).copied().unwrap_or(RS_FIELDMASK_ALL);
        self.last = Some((block, index));
        self.next = if index + 1 < self.blocks[block].docs.len() {
            (block, index + 1)
        } else {
            (block + 1, 0)
        };
        Ok(true)
    }

    fn seek_record(
        &mut self,
        doc_id: DocId,
        result: &mut RSIndexResult<'static>,
    ) -> std::io::Result<bool> {
        self.jump_to_block_of(doc_id);
        while self.next_record(result)? {
            if result.doc_id >= doc_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn skip_to(&mut self, doc_id: DocId) -> bool {
        self.jump_to_block_of(doc_id);
        self.next.0 < self.blocks.len()
    }

    fn reset(&mut self) {
        self.last = None;
        self.next = (0, 0);
    }

    fn unique_docs(&self) -> u64 {
        self.blocks
            .iter()
            .map(|block| block.docs.len() as u64)
            .sum()
    }

    fn has_duplicates(&self) -> bool {
        false
    }

    fn flags(&self) -> IndexFlags {
        0
    }

    fn needs_revalidation(&self) -> bool {
        false
    }

    fn reports_score_bounds(&self) -> bool {
        true
    }

    fn current_record_max_score(&self) -> f64 {
        self.last.map_or(f64::INFINITY, |(block, index)| {
            self.blocks[block].docs[index].1
        })
    }

    fn current_block_bound(&self) -> Option<BlockBound> {
        self.last.map(|(block, _)| BlockBound {
            last_doc_id: self.block_last_doc_id(block),
            max_score: self.blocks[block].max_score,
        })
    }

    fn next_record_with_threshold(
        &mut self,
        result: &mut RSIndexResult<'static>,
        min_competitive_score: MinCompetitiveScore,
    ) -> std::io::Result<bool> {
        loop {
            let (block, index) = self.next;
            if block >= self.blocks.len() {
                return Ok(false);
            }
            if self.skips_blocks
                && index == 0
                && can_drop(self.blocks[block].max_score, min_competitive_score)
            {
                self.log.borrow_mut().blocks_skipped += 1;
                self.next = (block + 1, 0);
                continue;
            }
            if !self.next_record(result)? {
                return Ok(false);
            }
            if !can_drop(self.current_record_max_score(), min_competitive_score) {
                return Ok(true);
            }
        }
    }
}

fn iterator<R: IndexReader<'static>>(reader: R) -> InvIndIterator<'static, R, NoOpChecker> {
    InvIndIterator::new(reader, RSIndexResult::build_virt().build(), NoOpChecker)
}

fn threshold(value: f64) -> MinCompetitiveScore {
    MinCompetitiveScore::new(value).expect("test thresholds are positive")
}

/// Every document the iterator yields when read with `min_competitive_score` until exhausted.
fn read_all_with_threshold<'index>(
    it: &mut impl RQEIterator<'index>,
    min_competitive_score: MinCompetitiveScore,
) -> Vec<DocId> {
    let mut ids = Vec::new();
    while let Some(result) = it.read_with_threshold(min_competitive_score).unwrap() {
        ids.push(result.doc_id);
    }
    ids
}

fn read_all<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    let mut ids = Vec::new();
    while let Some(result) = it.read().unwrap() {
        ids.push(result.doc_id);
    }
    ids
}

/// One strong document between two weak blocks.
fn weak_strong_weak() -> Vec<Block> {
    vec![
        Block::tight(&[(1, 0.1), (2, 0.2)]),
        Block::tight(&[(3, 5.0), (4, 0.5)]),
        Block::tight(&[(5, 0.3)]),
    ]
}

#[test]
fn hooks_report_the_reader_bounds() {
    let mut it = iterator(BlockedReader::new(weak_strong_weak()));
    assert!(it.reports_score_bounds());
    assert_eq!(it.current_block_bound(), None, "unread");
    assert_eq!(it.current_max_score(), f64::INFINITY, "unread");

    it.read().unwrap();
    assert_eq!(it.current_max_score(), 0.1);
    assert_eq!(
        it.current_block_bound(),
        Some(BlockBound {
            last_doc_id: 2,
            max_score: 0.2
        }),
    );

    while it.read().unwrap().is_some() {}
    assert_eq!(it.current_block_bound(), None, "exhausted");
}

#[test]
fn plain_read_yields_every_record() {
    let mut it = ContractChecker::new(iterator(BlockedReader::new(weak_strong_weak())));
    assert_eq!(read_all(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
fn thresholded_read_passes_over_blocks_below_the_threshold_without_decoding_them() {
    let reader = BlockedReader::new(weak_strong_weak());
    let log = reader.log();
    let mut it = ContractChecker::new(iterator(reader));

    assert_eq!(read_all_with_threshold(&mut it, threshold(1.0)), [3]);
    let log = log.borrow();
    assert_eq!(log.decoded, [3, 4]);
    assert_eq!(log.blocks_skipped, 2);
}

#[test]
fn thresholded_read_drops_records_through_the_default_reader_method() {
    let reader = BlockedReader::new(weak_strong_weak()).decoding_every_record();
    let log = reader.log();
    let mut it = ContractChecker::new(iterator(reader));

    assert_eq!(read_all_with_threshold(&mut it, threshold(1.0)), [3]);
    assert_eq!(log.borrow().decoded, [1, 2, 3, 4, 5]);
}

#[test]
fn thresholded_read_keeps_a_record_whose_bound_equals_the_threshold() {
    let mut it = ContractChecker::new(iterator(BlockedReader::new(weak_strong_weak())));
    assert_eq!(read_all_with_threshold(&mut it, threshold(0.5)), [3, 4]);
}

#[test]
fn field_mask_reader_forwards_the_bounds_and_still_filters() {
    let reader = BlockedReader::new(vec![
        Block::tight(&[(1, 0.1)]),
        Block::tight(&[(2, 4.0), (3, 6.0), (4, 5.0)]),
    ])
    .with_masks([(2, 0b10), (3, 0b01), (4, 0b11)]);
    let log = reader.log();
    let mut it = ContractChecker::new(iterator(FilterMaskReader::new(0b10, reader)));

    assert!(it.reports_score_bounds());
    assert_eq!(read_all_with_threshold(&mut it, threshold(4.5)), [4]);
    assert_eq!(log.borrow().blocks_skipped, 1);
    assert_eq!(
        it.current_block_bound(),
        None,
        "exhausted, even though the inner reader is still on its last block",
    );
}

#[test]
fn rewind_restarts_a_thresholded_read() {
    let mut it = ContractChecker::new(iterator(BlockedReader::new(weak_strong_weak())));
    assert_eq!(read_all_with_threshold(&mut it, threshold(1.0)), [3]);
    it.rewind();
    assert_eq!(read_all(&mut it), [1, 2, 3, 4, 5]);
}

/// Blocks of strictly increasing ids, with random scores and maxima at least as large.
fn arb_blocks() -> impl Strategy<Value = Vec<Block>> {
    prop::collection::vec(
        (
            prop::collection::vec((1u64..4, 0.0f64..10.0), 1..6),
            0.0f64..3.0,
        ),
        1..8,
    )
    .prop_map(|blocks| {
        let mut next_id = 0;
        blocks
            .into_iter()
            .map(|(docs, slack)| {
                let docs: Vec<_> = docs
                    .into_iter()
                    .map(|(gap, score)| {
                        next_id += gap;
                        (next_id, score)
                    })
                    .collect();
                let max = docs.iter().map(|&(_, score)| score).fold(0.0, f64::max);
                Block::loose(&docs, max + slack)
            })
            .collect()
    })
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    /// A thresholded read returns exactly the plain-read documents that reach the threshold, with
    /// or without block skipping, and with or without a field mask in between.
    #[test]
    #[cfg_attr(miri, ignore = "too slow under Miri")]
    fn thresholded_read_keeps_exactly_the_competitive_records(
        blocks in arb_blocks(),
        threshold_value in 0.01f64..10.0,
        skips_blocks in any::<bool>(),
        masked in any::<bool>(),
    ) {
        let min = threshold(threshold_value);
        let mask_of = |id: DocId| -> FieldMask { if id % 3 == 0 { 0b01 } else { 0b10 } };
        let expected: Vec<DocId> = blocks
            .iter()
            .flat_map(|block| block.docs.iter().copied())
            .filter(|&(id, score)| !can_drop(score, min) && (!masked || mask_of(id) & 0b10 != 0))
            .map(|(id, _)| id)
            .collect();

        let ids: Vec<DocId> = blocks.iter().flat_map(|block| block.docs.iter().map(|&(id, _)| id)).collect();
        let mut reader = BlockedReader::new(blocks).with_masks(ids.iter().map(|&id| (id, mask_of(id))));
        if !skips_blocks {
            reader = reader.decoding_every_record();
        }
        let got = if masked {
            read_all_with_threshold(&mut ContractChecker::new(iterator(FilterMaskReader::new(0b10, reader))), min)
        } else {
            read_all_with_threshold(&mut ContractChecker::new(iterator(reader)), min)
        };
        prop_assert_eq!(got, expected);
    }
}
