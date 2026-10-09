/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`NumericScoreBatch`] — a [`ScoreBatch`] over a chunk of value-ordered ranges.
//!
//! [`ScoreBatch`]: top_k::ScoreBatch

use index_result::RSIndexResult;
use inverted_index::{FilterNumericReader, IndexReader, NumericFilter};
use numeric_range_tree::{NumericIndexReader, NumericRange};
use rqe_core::DocId;
use rqe_iterators::RQEIteratorError;

/// The records of a chunk of ranges, merged lazily in doc-id order, so a
/// seek skips the index blocks that hold no candidate. A multivalue doc's
/// entries are folded onto its best score.
pub struct NumericScoreBatch<'index> {
    cursors: Vec<RangeCursor<'index>>,
    /// Cursors on the doc id yielded last move on at the next read, straight
    /// to its target.
    last: DocId,
    ascending: bool,
    multivalued: bool,
}

struct RangeCursor<'index> {
    reader: FilterNumericReader<NumericIndexReader<'index>>,
    record: RSIndexResult<'index>,
}

impl<'index> NumericScoreBatch<'index> {
    /// Open a reader on each of `ranges`, keeping only records `filter` admits.
    pub(crate) fn new(
        ranges: &[&'index NumericRange],
        filter: NumericFilter,
        multivalued: bool,
    ) -> Self {
        let cursors = ranges
            .iter()
            .map(|range| RangeCursor {
                reader: FilterNumericReader::new(filter, range.reader()),
                record: RSIndexResult::build_numeric(0.0).build(),
            })
            .collect();
        Self {
            cursors,
            last: 0,
            ascending: filter.ascending,
            multivalued,
        }
    }

    /// Yield the first `(doc_id, score)` with `doc_id >= target`, or `Ok(None)`
    /// once every range is exhausted.
    #[inline(always)]
    pub(crate) fn read(&mut self, target: DocId) -> Result<Option<(DocId, f64)>, RQEIteratorError> {
        let target = target.max(self.last + 1);
        let mut min: Option<(DocId, usize)> = None;
        let mut i = 0;
        while i < self.cursors.len() {
            let cursor = &mut self.cursors[i];
            if cursor.record.doc_id < target
                && !cursor.reader.seek_record(target, &mut cursor.record)?
            {
                self.cursors.swap_remove(i);
                continue;
            }
            let doc_id = cursor.record.doc_id;
            if min.is_none_or(|(min_id, _)| doc_id < min_id) {
                min = Some((doc_id, i));
            }
            i += 1;
        }
        let Some((doc_id, at)) = min else {
            return Ok(None);
        };
        self.last = doc_id;
        let score = if self.multivalued {
            self.consume(doc_id)?
        } else {
            self.cursors[at]
                .record
                .as_numeric()
                .expect("numeric range yields numeric records")
        };
        Ok(Some((doc_id, score)))
    }

    /// Move every cursor past `doc_id`, returning its best score.
    fn consume(&mut self, doc_id: DocId) -> Result<f64, RQEIteratorError> {
        let ascending = self.ascending;
        let mut best: Option<f64> = None;
        let mut i = 0;
        while i < self.cursors.len() {
            let cursor = &mut self.cursors[i];
            let mut live = true;
            while live && cursor.record.doc_id == doc_id {
                let score = cursor
                    .record
                    .as_numeric()
                    .expect("numeric range yields numeric records");
                best = Some(best.map_or(score, |best| {
                    if ascending {
                        best.min(score)
                    } else {
                        best.max(score)
                    }
                }));
                live = cursor.reader.next_record(&mut cursor.record)?;
            }
            if live {
                i += 1;
            } else {
                self.cursors.swap_remove(i);
            }
        }
        Ok(best.expect("`doc_id` is the record of some cursor"))
    }
}
