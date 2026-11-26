/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Garbage collection of a [`TagIndex<InMemoryMode>`]: removing the postings of deleted
//! documents from each tag value, and the values left without any.
//!
//! A scan and its apply are separate so a forking collector can scan in its child and apply in
//! the parent. A delta is applied only to the posting list it was scanned from, named by its
//! unique id, as the blocks it addresses are positions in that list.

use std::io;
use std::ops::RangeInclusive;

use index_result::RSIndexResult;
use inverted_index::{
    DocId, DocLiveness, GcApplyInfo, GcScanDelta, GcSnapshotScope, InvertedIndex, RepairContext,
    doc_ids_only::DocIdsOnly,
};

use serde::{Deserialize, Serialize};
use trie_rs::iter::{RangeBoundary, RangeFilter};

use crate::{InMemoryMode, TagIndex};

const NO_REPAIR: Option<fn(&RSIndexResult<'_>, &RepairContext<'_>)> = None;

/// The GC work one tag value holds, from [`TagIndex::scan_gc`], serializable so a forking
/// collector can send it from its child.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagValueGcDelta {
    /// The tag value.
    #[serde(with = "serde_bytes")]
    pub tag: Vec<u8>,
    /// The unique id of the posting list scanned, which [`TagIndex::apply_gc`] checks.
    pub list_unique_id: u32,
    /// What to remove from it.
    pub delta: GcScanDelta,
}

/// What [`TagIndex::scan_gc`] found.
#[derive(Debug, Default)]
pub struct TagGcScan {
    /// One delta per value whose posting list holds a gone document.
    pub deltas: Vec<TagValueGcDelta>,
    /// The doc IDs spanned by each posting list whose scan failed, so a collector that
    /// forgets the gone IDs it collected can keep these for a later cycle.
    pub failed: Vec<RangeInclusive<DocId>>,
    /// The values visited, as a measure of the work the scan did.
    pub values_visited: usize,
}

/// One tag value's posting list, copied by [`TagIndex::gc_snapshot_value`] so it can be scanned
/// without the lock that guards the index.
pub struct TagValueSnapshot {
    /// The tag value.
    pub tag: Vec<u8>,
    /// The unique id of the posting list copied, which [`TagIndex::apply_gc`] checks.
    pub list_unique_id: u32,
    /// The copy; see [`InvertedIndex::gc_snapshot`].
    pub snapshot: InvertedIndex<DocIdsOnly>,
}

impl TagValueSnapshot {
    /// Run the GC scan on the copy; `Ok(None)` when it finds nothing to collect, and on failure
    /// the error with the doc IDs the copy spans, whose gone entries stay in the index.
    pub fn scan(
        self,
        live: impl DocLiveness,
    ) -> Result<Option<TagValueGcDelta>, (io::Error, Option<RangeInclusive<DocId>>)> {
        let span = self.snapshot.doc_id_range();
        let delta = self
            .snapshot
            .scan_gc(live, NO_REPAIR)
            .map_err(|error| (error, span))?;
        Ok(delta.map(|delta| TagValueGcDelta {
            tag: self.tag,
            list_unique_id: self.list_unique_id,
            delta,
        }))
    }
}

/// What one [`TagIndex::gc_snapshots_after`] walk did.
#[derive(Debug, Default)]
pub struct TagSnapshotWalk {
    /// The values visited.
    pub visited: usize,
    /// Whether `should_stop` stopped the walk.
    pub stopped: bool,
    /// The last value visited, to resume after, when values are left; `None` once every value
    /// was visited.
    pub resume_after: Option<Vec<u8>>,
}

/// What one [`TagIndex::apply_gc`] changed.
#[derive(Debug)]
pub struct TagGcOutcome {
    /// What the posting list's own GC did.
    pub info: GcApplyInfo,
    /// Set when the posting list was left empty and the value removed.
    pub removed: Option<RemovedTagValue>,
}

/// What removing an emptied tag value freed, beyond [`GcApplyInfo`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemovedTagValue {
    /// The bytes the empty posting list still held.
    pub bytes: usize,
    /// The blocks it still held.
    pub blocks: usize,
}

impl TagIndex<InMemoryMode> {
    /// The GC work of every tag value whose posting list holds a document `live` says is gone.
    ///
    /// A value whose scan fails is skipped and reported in [`TagGcScan::failed`].
    pub fn scan_gc(&self, live: impl DocLiveness + Copy) -> TagGcScan {
        let mut scan = TagGcScan::default();
        let mut it = self.mode.values.iter();
        for (tag, list) in &mut it {
            scan.values_visited += 1;
            match list.scan_gc(live, NO_REPAIR) {
                Ok(Some(delta)) => scan.deltas.push(TagValueGcDelta {
                    tag,
                    list_unique_id: u32::from(list.unique_id()),
                    delta,
                }),
                Ok(None) => {}
                Err(_) => scan.failed.extend(list.doc_id_range()),
            }
        }
        scan
    }

    /// Copy, in key order from after `after` (from the first value without it), the posting
    /// lists of the values that may hold a document `live` says is gone, as
    /// [`TagIndex::gc_snapshot_value`] does, into `found`. `should_stop` is asked after each value
    /// visited, and a `true` stops the walk; returns what the walk did, so a caller can resume
    /// it after the last value visited.
    pub fn gc_snapshots_after(
        &self,
        after: Option<&[u8]>,
        live: &impl DocLiveness,
        should_stop: &mut dyn FnMut() -> bool,
        found: &mut Vec<TagValueSnapshot>,
    ) -> TagSnapshotWalk {
        let filter = RangeFilter {
            min: after.map(|value| RangeBoundary {
                value,
                is_included: false,
            }),
            max: None,
        };
        let mut walk = TagSnapshotWalk::default();
        let mut values = self.mode.values.range_iter(filter).peekable();
        while let Some((tag, list)) = values.next() {
            walk.visited += 1;
            if let Some(snapshot) = list.gc_snapshot(live, GcSnapshotScope::DirtyBlocks) {
                found.push(TagValueSnapshot {
                    list_unique_id: u32::from(list.unique_id()),
                    tag: tag.clone(),
                    snapshot,
                });
            }
            if should_stop() {
                walk.stopped = true;
                if values.peek().is_some() {
                    walk.resume_after = Some(tag);
                }
                break;
            }
        }
        walk
    }

    /// A copy of `tag`'s posting list, if it holds a document `live` says may be gone, its
    /// clean blocks copied without their data; `None` when the value is gone or clean.
    pub fn gc_snapshot_value(
        &self,
        tag: &[u8],
        live: &impl DocLiveness,
    ) -> Option<TagValueSnapshot> {
        let list = self.mode.values.find(tag)?;
        let snapshot = list.gc_snapshot(live, GcSnapshotScope::DirtyBlocks)?;
        Some(TagValueSnapshot {
            tag: tag.to_vec(),
            list_unique_id: u32::from(list.unique_id()),
            snapshot,
        })
    }

    /// The doc IDs each value's posting list spans.
    pub fn doc_id_ranges(&self) -> Vec<RangeInclusive<DocId>> {
        let mut it = self.mode.values.iter();
        (&mut it)
            .filter_map(|(_, list)| list.doc_id_range())
            .collect()
    }

    /// Apply a GC delta to `tag`'s posting list, and remove the value if that leaves it empty.
    ///
    /// Returns `None` without touching anything when the value is gone or its posting list is
    /// not the one `list_unique_id` names, so a delta computed against a replaced list is never
    /// applied.
    ///
    /// With `live`, a last block that grew since the scan is rescanned instead of denied; see
    /// [`InvertedIndex::apply_gc_with`](inverted_index::InvertedIndex::apply_gc_with).
    ///
    /// An emptied value leaves the [suffix index](crate::TagSuffixIndex) too, as the C tag
    /// index's GC removes both.
    pub fn apply_gc(
        &mut self,
        tag: &[u8],
        list_unique_id: u32,
        delta: GcScanDelta,
        live: Option<&dyn DocLiveness>,
    ) -> Option<TagGcOutcome> {
        let list = self.mode.values.find_mut(tag)?;
        if u32::from(list.unique_id()) != list_unique_id {
            return None;
        }
        let info = list.apply_gc_with(delta, live);
        let emptied = (list.unique_docs() == 0).then(|| RemovedTagValue {
            bytes: list.memory_usage(),
            blocks: list.number_of_blocks(),
        });
        if emptied.is_some() {
            self.mode.values.remove(tag);
            if let Some(suffix) = &mut self.suffix {
                suffix.remove(tag);
            }
        }
        Some(TagGcOutcome {
            info,
            removed: emptied,
        })
    }
}
