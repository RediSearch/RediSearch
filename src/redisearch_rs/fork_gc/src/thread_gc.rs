/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! In-process GC driver: runs the fork-GC scanners on the calling thread,
//! under the spec's own read/write lock, instead of in a forked child.
//!
//! A cycle learns which documents are gone from the doc table's
//! [`DeletedDocs`] set and never looks a document up again. For each scanner
//! it then repeats two steps until every key is done:
//!
//! 1. Under a read lock, visit up to [`KEYS_PER_HOLD`] keys in order, resuming
//!    after the last key the previous chunk reached, and copy the blocks of
//!    those that hold deleted documents.
//! 2. Release it, scan the copies, take the write lock once, and apply every
//!    delta that chunk produced.
//!
//! Terms and tag values are walked in key order from a cursor, so no hold has to
//! visit them all; numeric nodes, missing-docs fields and the existing-docs index
//! are few enough to list up front.
//!
//! Releasing the lock between the two steps is sound for the same reasons the
//! fork parent applying a child's stale snapshot is: writers only append to an
//! inverted index, which the delta's last-block check absorbs, and everything
//! else a delta targets is re-validated by the scanner's apply function. One
//! difference: no other GC may run on the spec between a scan and its apply,
//! since a delta addresses blocks by position. The caller must guarantee that
//! by running at most one cycle per spec at a time, forced or scheduled.
//!
//! At the end, the IDs whose entries the cycle collected leave the doc table's
//! set. An ID stays in two cases, so the next cycle looks for its entries
//! again: a last block that grew after its copy was taken and could not be
//! rescanned in place, and a numeric leaf that split since the tree's nodes
//! were listed, copying entries into leaves the cycle never scanned. Any other
//! apply failure means the scanned index is gone and its entries with it: an
//! index is only removed by this GC, and a replacement cannot hold an ID that
//! was deleted before the cycle began.

use std::{collections::HashSet, ffi::CString, ops::RangeInclusive, time::Instant};

use c_trie::TrieTerm;
use deleted_docs::{DeletedDocs, RoaringTreemap};
use index_spec::{IndexSpecReadGuard, IndexSpecWeakRef, IndexSpecWriteGuard};
use inverted_index::{DocId, DocLiveness, opaque::InvertedIndex};
use numeric_range_tree::NodeIndex;

use crate::{
    GcApplyStats, HandleError,
    existing_docs::{apply_existing_docs_with, snapshot_existing_docs},
    missing_docs::{apply_missing_docs_with, missing_field_names, snapshot_missing_field},
    numeric::{
        NumericNodeSnapshot, ScannedNumericNode, apply_numeric_node, compact_tree,
        numeric_tree_keys, ranges_of_nodes_added_since, snapshot_numeric_node,
    },
    stats::GcStatsSink,
    tags::{TagSnapshot, TagsCursor, apply_tag_entry_with, scan_tags_after},
    terms::{TermsCursor, apply_terms_with, scan_terms_after},
    util::SpecWriteAccess,
};

/// Keys one read-lock hold visits before the driver releases the lock to apply
/// what it found. Visiting a clean key costs one range check of the deleted
/// set; copying a key with garbage costs tens of microseconds, so this many
/// keys with garbage keep a hold near a millisecond.
pub const KEYS_PER_HOLD: usize = 64;

/// Provides closure-scoped access to a live, read-locked
/// [`IndexSpec`](index_spec::IndexSpec), next to the write access of
/// [`SpecWriteAccess`].
pub trait SpecLockAccess: SpecWriteAccess {
    /// Promote and read-lock the spec, then run `scan` under the lock.
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, HandleError>;
}

impl SpecLockAccess for IndexSpecWeakRef {
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, HandleError> {
        let spec_ref = self.promote().ok_or(HandleError::SpecDeleted)?;
        let lock = spec_ref.read();
        Ok(scan(&lock))
    }
}

/// How one cycle used the spec lock.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LockReport {
    /// Read-lock holds: one that copies the deleted set, one per scanner that
    /// lists its keys up front (numeric nodes, missing fields, the existing-docs
    /// index), one per scan chunk, and one per numeric tree that checks for
    /// leaves split since its nodes were listed.
    pub read_locks: u64,
    /// Write-lock holds: one per chunk that had deltas to apply, one per numeric
    /// tree compacted, and one to prune the set.
    pub write_locks: u64,
    /// The longest single read-lock hold.
    pub max_read_hold: std::time::Duration,
    /// The longest single write-lock hold.
    pub max_write_hold: std::time::Duration,
}

/// The documents deleted before a cycle began, as the cycle's only source of
/// liveness: a borrowed copy of the doc table's set, so a check needs no doc
/// table and no lock. Documents deleted later are left for the next cycle.
///
/// This rests on two facts about doc IDs: one is never reused, and a document's
/// entries are all written before its ID can be deleted (both under the spec
/// write lock), so a listed ID neither comes back nor gains entries the cycle
/// could miss.
#[derive(Debug, Clone, Copy)]
pub struct DeletedIds<'a>(&'a RoaringTreemap);

impl<'a> DeletedIds<'a> {
    /// Answer liveness questions from `ids`, the IDs known gone.
    pub const fn new(ids: &'a RoaringTreemap) -> Self {
        Self(ids)
    }
}

impl DocLiveness for DeletedIds<'_> {
    fn exists(&self, id: DocId) -> bool {
        !self.0.contains(id)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        self.gone_in_range(first, last) != Some(0)
    }

    fn gone_in_range(&self, first: DocId, last: DocId) -> Option<usize> {
        // The set keeps one 32-bit bitmap per high half of the ID; a range almost
        // always falls within one of them.
        let split = |id: DocId| ((id >> 32) as u32, id as u32);
        let ((hi_first, lo_first), (hi_last, lo_last)) = (split(first), split(last));
        let count: u64 = self
            .0
            .bitmaps()
            .filter(|(hi, _)| (hi_first..=hi_last).contains(hi))
            .map(|(hi, bitmap)| {
                let from = if hi == hi_first { lo_first } else { 0 };
                let to = if hi == hi_last { lo_last } else { u32::MAX };
                bitmap.range_cardinality(from..=to)
            })
            .sum();
        Some(count as usize)
    }
}

/// The doc IDs a scanned index's last block spanned when it was copied: what a
/// denied delta leaves uncollected.
type Tail = Option<RangeInclusive<DocId>>;

struct Driver<'a, S, G> {
    spec: &'a mut S,
    sink: &'a mut G,
    report: LockReport,
    deleted: DeletedIds<'a>,
    /// Deleted IDs this cycle could not collect every entry of; they stay in the set.
    retained: RoaringTreemap,
}

type StopCheck<'s> = &'s mut dyn FnMut() -> bool;

/// A scan-chunk function over a key list made up front: resumes where the
/// previous chunk stopped. Returns whether keys remain.
fn keyed<'k, K, E>(
    keys: &'k [K],
    scan: impl Fn(&IndexSpecReadGuard<'_>, &K) -> Option<E> + 'k,
) -> impl FnMut(&IndexSpecReadGuard<'_>, StopCheck<'_>, &mut Vec<E>) -> bool + 'k {
    let mut next = 0;
    move |guard, should_stop, found| {
        while let Some(key) = keys.get(next) {
            next += 1;
            if let Some(entry) = scan(guard, key) {
                found.push(entry);
            }
            if should_stop() {
                return next < keys.len();
            }
        }
        false
    }
}

impl<S: SpecLockAccess, G: GcStatsSink> Driver<'_, S, G> {
    fn read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, HandleError> {
        let result = self.spec.with_read(|guard| {
            let start = Instant::now();
            let value = scan(guard);
            (value, start.elapsed())
        });
        let (value, held) = result?;
        self.report.read_locks += 1;
        self.report.max_read_hold = self.report.max_read_hold.max(held);
        Ok(value)
    }

    fn write(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>, &mut GcApplyStats) -> Result<(), HandleError>,
    ) -> Result<(), HandleError> {
        let mut stats = GcApplyStats::default();
        let result = self.spec.with_write(|guard| {
            let start = Instant::now();
            let result = apply(guard, &mut stats);
            stats.apply_to_spec(guard);
            Ok((result, start.elapsed()))
        });
        let (result, held) = result?;
        self.report.write_locks += 1;
        self.report.max_write_hold = self.report.max_write_hold.max(held);
        if stats != GcApplyStats::default() {
            self.sink.record(&stats);
        }
        result
    }

    /// Run one scanner to completion: `scan_chunk` copies the next keys under a
    /// read lock and says whether any remain; `process` turns a copy into a delta
    /// and the ID span of its last block, or `Ok(None)` when the copy is clean;
    /// `apply` lands a delta under the write lock. Returns whether any delta was
    /// applied.
    fn run<F, E>(
        &mut self,
        scanner: &'static str,
        mut scan_chunk: impl FnMut(&IndexSpecReadGuard<'_>, StopCheck<'_>, &mut Vec<F>) -> bool,
        mut process: impl FnMut(F) -> std::io::Result<Option<(E, Tail)>>,
        mut apply: impl FnMut(E, &mut IndexSpecWriteGuard<'_>) -> Result<GcApplyStats, HandleError>,
    ) -> Result<bool, HandleError> {
        let mut had_work = false;

        loop {
            let (found, more) = self.read(|guard| {
                let mut visited = 0;
                let mut should_stop = || {
                    visited += 1;
                    visited >= KEYS_PER_HOLD
                };
                let mut found = Vec::new();
                let more = scan_chunk(guard, &mut should_stop, &mut found);
                (found, more)
            })?;

            let mut entries: Vec<(E, Tail)> = Vec::new();
            for copy in found {
                match process(copy) {
                    Ok(Some(entry)) => entries.push(entry),
                    Ok(None) => {}
                    // The live index would not decode either; a cycle cannot repair that.
                    Err(error) => {
                        tracing::warn!(scanner, %error, "thread GC: an index copy would not decode")
                    }
                }
            }
            if !entries.is_empty() {
                had_work = true;
                let mut retained = std::mem::take(&mut self.retained);
                let result = self.write(|guard, stats| {
                    for (entry, tail) in entries {
                        match apply(entry, guard) {
                            Ok(entry_stats) => {
                                if entry_stats.blocks_denied > 0
                                    && let Some(tail) = tail
                                {
                                    retained.insert_range(tail);
                                }
                                stats.record(entry_stats);
                            }
                            Err(HandleError::ApplyError(reason)) => {
                                tracing::debug!(scanner, reason, "thread GC skipped a stale delta");
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(())
                });
                self.retained = retained;
                result?;
            }

            if !more {
                return Ok(had_work);
            }
        }
    }

    fn terms(&mut self) -> Result<(), HandleError> {
        let mut cursor = TermsCursor::default();
        let live = self.deleted;
        self.run(
            "terms",
            |guard, should_stop, found| {
                scan_terms_after(guard, &mut cursor, live, should_stop, found)
            },
            |(term, snapshot): (TrieTerm, InvertedIndex)| {
                let tail = snapshot.last_block_doc_id_range();
                let unique_id = u32::from(snapshot.unique_id());
                Ok(snapshot
                    .scan_gc(live)?
                    .map(|delta| ((term, unique_id, delta), tail)))
            },
            |(term, unique_id, delta), guard| {
                apply_terms_with(&term, unique_id, delta, Some(&live), guard)
            },
        )?;
        Ok(())
    }

    fn numeric(&mut self, clean_numeric_empty_nodes: bool) -> Result<(), HandleError> {
        // A tree has far fewer nodes than entries, so listing them in one hold is cheap.
        let trees = self.read(numeric_tree_keys)?;
        let live = self.deleted;
        for tree in &trees {
            let resolved = tree.resolved();
            let had_work = self.run(
                "numeric",
                keyed(&tree.nodes, |guard, &node_idx: &NodeIndex| {
                    snapshot_numeric_node(guard, tree, node_idx, &live)
                }),
                |snapshot: NumericNodeSnapshot| {
                    let tail = snapshot.last_block_doc_id_range();
                    Ok(snapshot
                        .scan(live)?
                        .map(|ScannedNumericNode { node, .. }| (node, tail)))
                },
                |node, guard| {
                    let tree = resolved.get_mut(guard)?;
                    // A node gone by now was trimmed empty or split; a split's new
                    // leaves are retained below.
                    Ok(apply_numeric_node(tree, node).unwrap_or(GcApplyStats {
                        numeric_nodes_missed: 1,
                        ..GcApplyStats::default()
                    }))
                },
            )?;

            // A leaf split since the nodes were listed copied its entries, deleted ones
            // included, into leaves this cycle never scanned; their IDs stay in the set.
            let listed: HashSet<NodeIndex> = tree.nodes.iter().copied().collect();
            for range in self.read(|guard| ranges_of_nodes_added_since(guard, tree, &listed))? {
                self.retained.insert_range(range);
            }

            if clean_numeric_empty_nodes && had_work {
                self.write(|guard, stats| {
                    match compact_tree(guard, resolved) {
                        Ok(compacted) => stats.record(compacted),
                        Err(HandleError::ApplyError(reason)) => {
                            tracing::debug!(reason, "thread GC skipped compacting a numeric tree");
                        }
                        Err(error) => return Err(error),
                    }
                    Ok(())
                })?;
            }
        }
        Ok(())
    }

    fn tags(&mut self) -> Result<(), HandleError> {
        let mut cursor = TagsCursor::default();
        let live = self.deleted;
        self.run(
            "tags",
            |guard, should_stop, found| {
                scan_tags_after(guard, &mut cursor, live, should_stop, found)
            },
            |snapshot: TagSnapshot| {
                let tail = snapshot.snapshot.last_block_doc_id_range();
                Ok(snapshot.scan(live)?.map(|entry| (entry, tail)))
            },
            |entry, guard| apply_tag_entry_with(entry, Some(&live), guard),
        )?;
        Ok(())
    }

    fn missing_docs(&mut self) -> Result<(), HandleError> {
        // One entry per INDEXMISSING field, so the list is short.
        let keys: Vec<CString> = self.read(missing_field_names)?;
        if keys.is_empty() {
            return Ok(());
        }
        let live = self.deleted;
        self.run(
            "missing docs",
            keyed(&keys, |guard, field_name: &CString| {
                snapshot_missing_field(guard, field_name, &live)
                    .map(|snapshot| (field_name.clone(), snapshot))
            }),
            |(field_name, snapshot): (CString, InvertedIndex)| {
                let tail = snapshot.last_block_doc_id_range();
                Ok(snapshot
                    .scan_gc(live)?
                    .map(|delta| ((field_name, delta), tail)))
            },
            |(field_name, delta), guard| {
                apply_missing_docs_with(&field_name, delta, Some(&live), guard)
            },
        )?;
        Ok(())
    }

    fn existing_docs(&mut self) -> Result<(), HandleError> {
        let live = self.deleted;
        self.run(
            "existing docs",
            keyed(&[()], move |guard, ()| snapshot_existing_docs(guard, &live)),
            |snapshot: InvertedIndex| {
                let tail = snapshot.last_block_doc_id_range();
                Ok(snapshot.scan_gc(live)?.map(|delta| (delta, tail)))
            },
            |delta, guard| apply_existing_docs_with(delta, Some(&live), guard),
        )?;
        Ok(())
    }
}

/// Run one GC cycle over `spec`: collect the entries of every document in the
/// doc table's [`DeletedDocs`] set, then drop the collected IDs from it. See the
/// [module docs](self) for the lock discipline. On error, names the scanner
/// that failed; a spec dropped mid-cycle ends the cycle with
/// [`HandleError::SpecDeleted`], keeping what was already applied.
pub fn run_cycle(
    spec: &mut impl SpecLockAccess,
    sink: &mut impl GcStatsSink,
    clean_numeric_empty_nodes: bool,
) -> Result<LockReport, (&'static str, HandleError)> {
    let mut report = LockReport::default();
    let (dead, held) = spec
        .with_read(|guard| {
            let start = Instant::now();
            (
                guard.deleted_docs().map(DeletedDocs::snapshot),
                start.elapsed(),
            )
        })
        .map_err(|e| ("deleted docs", e))?;
    report.read_locks += 1;
    report.max_read_hold = held;

    // A table that does not track deletions has nothing for this GC to work from.
    debug_assert!(
        dead.is_some(),
        "a THREAD GC cycle ran on a table without a deleted set"
    );
    let dead = match dead {
        // Nothing to collect, and nothing to tell the doc table.
        Some(dead) if !dead.is_empty() => dead,
        _ => return Ok(report),
    };

    let mut driver = Driver {
        spec,
        sink,
        report,
        deleted: DeletedIds(&dead),
        retained: RoaringTreemap::new(),
    };

    driver.terms().map_err(|e| ("terms", e))?;
    driver
        .numeric(clean_numeric_empty_nodes)
        .map_err(|e| ("numeric", e))?;
    driver.tags().map_err(|e| ("tags", e))?;
    driver.missing_docs().map_err(|e| ("missing docs", e))?;
    driver.existing_docs().map_err(|e| ("existing docs", e))?;

    let Driver {
        spec,
        mut report,
        retained,
        ..
    } = driver;
    let collected = &dead - retained;
    spec.with_write(|guard| {
        if let Some(set) = guard.deleted_docs_mut() {
            set.prune(&collected);
        }
        Ok(())
    })
    .map_err(|e| ("deleted docs", e))?;
    report.write_locks += 1;
    Ok(report)
}
