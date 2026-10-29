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
//! [`DeletedDocs`] set, or from a sweep of the table
//! when that set does not cover every dead entry, and never looks a document
//! up again. For each scanner it then repeats two steps until every key is done:
//!
//! 1. Under a read lock, copy the blocks of keys in order that hold deleted
//!    documents, resuming after the last key the previous chunk reached, until
//!    [`ScanBudget`] has elapsed.
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
//! set. An ID stays when a delta covering it could not be applied, or when a
//! numeric leaf split since its nodes were listed copied entries into leaves
//! the cycle never scanned, so the next cycle looks for its entries again.

use std::{
    collections::HashSet,
    ffi::CString,
    ops::RangeInclusive,
    time::{Duration, Instant},
};

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

/// Doc IDs one [`IndexSpecReadGuard::collect_dead_ids`] call examines before the
/// budget is checked again.
const TABLE_IDS_PER_STEP: usize = 1 << 12;

/// How long one read-lock hold may keep scanning before the driver releases
/// the lock to apply what it found.
///
/// The check runs after each key, so one hold can overrun by the time it takes
/// to copy a single inverted index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanBudget(pub Duration);

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
    /// Read-lock holds: one that reads the deleted set, the holds of a doc-table
    /// sweep, one per scanner that lists its keys up front (numeric nodes, missing
    /// fields, the existing-docs index), one per scan chunk, and one per numeric
    /// tree that checks for leaves split since its nodes were listed.
    pub read_locks: u64,
    /// Write-lock holds: one that creates a missing deleted set, one per chunk that
    /// had deltas to apply, one per numeric tree compacted, and one to prune the set.
    pub write_locks: u64,
    /// The longest single read-lock hold.
    pub max_read_hold: Duration,
    /// The longest single write-lock hold.
    pub max_write_hold: Duration,
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

    /// Collect every ID up to the table's highest that has no live document,
    /// sweeping the table under budgeted read locks.
    ///
    /// For a cycle whose deleted set is [`Coverage::Partial`](deleted_docs::Coverage::Partial).
    /// Documents added while the sweep is in progress have IDs beyond the highest
    /// seen; ones deleted meanwhile are recorded in the set for the next cycle.
    fn sweep_table(
        spec: &mut impl SpecLockAccess,
        budget: ScanBudget,
        report: &mut LockReport,
    ) -> Result<RoaringTreemap, HandleError> {
        let mut dead = RoaringTreemap::new();
        let mut buf = vec![0; TABLE_IDS_PER_STEP];
        let mut next: DocId = 1;
        let mut last = None;
        loop {
            let (more, held) = spec.with_read(|guard| {
                let last = *last.get_or_insert_with(|| guard.max_doc_id());
                let start = Instant::now();
                // At least one step per hold, so a zero budget still advances.
                loop {
                    let n = guard.collect_dead_ids(&mut next, last, &mut buf);
                    dead.extend(&buf[..n]);
                    if next > last {
                        return (false, start.elapsed());
                    }
                    if start.elapsed() >= budget.0 {
                        return (true, start.elapsed());
                    }
                }
            })?;
            report.read_locks += 1;
            report.max_read_hold = report.max_read_hold.max(held);
            if !more {
                return Ok(dead);
            }
        }
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

/// The doc IDs a scanned index spanned, so the driver knows which deleted IDs a
/// delta it could not apply leaves in the index.
#[derive(Debug, Clone)]
struct Spans {
    /// The whole index.
    all: Option<RangeInclusive<DocId>>,
    /// Its last block, the part an apply may deny on its own.
    tail: Option<RangeInclusive<DocId>>,
}

/// What processing a scanner's find off the lock came to.
enum Scanned<E> {
    /// A delta to apply, and the IDs it covers.
    Delta(E, Spans),
    /// The copy would not decode: nothing to apply, and the IDs it covers stay in
    /// the set for the next cycle, as the fork GC retries a failed scan.
    Failed(Spans),
}

impl<E> Scanned<E> {
    /// Sort a scan's outcome: `Ok(None)` is a clean index, nothing to do.
    fn from_scan(scan: std::io::Result<Option<E>>, spans: Spans) -> Option<Self> {
        match scan {
            Ok(Some(entry)) => Some(Self::Delta(entry, spans)),
            Ok(None) => None,
            Err(_) => Some(Self::Failed(spans)),
        }
    }
}

impl Spans {
    fn of(ii: &InvertedIndex) -> Self {
        Self {
            all: ii.doc_id_range(),
            tail: ii.last_block_doc_id_range(),
        }
    }

    /// Keep the IDs an apply left uncollected out of this cycle's pruning.
    fn retain_after(
        &self,
        result: &Result<GcApplyStats, HandleError>,
        retained: &mut RoaringTreemap,
    ) {
        let range = match result {
            // The target vanished or was replaced: whatever it held is unaccounted for.
            Err(HandleError::ApplyError(_)) => &self.all,
            Ok(stats) if stats.numeric_nodes_missed > 0 => &self.all,
            Ok(stats) if stats.blocks_denied > 0 => &self.tail,
            _ => &None,
        };
        if let Some(range) = range {
            retained.insert_range(range.clone());
        }
    }
}

/// The lock-taking half of a cycle, shared by every scanner.
struct Driver<'a, S, G> {
    spec: &'a mut S,
    sink: &'a mut G,
    budget: ScanBudget,
    report: LockReport,
    deleted: DeletedIds<'a>,
    /// Deleted IDs some delta left in place, to keep in the doc table's set.
    retained: RoaringTreemap,
}

/// Tells a chunk scanner, after each key, whether to stop and report whether keys
/// remain; see [`keyed`] for the shape of a chunk scanner.
type StopCheck<'s> = &'s mut dyn FnMut() -> bool;

/// Turn a key list into a chunk scanner: a closure that scans keys under a read
/// lock, appending what it finds to the `Vec` until the stop check says so after
/// a key, keeps its position between calls, and returns whether keys remain.
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

    /// Apply under one write lock, flushing what `apply` tallied to the spec
    /// in the same hold and to the GC right after, when there is anything to flush.
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

    /// Run `scan_chunk` under budgeted read locks until it reports nothing
    /// left, turning each chunk's findings into entries with `process` under no
    /// lock, and applying them under one write lock.
    ///
    /// An apply that fails because its target changed since the scan only skips
    /// that entry, retaining the IDs its [`Spans`] cover, as does a copy that would
    /// not decode ([`Scanned::Failed`]); [`HandleError::SpecDeleted`] ends the scanner.
    ///
    /// Returns whether any entry was found.
    fn run<F, E>(
        &mut self,
        scanner: &'static str,
        mut scan_chunk: impl FnMut(&IndexSpecReadGuard<'_>, StopCheck<'_>, &mut Vec<F>) -> bool,
        mut process: impl FnMut(F) -> Option<Scanned<E>>,
        mut apply: impl FnMut(E, &mut IndexSpecWriteGuard<'_>) -> Result<GcApplyStats, HandleError>,
    ) -> Result<bool, HandleError> {
        let budget = self.budget.0;
        let mut had_work = false;

        loop {
            let (found, more) = self.read(|guard| {
                let start = Instant::now();
                let mut should_stop = || start.elapsed() >= budget;
                let mut found = Vec::new();
                let more = scan_chunk(guard, &mut should_stop, &mut found);
                (found, more)
            })?;

            let mut entries: Vec<(E, Spans)> = Vec::new();
            for scanned in found.into_iter().filter_map(&mut process) {
                match scanned {
                    Scanned::Delta(entry, spans) => entries.push((entry, spans)),
                    Scanned::Failed(spans) => {
                        tracing::debug!(scanner, "thread GC could not decode a copy");
                        if let Some(range) = &spans.all {
                            self.retained.insert_range(range.clone());
                        }
                    }
                }
            }
            if !entries.is_empty() {
                had_work = true;
                let mut retained = std::mem::take(&mut self.retained);
                let result = self.write(|guard, stats| {
                    for (entry, spans) in entries {
                        let result = apply(entry, guard);
                        spans.retain_after(&result, &mut retained);
                        match result {
                            Ok(entry_stats) => stats.record(entry_stats),
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
                let spans = Spans::of(&snapshot);
                let unique_id = u32::from(snapshot.unique_id());
                let scan = snapshot
                    .scan_gc(live)
                    .map(|delta| delta.map(|delta| (term, unique_id, delta)));
                Scanned::from_scan(scan, spans)
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
                    let spans = Spans {
                        all: snapshot.doc_id_range(),
                        tail: snapshot.last_block_doc_id_range(),
                    };
                    let scan = snapshot
                        .scan(live)
                        .map(|scanned| scanned.map(|ScannedNumericNode { node, .. }| node));
                    Scanned::from_scan(scan, spans)
                },
                |node, guard| {
                    let tree = resolved.get_mut(guard)?;
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
                let spans = Spans::of(&snapshot.snapshot);
                Scanned::from_scan(snapshot.scan(live), spans)
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
                let spans = Spans::of(&snapshot);
                let scan = snapshot
                    .scan_gc(live)
                    .map(|delta| delta.map(|delta| (field_name, delta)));
                Scanned::from_scan(scan, spans)
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
                let spans = Spans::of(&snapshot);
                Scanned::from_scan(snapshot.scan_gc(live), spans)
            },
            |delta, guard| apply_existing_docs_with(delta, Some(&live), guard),
        )?;
        Ok(())
    }
}

/// Run every scanner over the spec once, in the fork GC's scanner order, then
/// prune the doc table's deleted set by what was collected.
///
/// Stats are flushed per chunk, so a cycle that ends early keeps what it
/// already collected, and leaves the deleted set as it was. Returns how the
/// cycle used the spec lock, or the scanner that failed with its error;
/// [`HandleError::SpecDeleted`] is the only error a healthy cycle can hit.
pub fn run_cycle(
    spec: &mut impl SpecLockAccess,
    sink: &mut impl GcStatsSink,
    budget: ScanBudget,
    clean_numeric_empty_nodes: bool,
) -> Result<LockReport, (&'static str, HandleError)> {
    let mut report = LockReport::default();
    // `None`: the table has no set yet; `Some(None)`: it has one that is not complete.
    let (snapshot, held) = spec
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

    let dead = match snapshot {
        Some(Some(dead)) => {
            // Nothing to collect, and nothing to tell the doc table it did not already know.
            if dead.is_empty() {
                return Ok(report);
            }
            dead
        }
        Some(None) => {
            DeletedIds::sweep_table(spec, budget, &mut report).map_err(|e| ("doc table", e))?
        }
        None => {
            // Created before the sweep, so a deletion during this cycle is recorded and the
            // set is complete once the sweep's findings are collected.
            let held = spec
                .with_write(|guard| {
                    let start = Instant::now();
                    guard.ensure_deleted_docs();
                    Ok(start.elapsed())
                })
                .map_err(|e| ("deleted docs", e))?;
            report.write_locks += 1;
            report.max_write_hold = held;
            DeletedIds::sweep_table(spec, budget, &mut report).map_err(|e| ("doc table", e))?
        }
    };

    let mut driver = Driver {
        spec,
        sink,
        budget,
        report,
        deleted: DeletedIds(&dead),
        retained: RoaringTreemap::new(),
    };

    if !dead.is_empty() {
        driver.terms().map_err(|e| ("terms", e))?;
        driver
            .numeric(clean_numeric_empty_nodes)
            .map_err(|e| ("numeric", e))?;
        driver.tags().map_err(|e| ("tags", e))?;
        driver.missing_docs().map_err(|e| ("missing docs", e))?;
        driver.existing_docs().map_err(|e| ("existing docs", e))?;
    }

    let Driver {
        spec,
        mut report,
        retained,
        ..
    } = driver;
    let collected = &dead - retained;
    spec.with_write(|guard| {
        // A table rebuilt mid-cycle has no set; it gets one, and a sweep, next cycle.
        if let Some(set) = guard.deleted_docs_mut() {
            set.finish_cycle(&dead, &collected);
        }
        Ok(())
    })
    .map_err(|e| ("deleted docs", e))?;
    report.write_locks += 1;
    Ok(report)
}
