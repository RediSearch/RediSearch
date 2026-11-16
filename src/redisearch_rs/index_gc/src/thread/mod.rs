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
//! 2. Release it and scan the copies. Once [`DELTAS_PER_APPLY`] deltas have
//!    accumulated, or the scanner has no keys left, take the write lock once
//!    and apply them all.
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
//! set. An ID stays in three cases, so the next cycle looks for its entries
//! again: a last block that grew after its copy was taken and could not be
//! rescanned in place, a copy that would not decode, and a numeric leaf that
//! split since the tree's nodes were listed, copying entries into leaves the
//! cycle never scanned. Any other
//! apply failure means the scanned index is gone and its entries with it: an
//! index is only removed by this GC, and a replacement cannot hold an ID that
//! was deleted before the cycle began.

use std::{ops::RangeInclusive, time::Instant};

use c_trie::TrieTerm;
use deleted_docs::{DeletedDocs, RoaringTreemap};
use index_spec::{IndexSpecReadGuard, IndexSpecWeakRef, IndexSpecWriteGuard};
use inverted_index::{DocId, opaque::InvertedIndex};
use numeric_range_tree::NodeIndex;

use crate::liveness::GoneIds;
use crate::{
    GcApplyStats, GcError,
    scanners::existing_docs::{
        ExistingDocsEntry, apply_existing_docs_with, snapshot_existing_docs,
    },
    scanners::missing_docs::{
        apply_missing_docs_with, missing_field_indexes, snapshot_missing_field,
    },
    scanners::numeric::{
        NumericNodeSnapshot, ScannedNumericNode, apply_numeric_node_with, compact_tree,
        numeric_tree_keys, ranges_of_nodes_added_since, snapshot_numeric_node,
    },
    scanners::tags::{TagSnapshot, TagsCursor, apply_tag_entry_with, scan_tags_after},
    scanners::terms::{TermsCursor, apply_terms_with, scan_terms_after},
    spec_access::SpecWriteAccess,
    stats::GcStatsSink,
};
use rqe_core::FieldIndex;

/// Keys one read-lock hold visits before the driver releases the lock.
///
/// A read hold keeps writers (document indexing) out for its whole duration, so
/// this bounds the longest write stall the GC causes. Visiting a clean key costs
/// one range check of the deleted set; copying a key with garbage costs tens of
/// microseconds, so this many keys with garbage keep a hold near a millisecond
/// (measured: 1.6 ms at 64 on a 1M-document index whose first chunk was all
/// dirty, 6 ms at 256). Readers are never blocked by a read hold, so the number
/// of holds per cycle is free as far as queries are concerned; only their length
/// matters, which is why this is small.
pub const KEYS_PER_HOLD: usize = 64;

/// Deltas a scanner accumulates, across read holds, before it takes the write
/// lock once to apply them all.
///
/// The spec lock prefers writers: a pending write hold waits for the in-flight
/// queries and queues every new one behind it. Each write hold is therefore a
/// barrier whose cost to queries is one query duration, however short the hold
/// itself is, so what a cycle costs queries is the number of write holds, not
/// their length. Applying a delta is a few block swaps, tens of microseconds,
/// so this many deltas keep a hold around 50 ms while cutting the holds per
/// cycle to a handful: on a 1M-document index with 250 ms queries, applying per
/// read hold took about 100 write holds and cost queries 12% at p50; applying
/// per [`DELTAS_PER_APPLY`] took about 6 and the cost disappeared into noise.
///
/// Hence the ratio to [`KEYS_PER_HOLD`]: the read side is bounded by hold
/// length because it stalls writers, the write side by hold count because it
/// fences readers. What the ratio buys is paid in memory, since the repaired
/// block copies of up to this many keys wait for the apply instead of being
/// freed after each chunk.
pub const DELTAS_PER_APPLY: usize = 1024;

/// Provides closure-scoped access to a live, read-locked
/// [`IndexSpec`](index_spec::IndexSpec), next to the write access of
/// [`SpecWriteAccess`].
pub trait SpecLockAccess: SpecWriteAccess {
    /// Promote and read-lock the spec, then run `scan` under the lock.
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, GcError>;
}

impl SpecLockAccess for IndexSpecWeakRef {
    fn with_read<T>(
        &mut self,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, GcError> {
        let spec_ref = self.promote().ok_or(GcError::SpecDeleted)?;
        let lock = spec_ref.read();
        Ok(scan(&lock.guard()))
    }
}

/// What one cycle did: how it used the spec lock, and what it left for a later one.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CycleReport {
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
    /// Deleted IDs the cycle left in the set because it could not collect all their
    /// entries.
    pub retained: u64,
    /// Deleted IDs the cycle collected and pruned from the set.
    pub pruned: u64,
}

impl CycleReport {
    /// Run `scan` under one read hold of `spec`, and count the hold.
    fn read<T>(
        &mut self,
        spec: &mut impl SpecLockAccess,
        scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T,
    ) -> Result<T, GcError> {
        let (value, held) = spec.with_read(|guard| {
            let start = Instant::now();
            let value = scan(guard);
            (value, start.elapsed())
        })?;
        self.read_locks += 1;
        self.max_read_hold = self.max_read_hold.max(held);
        Ok(value)
    }

    /// Run `apply` under one write hold of `spec`, and count the hold, whatever
    /// `apply` returns.
    fn write<T>(
        &mut self,
        spec: &mut impl SpecWriteAccess,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>) -> T,
    ) -> Result<T, GcError> {
        let (value, held) = spec.with_write(|guard| {
            let start = Instant::now();
            let value = apply(guard);
            Ok((value, start.elapsed()))
        })?;
        self.write_locks += 1;
        self.max_write_hold = self.max_write_hold.max(held);
        Ok(value)
    }
}

/// The doc IDs a scanned index's last block spanned when it was copied: what a
/// denied delta leaves uncollected.
type Tail = Option<RangeInclusive<DocId>>;

/// The doc IDs a whole scanned index spanned when it was copied: what a copy that
/// would not decode leaves uncollected.
type Span = Option<RangeInclusive<DocId>>;

struct Driver<'a, 'd, S, G> {
    spec: &'a mut S,
    sink: &'a mut G,
    report: CycleReport,
    /// The documents deleted before the cycle began, copied from the doc table's set,
    /// so a check needs no doc table and no lock. This rests on two facts about doc
    /// IDs: one is never reused, and a document's entries are all written before its
    /// ID can be deleted (both under the spec write lock), so a listed ID neither
    /// comes back nor gains entries the cycle could miss.
    deleted: &'d GoneIds,
    /// Deleted IDs this cycle could not collect every entry of; they stay in the set.
    /// Only IDs of [`Self::deleted`] go in, so its size is bounded by that set's
    /// rather than by the width of the doc ID ranges retained.
    retained: RoaringTreemap,
}

type StopCheck<'s> = &'s mut dyn FnMut() -> bool;

/// [`Driver::run`]'s working vectors, kept across its read and write holds, and
/// across the numeric trees, so a cycle allocates them once per scanner rather
/// than once per hold.
struct Buffers<F, E> {
    /// The snapshots one read hold took.
    snapshots: Vec<F>,
    /// Deltas waiting for the next write hold.
    pending_deltas: Vec<(E, Tail)>,
}

impl<F, E> Buffers<F, E> {
    fn new() -> Self {
        Self {
            // A hold copies at most one index per key it visits.
            snapshots: Vec::with_capacity(KEYS_PER_HOLD),
            pending_deltas: Vec::new(),
        }
    }
}

/// A [`Driver::run`] `snapshot_chunk` over a key list made up front: resumes
/// where the previous chunk stopped. Returns whether keys remain.
fn keyed<'k, K, F>(
    keys: &'k [K],
    snapshot: impl Fn(&IndexSpecReadGuard<'_>, &'k K) -> Option<F> + 'k,
) -> impl FnMut(&IndexSpecReadGuard<'_>, StopCheck<'_>, &mut Vec<F>) -> bool + 'k {
    let mut next = 0;
    move |guard, should_stop, snapshots| {
        while let Some(key) = keys.get(next) {
            next += 1;
            if let Some(taken) = snapshot(guard, key) {
                snapshots.push(taken);
            }
            if should_stop() {
                return next < keys.len();
            }
        }
        false
    }
}

impl<S: SpecLockAccess, G: GcStatsSink> Driver<'_, '_, S, G> {
    fn read<T>(&mut self, scan: impl FnOnce(&IndexSpecReadGuard<'_>) -> T) -> Result<T, GcError> {
        self.report.read(self.spec, scan)
    }

    fn write(
        &mut self,
        apply: impl FnOnce(&mut IndexSpecWriteGuard<'_>, &mut GcApplyStats) -> Result<(), GcError>,
    ) -> Result<(), GcError> {
        let sink = &mut *self.sink;
        self.report.write(self.spec, |guard| {
            let mut stats = GcApplyStats::default();
            let result = apply(guard, &mut stats);
            stats.apply_to_spec(guard);
            // Under the lock, as `FT.INFO` reads the counters under it.
            sink.record(&stats);
            result
        })?
    }

    /// Run one scanner to completion: `snapshot_chunk` copies the indexes of the
    /// next keys under a read lock and says whether any keys remain;
    /// `scan_snapshot` scans a copy without the lock, into a delta and the ID span
    /// of its last block, or `Ok(None)` when the copy is clean; `apply_delta`
    /// lands a delta under the write lock. Returns whether any delta was
    /// applied.
    ///
    /// `buffers` is left empty, its capacity kept for the next call.
    fn run<F, E>(
        &mut self,
        scanner: &'static str,
        buffers: &mut Buffers<F, E>,
        mut snapshot_chunk: impl FnMut(&IndexSpecReadGuard<'_>, StopCheck<'_>, &mut Vec<F>) -> bool,
        mut scan_snapshot: impl FnMut(F) -> Result<Option<(E, Tail)>, (std::io::Error, Span)>,
        mut apply_delta: impl FnMut(E, &mut IndexSpecWriteGuard<'_>) -> Result<GcApplyStats, GcError>,
    ) -> Result<bool, GcError> {
        let Buffers {
            snapshots,
            pending_deltas,
        } = buffers;
        let mut had_work = false;

        loop {
            let more = self.read(|guard| {
                let mut visited = 0;
                let mut should_stop = || {
                    visited += 1;
                    visited >= KEYS_PER_HOLD
                };
                snapshot_chunk(guard, &mut should_stop, snapshots)
            })?;

            for snapshot in snapshots.drain(..) {
                match scan_snapshot(snapshot) {
                    Ok(Some(delta)) => pending_deltas.push(delta),
                    Ok(None) => {}
                    // The live index would not decode either; a cycle cannot repair that,
                    // but the IDs it holds stay in the set for a later one.
                    Err((error, span)) => {
                        tracing::warn!(scanner, %error, "thread GC: an index copy would not decode");
                        if let Some(span) = span {
                            self.deleted.insert_gone_in(span, &mut self.retained);
                        }
                    }
                }
            }
            if !pending_deltas.is_empty() && (!more || pending_deltas.len() >= DELTAS_PER_APPLY) {
                had_work = true;
                let mut retained = std::mem::take(&mut self.retained);
                let deleted = self.deleted;
                let result = self.write(|guard, stats| {
                    for (delta, tail) in pending_deltas.drain(..) {
                        match apply_delta(delta, guard) {
                            Ok(delta_stats) => {
                                if delta_stats.blocks_denied > 0
                                    && let Some(tail) = tail
                                {
                                    deleted.insert_gone_in(tail, &mut retained);
                                }
                                stats.record(delta_stats);
                            }
                            Err(GcError::TargetGone(reason)) => {
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
                // An error above can leave copies behind, but the cycle ends with it.
                debug_assert!(pending_deltas.is_empty());
                return Ok(had_work);
            }
        }
    }

    fn terms(&mut self) -> Result<(), GcError> {
        let mut cursor = TermsCursor::default();
        let live = self.deleted;
        let snapshot_chunk =
            |guard: &IndexSpecReadGuard<'_>, should_stop: StopCheck<'_>, snapshots: &mut Vec<_>| {
                scan_terms_after(guard, &mut cursor, live, should_stop, snapshots)
            };
        let scan_snapshot = |(term, snapshot): (TrieTerm, InvertedIndex)| {
            let tail = snapshot.last_block_doc_id_range();
            let unique_id = u32::from(snapshot.unique_id());
            Ok(snapshot
                .scan_gc(live)
                .map_err(|e| (e, snapshot.doc_id_range()))?
                .map(|delta| ((term, unique_id, delta), tail)))
        };
        let apply_delta = |(term, unique_id, delta), guard: &mut IndexSpecWriteGuard<'_>| {
            apply_terms_with(&term, unique_id, delta, Some(&live), guard)
        };
        self.run(
            "terms",
            &mut Buffers::new(),
            snapshot_chunk,
            scan_snapshot,
            apply_delta,
        )?;
        Ok(())
    }

    fn numeric(&mut self, clean_numeric_empty_nodes: bool) -> Result<(), GcError> {
        // A tree has far fewer nodes than entries, so listing them in one hold is cheap.
        let trees = self.read(numeric_tree_keys)?;
        let live = self.deleted;
        let mut buffers = Buffers::new();
        for tree in &trees {
            let id = tree.id;
            let snapshot_chunk = keyed(&tree.nodes, |guard, &node_idx: &NodeIndex| {
                snapshot_numeric_node(guard, &id, node_idx, &live)
            });
            let scan_snapshot = |snapshot: NumericNodeSnapshot| {
                let tail = snapshot.last_block_doc_id_range();
                let span = snapshot.doc_id_range();
                Ok(snapshot
                    .scan(live)
                    .map_err(|e| (e, span))?
                    .map(|ScannedNumericNode { node, .. }| (node, tail)))
            };
            let apply_delta = |node, guard: &mut IndexSpecWriteGuard<'_>| {
                let tree = id.tree_mut(guard)?;
                // A node gone by now was trimmed empty or split; a split's new
                // leaves are retained below.
                Ok(
                    apply_numeric_node_with(tree, node, Some(&live)).unwrap_or(GcApplyStats {
                        numeric_nodes_missed: 1,
                        ..GcApplyStats::default()
                    }),
                )
            };
            let had_work = self.run(
                "numeric",
                &mut buffers,
                snapshot_chunk,
                scan_snapshot,
                apply_delta,
            )?;

            // A leaf split since the nodes were listed copied its entries, deleted ones
            // included, into leaves this cycle never scanned; their IDs stay in the set.
            for range in self.read(|guard| ranges_of_nodes_added_since(guard, tree))? {
                self.deleted.insert_gone_in(range, &mut self.retained);
            }

            if clean_numeric_empty_nodes && had_work {
                self.write(|guard, stats| {
                    match compact_tree(guard, id) {
                        Ok(compacted) => stats.record(compacted),
                        Err(GcError::TargetGone(reason)) => {
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

    fn tags(&mut self) -> Result<(), GcError> {
        let mut cursor = TagsCursor::default();
        let live = self.deleted;
        let snapshot_chunk =
            |guard: &IndexSpecReadGuard<'_>, should_stop: StopCheck<'_>, snapshots: &mut Vec<_>| {
                scan_tags_after(guard, &mut cursor, live, should_stop, snapshots)
            };
        let scan_snapshot = |snapshot: TagSnapshot| {
            let tail = snapshot.snapshot.last_block_doc_id_range();
            let span = snapshot.snapshot.doc_id_range();
            Ok(snapshot
                .scan(live)
                .map_err(|e| (e, span))?
                .map(|delta| (delta, tail)))
        };
        let apply_delta = |delta, guard: &mut IndexSpecWriteGuard<'_>| {
            apply_tag_entry_with(delta, Some(&live), guard)
        };
        self.run(
            "tags",
            &mut Buffers::new(),
            snapshot_chunk,
            scan_snapshot,
            apply_delta,
        )?;
        Ok(())
    }

    fn missing_docs(&mut self) -> Result<(), GcError> {
        // One entry per INDEXMISSING field, so the list is short.
        let keys: Vec<FieldIndex> = self.read(missing_field_indexes)?;
        if keys.is_empty() {
            return Ok(());
        }
        let live = self.deleted;
        let snapshot_chunk = keyed(&keys, |guard, &field_index: &FieldIndex| {
            snapshot_missing_field(guard, field_index, &live)
                .map(|snapshot| (field_index, snapshot))
        });
        let scan_snapshot = |(field_index, snapshot): (FieldIndex, InvertedIndex)| {
            let tail = snapshot.last_block_doc_id_range();
            let unique_id = u32::from(snapshot.unique_id());
            Ok(snapshot
                .scan_gc(live)
                .map_err(|e| (e, snapshot.doc_id_range()))?
                .map(|delta| ((field_index, unique_id, delta), tail)))
        };
        let apply_delta = |(field_index, unique_id, delta), guard: &mut IndexSpecWriteGuard<'_>| {
            apply_missing_docs_with(field_index, unique_id, delta, Some(&live), guard)
        };
        self.run(
            "missing docs",
            &mut Buffers::new(),
            snapshot_chunk,
            scan_snapshot,
            apply_delta,
        )?;
        Ok(())
    }

    fn existing_docs(&mut self) -> Result<(), GcError> {
        let live = self.deleted;
        let snapshot_chunk = keyed(&[()], move |guard, ()| snapshot_existing_docs(guard, &live));
        let scan_snapshot = |snapshot: InvertedIndex| {
            let tail = snapshot.last_block_doc_id_range();
            let unique_id = u32::from(snapshot.unique_id());
            Ok(snapshot
                .scan_gc(live)
                .map_err(|e| (e, snapshot.doc_id_range()))?
                .map(|delta| (ExistingDocsEntry { unique_id, delta }, tail)))
        };
        let apply_delta = |delta, guard: &mut IndexSpecWriteGuard<'_>| {
            apply_existing_docs_with(delta, Some(&live), guard)
        };
        self.run(
            "existing docs",
            &mut Buffers::new(),
            snapshot_chunk,
            scan_snapshot,
            apply_delta,
        )?;
        Ok(())
    }
}

/// Run one GC cycle over `spec`: collect the entries of every document in the
/// doc table's [`DeletedDocs`] set, then drop the collected IDs from it. See the
/// [module docs](self) for the lock discipline. On error, names the scanner
/// that failed; a spec dropped mid-cycle ends the cycle with
/// [`GcError::SpecDeleted`], keeping what was already applied.
pub fn run_cycle(
    spec: &mut impl SpecLockAccess,
    sink: &mut impl GcStatsSink,
    clean_numeric_empty_nodes: bool,
) -> Result<CycleReport, (&'static str, GcError)> {
    let mut report = CycleReport::default();
    let dead = report
        .read(spec, |guard| {
            guard.deleted_docs().map(DeletedDocs::snapshot)
        })
        .map_err(|e| ("deleted docs", e))?;

    let dead = match dead {
        // A table that does not track deletions gives this GC nothing to find garbage with;
        // reporting success would leave its stale postings in place silently.
        None => {
            return Err(("deleted docs", GcError::DeletionsUntracked));
        }
        // Nothing to collect, and nothing to tell the doc table.
        Some(dead) if dead.is_empty() => return Ok(report),
        Some(dead) => dead,
    };
    let gone = GoneIds::from_set(dead);

    let mut driver = Driver {
        spec,
        sink,
        report,
        deleted: &gone,
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
    let mut collected = gone.into_set();
    let listed = collected.len();
    collected -= retained;
    report.retained = listed - collected.len();
    report
        .write(spec, |guard| {
            if let Some(set) = guard.deleted_docs_mut() {
                set.prune(&collected);
            }
        })
        .map_err(|e| ("deleted docs", e))?;
    report.pruned = collected.len();
    Ok(report)
}
