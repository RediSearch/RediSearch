/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection for the terminals of every NESTED field in a spec.
//!
//! A terminal is a numeric range tree or a tag index, collected as a NUMERIC or TAG field's
//! index is. Once a field's terminals are collected, the ones left without postings are
//! dropped with the routing nodes above them, so a deleted element's path stops costing
//! memory and walks. Dropping a terminal revokes its
//! [`Terminal::alive`](nested_index::Terminal::alive) token, which the query iterators reading
//! it check before reading it again; that is what makes freeing it safe.

use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::ops::RangeInclusive;
use std::ptr::NonNull;
use std::sync::Arc;

use field_spec::{FieldSpec, FieldSpecType};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocId, DocLiveness};
use nested_index::{NestedIndex, PostingsMut, PostingsRef, Terminal};
use numeric_range_tree::{NodeIndex, NumericIndex};
use rqe_core::FieldIndex;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tag_index::{TagValueGcDelta, TagValueSnapshot};

use crate::fork::wire::{deserialize, serialize};
use crate::scanners::numeric::{NumericNodeDelta, apply_numeric_node_with, compact_if_sparse};
use crate::spec_access::SpecWriteAccess;
use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// The pointer `fs` owns its index through, or `None` when the field is not NESTED or its
/// index could not be built.
fn nested_index_ptr(fs: &FieldSpec) -> Option<NonNull<NestedIndex>> {
    if !fs.types().contains(FieldSpecType::Nested) {
        return None;
    }
    let raw_fs = std::ptr::from_ref(fs).cast::<ffi::FieldSpec>();
    // SAFETY: `FieldSpec` is a `repr(transparent)` wrapper of `ffi::FieldSpec`, and `fs` is a
    // live reference, so the cast pointer is valid.
    let raw_fs = unsafe { raw_fs.as_ref() }.expect("a reference is never null");
    // SAFETY: the field is NESTED, so `nestedOpts` is the active union member.
    let index = unsafe { raw_fs.__bindgen_anon_1.nestedOpts.nestedIndex };
    NonNull::new(index.cast::<NestedIndex>())
}

/// The index of `fs`; see [`nested_index_ptr`].
fn fetch_nested_index(fs: &FieldSpec) -> Option<&NestedIndex> {
    // SAFETY: the pointer is the `NestedIndex` the FFI allocated for this field, owned by it and
    // kept alive by the caller's spec lock, under which nothing writes to it.
    nested_index_ptr(fs).map(|index| unsafe { index.as_ref() })
}

/// [`fetch_nested_index`], exclusively.
fn fetch_nested_index_mut(fs: &mut FieldSpec) -> Option<&mut NestedIndex> {
    // SAFETY: as in `fetch_nested_index`; the caller's write lock, which `&mut fs` stands for,
    // makes the borrow unique.
    nested_index_ptr(fs).map(|mut index| unsafe { index.as_mut() })
}

fn field_index_of(position: usize) -> FieldIndex {
    FieldIndex::try_from(position).expect("a spec has fewer fields than a field index can count")
}

/// A NESTED field scanned by the child process, ahead of its terminals' deltas.
#[derive(Debug, Serialize, Deserialize)]
pub struct NestedField {
    /// Position of the field in the spec's fields, which only grow.
    pub field_index: FieldIndex,
}

/// A terminal's path as it travels the pipe: its tag value on each routing level, as bytes.
fn to_wire(path: &[Vec<u8>]) -> Vec<ByteBuf> {
    path.iter().cloned().map(ByteBuf::from).collect()
}

/// The GC work one terminal holds.
#[derive(Debug, Serialize, Deserialize)]
pub enum TerminalWork {
    /// The nodes of a numeric terminal's tree with garbage.
    Numeric {
        /// [`NumericRangeTree::unique_id`](numeric_range_tree::NumericRangeTree::unique_id) of the
        /// tree scanned.
        unique_id: u32,
        /// One delta per node with garbage.
        nodes: Vec<NumericNodeDelta>,
    },
    /// The values of a tag terminal with garbage.
    Tag(Vec<TagValueGcDelta>),
}

/// One terminal's GC work, located by its path.
#[derive(Debug, Serialize, Deserialize)]
pub struct NestedTerminalDelta {
    /// The terminal's path, its tag value on each routing level, as
    /// [`NestedIndex::collect_terminal`] takes it.
    pub path: Vec<ByteBuf>,
    /// What to collect from it.
    pub work: TerminalWork,
}

/// What [`scan_terminal`] found in one terminal.
#[derive(Debug, Default)]
pub struct TerminalScan {
    /// The terminal's GC work, `None` when no posting of it names a gone document.
    pub work: Option<TerminalWork>,
    /// The doc IDs spanned by each posting list whose scan failed. Its gone documents stay in
    /// the terminal; a collector that forgets the gone IDs it collected must keep these.
    pub failed: Vec<RangeInclusive<DocId>>,
    /// The posting lists scanned, numeric ranges or tag values, as a measure of the work done.
    pub lists_scanned: usize,
}

/// The delta of one numeric node, from its scan's outcome; a failed scan records the doc IDs
/// the scanned range spans in `failed`.
fn node_delta(
    node: NodeIndex,
    scanned: io::Result<Option<numeric_range_tree::NodeGcDelta>>,
    range: &NumericIndex,
    failed: &mut Vec<RangeInclusive<DocId>>,
) -> Option<NumericNodeDelta> {
    match scanned {
        Ok(delta) => delta.map(|delta| NumericNodeDelta {
            position: node.position(),
            generation: node.generation(),
            delta,
        }),
        Err(_) => {
            failed.extend(range.doc_id_range());
            None
        }
    }
}

/// The GC work of `terminal`, against the documents `live` says are gone, scanned in place: the
/// fork child's view, which nothing else writes to.
pub fn scan_terminal(terminal: &Terminal, live: impl DocLiveness + Copy) -> TerminalScan {
    let mut scan = TerminalScan::default();
    match terminal.postings() {
        PostingsRef::Numeric(tree) => {
            let mut nodes = Vec::new();
            for (node_idx, node) in tree.indexed_post_order_iter() {
                let Some(range) = node.range() else {
                    continue;
                };
                scan.lists_scanned += 1;
                let scanned =
                    numeric_range_tree::scan_range_index_gc(range.entries(), live, node.is_leaf());
                nodes.extend(node_delta(
                    node_idx,
                    scanned,
                    range.entries(),
                    &mut scan.failed,
                ));
            }
            scan.work = (!nodes.is_empty()).then(|| TerminalWork::Numeric {
                unique_id: u32::from(tree.unique_id()),
                nodes,
            });
        }
        PostingsRef::Tag(tags) => {
            let tags = tags.scan_gc(live);
            scan.failed = tags.failed;
            scan.lists_scanned = tags.values_visited;
            scan.work = (!tags.deltas.is_empty()).then_some(TerminalWork::Tag(tags.deltas));
        }
    }
    scan
}

/// The NESTED fields of the spec that have an index.
pub fn nested_field_indexes(spec: &IndexSpecReadGuard) -> Vec<FieldIndex> {
    spec.field_specs()
        .iter()
        .enumerate()
        .filter(|(_, fs)| fetch_nested_index(fs).is_some())
        .map(|(position, _)| field_index_of(position))
        .collect()
}

/// The path of the first terminal of the NESTED field at `field_index` after `after` (the first
/// terminal without it), in the order of [`CompoundIndex::for_each_terminal`]; `None` when there
/// is none, or the field is gone. Seeks rather than walks, so a walk over many terminals resumes
/// across lock releases at a cost independent of how far it got.
///
/// [`CompoundIndex::for_each_terminal`]: nested_index::CompoundIndex::for_each_terminal
pub fn next_nested_terminal(
    spec: &IndexSpecReadGuard,
    field_index: FieldIndex,
    after: Option<&[Vec<u8>]>,
) -> Option<Vec<Vec<u8>>> {
    let index = fetch_nested_index(spec.field_specs().get(usize::from(field_index))?)?;
    let mut next = None;
    index.routing().for_each_terminal_after(after, |path, _| {
        next = Some(path.to_vec());
        std::ops::ControlFlow::Break(())
    });
    next
}

/// What a numeric terminal's tree held when a thread-GC walk of it started.
#[derive(Debug)]
pub struct TreeListing {
    unique_id: u32,
    revision_id: u32,
    /// Every node, so a reshape since can tell which nodes are new.
    nodes: Vec<NodeIndex>,
}

/// Where a thread-GC walk of one terminal's posting lists resumes, across read holds.
#[derive(Debug, Default)]
pub struct TerminalCursor {
    listing: Option<Listing>,
    next: usize,
    done: bool,
}

#[derive(Debug)]
enum Listing {
    /// What the tree held when the walk started; the walk visits these nodes.
    Numeric(Arc<TreeListing>),
    /// The last tag value a hold visited, to resume after.
    Tag(Vec<u8>),
}

impl TerminalCursor {
    /// Whether every posting list of the terminal was visited, or the terminal is gone.
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// What a numeric terminal's tree held when the walk started, for
    /// [`nested_ranges_added_since`] once the walk's deltas are applied.
    pub fn numeric_listing(&self) -> Option<Arc<TreeListing>> {
        match &self.listing {
            Some(Listing::Numeric(listing)) => Some(Arc::clone(listing)),
            _ => None,
        }
    }
}

/// Posting lists of one terminal that may hold gone documents, copied under a read hold by
/// [`snapshot_nested_chunk`] so the thread GC can scan them without the lock.
pub struct NestedTerminalSnapshot {
    path: Vec<ByteBuf>,
    copies: TerminalCopies,
}

enum TerminalCopies {
    Numeric {
        listing: Arc<TreeListing>,
        /// The dirty ranges: the node, whether it was a leaf, and the copy.
        nodes: Vec<(NodeIndex, bool, NumericIndex)>,
    },
    Tag(Vec<TagValueSnapshot>),
}

/// What [`NestedTerminalSnapshot::scan`] found.
pub struct ScannedNestedTerminal {
    /// The GC work of the copied lists, `None` when they held none.
    pub delta: Option<NestedTerminalDelta>,
    /// The doc IDs spanned by each copy whose scan failed; their gone entries stay.
    pub failed: Vec<RangeInclusive<DocId>>,
}

impl NestedTerminalSnapshot {
    /// Scan the copies, against the liveness source they were taken with.
    pub fn scan(self, live: impl DocLiveness + Copy) -> ScannedNestedTerminal {
        let mut failed = Vec::new();
        let work = match self.copies {
            TerminalCopies::Numeric { listing, nodes } => {
                let nodes: Vec<_> = nodes
                    .into_iter()
                    .filter_map(|(node, leaf, index)| {
                        let scanned = numeric_range_tree::scan_range_index_gc(&index, live, leaf);
                        node_delta(node, scanned, &index, &mut failed)
                    })
                    .collect();
                (!nodes.is_empty()).then(|| TerminalWork::Numeric {
                    unique_id: listing.unique_id,
                    nodes,
                })
            }
            TerminalCopies::Tag(values) => {
                let values: Vec<_> = values
                    .into_iter()
                    .filter_map(|value| {
                        value
                            .scan(live)
                            .map_err(|(_, span)| failed.extend(span))
                            .ok()
                            .flatten()
                    })
                    .collect();
                (!values.is_empty()).then_some(TerminalWork::Tag(values))
            }
        };
        ScannedNestedTerminal {
            delta: work.map(|work| NestedTerminalDelta {
                path: self.path,
                work,
            }),
            failed,
        }
    }
}

/// Copy, under one read hold, the next posting lists of the terminal at `path` of the NESTED
/// field at `field_index` that `live` says may hold a gone document, resuming at `cursor`.
/// `should_stop` is asked after each list visited, or once for a terminal with none, and a
/// `true` ends the hold; returns the copies taken, `None` when every list visited was clean,
/// and whether `should_stop` ended the hold.
///
/// A terminal is walked across as many holds as its lists need: a numeric terminal's walk
/// visits the nodes its tree held when the walk started, and a tag terminal's resumes after the
/// last value it visited. A numeric range is copied whole, as its scan's cardinality repair has
/// to see every surviving entry; a tag value's clean blocks are copied without their data. A
/// field or terminal gone since ends the walk.
pub fn snapshot_nested_chunk(
    spec: &IndexSpecReadGuard,
    field_index: FieldIndex,
    path: &[Vec<u8>],
    cursor: &mut TerminalCursor,
    live: &impl DocLiveness,
    should_stop: &mut dyn FnMut() -> bool,
) -> (Option<NestedTerminalSnapshot>, bool) {
    let terminal = spec
        .field_specs()
        .get(usize::from(field_index))
        .and_then(fetch_nested_index)
        .and_then(|index| index.routing().terminal_at(path));
    let Some(terminal) = terminal else {
        cursor.done = true;
        return (None, should_stop());
    };
    let (copies, visited, stopped) = match terminal.postings() {
        PostingsRef::Numeric(tree) => {
            let listing = match &cursor.listing {
                Some(Listing::Numeric(listing)) => Arc::clone(listing),
                _ => {
                    let listing = Arc::new(TreeListing {
                        unique_id: u32::from(tree.unique_id()),
                        revision_id: tree.revision_id(),
                        nodes: tree
                            .indexed_post_order_iter()
                            .map(|(node, _)| node)
                            .collect(),
                    });
                    cursor.listing = Some(Listing::Numeric(Arc::clone(&listing)));
                    listing
                }
            };
            let mut nodes = Vec::new();
            let mut visited = 0;
            let mut stopped = false;
            while let Some(&node_idx) = listing.nodes.get(cursor.next)
                && !stopped
            {
                cursor.next += 1;
                visited += 1;
                // A node gone since the walk started is accounted for once the walk ends.
                if let Some(node) = tree.try_node(node_idx)
                    && let Some(copy) = node
                        .range()
                        .and_then(|range| range.entries().gc_snapshot(live))
                {
                    nodes.push((node_idx, node.is_leaf(), copy));
                }
                stopped = should_stop();
            }
            cursor.done = cursor.next == listing.nodes.len();
            let copies = (!nodes.is_empty()).then_some(TerminalCopies::Numeric { listing, nodes });
            (copies, visited, stopped)
        }
        PostingsRef::Tag(tags) => {
            let after = match cursor.listing.take() {
                Some(Listing::Tag(after)) => Some(after),
                _ => None,
            };
            let mut values = Vec::new();
            let walk = tags.gc_snapshots_after(after.as_deref(), live, should_stop, &mut values);
            cursor.done = walk.resume_after.is_none();
            cursor.listing = walk.resume_after.map(Listing::Tag);
            let copies = (!values.is_empty()).then_some(TerminalCopies::Tag(values));
            (copies, walk.visited, walk.stopped)
        }
    };
    // A terminal with no list still costs its lookup.
    let stopped = if visited == 0 { should_stop() } else { stopped };
    let snapshot = copies.map(|copies| NestedTerminalSnapshot {
        path: to_wire(path),
        copies,
    });
    (snapshot, stopped)
}

/// The doc IDs spanned by the nodes the numeric terminal at `path` gained since `listing` was
/// taken, when its tree changed shape since: a leaf that split copied its entries, gone ones
/// included, into nodes the walk never saw, whether or not any delta of the walk was applied.
/// Empty when nothing changed, or the field or the terminal is gone, with its postings.
///
/// Run after the walk's deltas are applied and before its collection is finished, which may
/// compact the tree into nodes the listing holds.
pub fn nested_ranges_added_since(
    spec: &IndexSpecReadGuard,
    field_index: FieldIndex,
    path: &[Vec<u8>],
    listing: &TreeListing,
) -> Vec<RangeInclusive<DocId>> {
    let tree = spec
        .field_specs()
        .get(usize::from(field_index))
        .and_then(fetch_nested_index)
        .and_then(|index| index.routing().terminal_at(path))
        .and_then(Terminal::numeric);
    let Some(tree) = tree else {
        return Vec::new();
    };
    if u32::from(tree.unique_id()) != listing.unique_id || tree.revision_id() == listing.revision_id
    {
        return Vec::new();
    }
    let listed: HashSet<_> = listing.nodes.iter().copied().collect();
    tree.indexed_post_order_iter()
        .filter(|(node_idx, _)| !listed.contains(node_idx))
        .filter_map(|(_, node)| node.range()?.entries().doc_id_range())
        .collect()
}

/// The doc IDs every posting list of `terminal` spans.
fn terminal_ranges(terminal: &Terminal) -> Vec<RangeInclusive<DocId>> {
    match terminal.postings() {
        PostingsRef::Numeric(tree) => tree
            .indexed_post_order_iter()
            .filter_map(|(_, node)| node.range()?.entries().doc_id_range())
            .collect(),
        PostingsRef::Tag(tags) => tags.doc_id_ranges(),
    }
}

/// The NESTED field at `field_index`, under the write lock.
fn nested_index_mut<'g>(
    guard: &'g mut IndexSpecWriteGuard<'_>,
    field_index: FieldIndex,
) -> Result<&'g mut NestedIndex, GcError> {
    let fs = guard
        .field_specs_mut()
        .get_mut(usize::from(field_index))
        .ok_or(GcError::TargetGone(
            "the spec has no field at the scanned position",
        ))?;
    fetch_nested_index_mut(fs).ok_or(GcError::TargetGone("the field has no NESTED index"))
}

/// What [`apply_nested_terminal_with`] did to one terminal.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TerminalApplied {
    /// What the posting lists' own GC did.
    pub stats: GcApplyStats,
    /// With a liveness source, the doc IDs whose gone entries may still be in the terminal:
    /// every list's after a denied last block, which the source should make impossible.
    pub uncollected: Vec<RangeInclusive<DocId>>,
}

/// Apply one terminal's GC work to the NESTED field at `field_index`, through
/// [`NestedIndex::collect_terminal`], which keeps the terminal for
/// [`finish_nested_field`].
///
/// With `live`, a last block that grew since the scan is rescanned instead of denied; it must
/// be the source the scan used. A numeric node gone since the scan is counted in
/// [`GcApplyStats::numeric_nodes_missed`]; a reshape is found by [`nested_ranges_added_since`].
pub fn apply_nested_terminal_with(
    guard: &mut IndexSpecWriteGuard<'_>,
    field_index: FieldIndex,
    delta: NestedTerminalDelta,
    live: Option<&dyn DocLiveness>,
) -> Result<TerminalApplied, GcError> {
    let index = nested_index_mut(guard, field_index)?;
    let path: Vec<Vec<u8>> = delta.path.into_iter().map(ByteBuf::into_vec).collect();
    let track = live.is_some();
    let applied = index.collect_terminal(&path, |terminal| {
        let applied = apply_work(terminal, delta.work, live);
        let removed = applied
            .as_ref()
            .map_or(0, |applied| applied.stats.records_removed);
        (applied, removed)
    });
    let mut applied = applied.ok_or(GcError::TargetGone(
        "the terminal was dropped since the scan",
    ))??;
    if track && applied.stats.blocks_denied > 0 {
        let terminal = index
            .routing()
            .terminal_at(&path)
            .expect("the terminal was just collected");
        applied.uncollected = terminal_ranges(terminal);
    }
    Ok(applied)
}

fn apply_work(
    terminal: &mut Terminal,
    work: TerminalWork,
    live: Option<&dyn DocLiveness>,
) -> Result<TerminalApplied, GcError> {
    let mut applied = TerminalApplied::default();
    match (terminal.postings_mut(), work) {
        (PostingsMut::Numeric(tree), TerminalWork::Numeric { unique_id, nodes }) => {
            if u32::from(tree.unique_id()) != unique_id {
                return Err(GcError::TargetGone(
                    "the terminal's tree is not the one the scan saw",
                ));
            }
            for node in nodes {
                match apply_numeric_node_with(tree, node, live) {
                    Some(node_stats) => applied.stats.record(node_stats),
                    None => applied.stats.numeric_nodes_missed += 1,
                }
            }
        }
        (PostingsMut::Tag(tags), TerminalWork::Tag(values)) => {
            for value in values {
                // A value gone or replaced since the scan holds none of the scanned postings.
                let Some(outcome) =
                    tags.apply_gc(&value.tag, value.list_unique_id, value.delta, live)
                else {
                    continue;
                };
                let (extra_bytes, remaining_blocks) = outcome
                    .removed
                    .map_or((0, 0), |removed| (removed.bytes, removed.blocks));
                applied.stats.record(GcApplyStats {
                    records_removed: outcome.info.entries_removed,
                    bytes_collected: outcome.info.bytes_freed + extra_bytes,
                    bytes_allocated: outcome.info.bytes_allocated,
                    block_count_delta: outcome.info.block_count_delta - remaining_blocks as i64,
                    blocks_denied: outcome.info.ignored_last_block as u64,
                    ..GcApplyStats::default()
                });
            }
        }
        _ => {
            return Err(GcError::TargetGone(
                "the terminal is not of the kind the scan saw",
            ));
        }
    }
    Ok(applied)
}

/// Whether the NESTED field at `field_index` has terminals a collection reached and did not
/// finish, so [`finish_nested_field`] has work even in a cycle that applied none.
pub fn nested_field_has_collected(spec: &IndexSpecReadGuard, field_index: FieldIndex) -> bool {
    spec.field_specs()
        .get(usize::from(field_index))
        .and_then(fetch_nested_index)
        .is_some_and(NestedIndex::has_collected)
}

/// Terminals one [`finish_nested_field`] call finishes, so one write hold stays short however
/// many terminals a cycle reached: each may compact a tree and walk a terminal's lists.
pub const TERMINALS_PER_FINISH: usize = 64;

/// Finish the collection of up to [`TERMINALS_PER_FINISH`] terminals the NESTED field at
/// `field_index` reached: compact the numeric ones if `compact` is set and GC left them sparse,
/// then drop those left without records, and the routing nodes left without terminals. Returns
/// what it freed, and whether terminals are left to finish.
pub fn finish_nested_field(
    guard: &mut IndexSpecWriteGuard<'_>,
    field_index: FieldIndex,
    compact: bool,
) -> Result<(GcApplyStats, bool), GcError> {
    let mut stats = GcApplyStats::default();
    let index = nested_index_mut(guard, field_index)?;
    let pruned = index.finish_collection(TERMINALS_PER_FINISH, |terminal| {
        if compact && let PostingsMut::Numeric(tree) = terminal.postings_mut() {
            stats.record(compact_if_sparse(tree));
        }
    });
    tracing::debug!(
        field_index,
        terminals = pruned.terminals,
        "GC dropped emptied nested terminals"
    );
    stats.record(GcApplyStats {
        bytes_collected: pruned.bytes,
        block_count_delta: -(pruned.blocks as i64),
        ..GcApplyStats::default()
    });
    Ok((stats, index.has_collected()))
}

/// The NESTED fields with terminals a collection reached and did not finish, so a cycle with
/// nothing deleted still finishes them.
pub fn nested_fields_with_collected(spec: &IndexSpecReadGuard) -> Vec<FieldIndex> {
    spec.field_specs()
        .iter()
        .enumerate()
        .filter(|(_, fs)| fetch_nested_index(fs).is_some_and(NestedIndex::has_collected))
        .map(|(position, _)| field_index_of(position))
        .collect()
}

/// Collect GC deltas for every terminal of every NESTED field in the spec and write them to
/// the parent process.
///
/// For each NESTED field with an index, sends a `Some` [`NestedField`], then one `Some`
/// [`NestedTerminalDelta`] per terminal with garbage, followed by a `None` terminal
/// terminator. A final `None` field terminates the scanner. Write errors are surfaced so the
/// caller can terminate the child process.
pub fn collect_nested(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> io::Result<()> {
    for (position, fs) in spec.field_specs().iter().enumerate() {
        let Some(index) = fetch_nested_index(fs) else {
            continue;
        };
        serialize(
            writer,
            Some(NestedField {
                field_index: field_index_of(position),
            }),
        )?;
        let mut written = Ok(());
        index.routing().for_each_terminal(|path, terminal| {
            if written.is_err() {
                return;
            }
            // A posting list whose scan failed keeps its gone entries; this policy finds gone
            // documents in the doc table, not in a set it prunes, so a later cycle meets them.
            if let Some(work) = scan_terminal(terminal, live).work {
                written = serialize(
                    writer,
                    Some(NestedTerminalDelta {
                        path: to_wire(path),
                        work,
                    }),
                );
            }
        });
        written?;
        serialize(writer, None::<NestedTerminalDelta>)?;
    }
    serialize(writer, None::<NestedField>)
}

/// Nodes or tag values the fork parent applies per write hold.
pub const WORK_PER_APPLY: usize = 64;

/// `delta`'s work, cut into pieces of at most `max` nodes or values, each for the same terminal.
fn split_work(delta: NestedTerminalDelta, max: usize) -> Vec<NestedTerminalDelta> {
    let NestedTerminalDelta { path, work } = delta;
    let pieces: Vec<TerminalWork> = match work {
        TerminalWork::Numeric { unique_id, nodes } => chunked(nodes, max)
            .into_iter()
            .map(|nodes| TerminalWork::Numeric { unique_id, nodes })
            .collect(),
        TerminalWork::Tag(values) => chunked(values, max)
            .into_iter()
            .map(TerminalWork::Tag)
            .collect(),
    };
    pieces
        .into_iter()
        .map(|work| NestedTerminalDelta {
            path: path.clone(),
            work,
        })
        .collect()
}

fn chunked<T>(items: Vec<T>, max: usize) -> Vec<Vec<T>> {
    let mut pieces = Vec::new();
    let mut items = items.into_iter().peekable();
    while items.peek().is_some() {
        pieces.push(items.by_ref().take(max.max(1)).collect());
    }
    pieces
}

/// Receive and apply one NESTED field's terminal deltas, in write holds of at most
/// [`WORK_PER_APPLY`] nodes or values, then finish its collection.
///
/// Returns [`HandleOutcome::Done`] when the child sent the global terminator instead of a
/// field.
pub fn handle_nested_with(
    reader: &mut impl Read,
    spec: &mut impl SpecWriteAccess,
    stats: &mut GcApplyStats,
    clean_numeric_empty_nodes: bool,
) -> Result<HandleOutcome, GcError> {
    let Some(NestedField { field_index }) =
        deserialize::<Option<NestedField>>(&mut *reader, "decoding nested field")?
    else {
        return Ok(HandleOutcome::Done);
    };
    while let Some(delta) =
        deserialize::<Option<NestedTerminalDelta>>(&mut *reader, "decoding nested terminal")?
    {
        // A terminal's work is applied in pieces of at most `WORK_PER_APPLY` nodes or
        // values, one write hold each, as a large terminal's would otherwise hold the lock long.
        // A reshape needs no care here: this policy finds gone documents in the doc table, so
        // the next cycle meets what a split copied.
        for piece in split_work(delta, WORK_PER_APPLY) {
            let result = spec
                .with_write(|guard| apply_nested_terminal_with(guard, field_index, piece, None));
            match result {
                Ok(applied) => stats.record(applied.stats),
                // The rest of the field's stream must still be read off the pipe.
                Err(GcError::TargetGone(reason)) => {
                    tracing::debug!(reason, "fork GC skipped a stale nested terminal delta");
                    break;
                }
                Err(error) => return Err(error),
            }
        }
    }
    // Also after a stream with nothing applied: a cycle interrupted before its finish left
    // terminals for this one.
    loop {
        let finished = spec
            .with_write(|guard| finish_nested_field(guard, field_index, clean_numeric_empty_nodes));
        match finished {
            Ok((finished, more)) => {
                stats.record(finished);
                if !more {
                    break;
                }
            }
            Err(GcError::TargetGone(reason)) => {
                tracing::debug!(reason, "fork GC skipped finishing a nested field");
                break;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(HandleOutcome::Collected)
}

/// Handle one NESTED field off `fgc`'s pipe, flushing the tallied [`GcApplyStats`] to the spec
/// and the GC.
///
/// Returns [`HandleOutcome::Done`] once the child has sent every field.
pub fn handle_nested(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    let mut spec = fgc.index_spec();
    let mut stats = GcApplyStats::default();
    let clean_numeric_empty_nodes = fgc.clean_numeric_empty_nodes();

    let result = handle_nested_with(
        &mut fgc.reader(),
        &mut spec,
        &mut stats,
        clean_numeric_empty_nodes,
    );

    if stats != GcApplyStats::default() {
        spec.with_write(|guard| {
            stats.apply(fgc, guard);
            Ok(())
        })?;
    }

    result
}
