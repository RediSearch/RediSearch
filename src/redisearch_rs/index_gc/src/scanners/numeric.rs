/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection for numeric and geo inverted indexes.

use std::{
    collections::HashSet,
    io::{self, Read, Write},
    ops::RangeInclusive,
};

use field_spec::FieldSpecType;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocId, DocLiveness};
use serde::{Deserialize, Serialize};

use numeric_range_tree::{NodeGcDelta, NodeIndex, NumericRangeTree};

use crate::fork::wire::{deserialize, serialize};
use crate::spec_access::SpecWriteAccess;
use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// A numeric or geo field scanned by the child process.
#[derive(Debug, Serialize, Deserialize)]
pub struct NumericField {
    /// Position of the field in the spec's field list.
    pub field_index: ffi::t_fieldIndex,
    /// [`NumericRangeTree::unique_id`] at scan time.
    pub unique_id: u32,
}

/// A single node delta in the numeric GC wire protocol.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NumericNodeDelta {
    pub position: u32,
    pub generation: u32,
    pub delta: NodeGcDelta,
}

/// A node delta scanned from a snapshot, with the doc IDs the snapshot spanned,
/// so the consumer can tell which deleted IDs a dropped or denied delta leaves
/// in the index.
pub struct ScannedNumericNode {
    /// The delta to apply, as the fork child would send it.
    pub node: NumericNodeDelta,
    /// What the whole scanned index spanned; `None` for an index without blocks.
    pub doc_id_range: Option<RangeInclusive<DocId>>,
    /// What its last block spanned. A numeric apply has no liveness source to
    /// rescan a grown last block with (the cardinality repair needs the whole
    /// range), so [`apply_numeric_node`] denies it, and the consumer keeps these
    /// IDs for its next cycle.
    pub last_block_doc_id_range: Option<RangeInclusive<DocId>>,
}

/// One NUMERIC or GEO field's tree, named so it can be found again across spec
/// lock releases.
///
/// The field position survives them because a spec's field array is only ever
/// appended to. Every lookup re-checks the tree's unique id regardless, so a
/// replaced tree, or a future violation of that invariant, is never mistaken for
/// the one named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NumericTreeId {
    /// Position of the field in the spec's field array.
    pub field_index: usize,
    /// [`NumericRangeTree::unique_id`] of the tree named.
    pub unique_id: u32,
}

impl NumericTreeId {
    /// Name the tree of the field at `field_index`, after checking that it is
    /// the one the child scanned.
    ///
    /// Returns an apply error when no field matches, the field has no tree, or
    /// the tree's unique id differs from `unique_id`.
    fn resolve(
        guard: &mut IndexSpecWriteGuard<'_>,
        field_index: ffi::t_fieldIndex,
        unique_id: u32,
    ) -> Result<Self, GcError> {
        let field_index = usize::from(field_index);
        let fs = guard
            .field_specs_mut()
            .get_mut(field_index)
            .ok_or(GcError::ApplyError(
                "no field in the spec is at the scanned field index",
            ))?;

        let tree = fs
            .tree()
            .ok_or(GcError::ApplyError("the field has no numeric tree"))?;

        let id = Self {
            field_index,
            unique_id,
        };
        id.check(tree)?;
        Ok(id)
    }

    /// The tree under the read lock; `None` when the field no longer holds it.
    pub fn tree<'s>(&self, spec: &'s IndexSpecReadGuard) -> Option<&'s NumericRangeTree> {
        let tree = spec.field_specs().get(self.field_index)?.tree()?;
        self.check(tree).ok().map(|()| tree)
    }

    /// The tree under the write lock, or an apply error when the field no longer
    /// holds it.
    pub(crate) fn tree_mut<'g>(
        &self,
        guard: &'g mut IndexSpecWriteGuard<'_>,
    ) -> Result<&'g mut NumericRangeTree, GcError> {
        let tree = guard
            .field_specs_mut()
            .get_mut(self.field_index)
            .and_then(|fs| fs.tree_mut())
            .ok_or(GcError::ApplyError(
                "the numeric tree was dropped while its deltas were being applied",
            ))?;
        self.check(tree)?;
        Ok(tree)
    }

    fn check(&self, tree: &NumericRangeTree) -> Result<(), GcError> {
        if u32::from(tree.unique_id()) != self.unique_id {
            return Err(GcError::ApplyError(
                "the field's numeric tree is not the one the child scanned",
            ));
        }
        Ok(())
    }
}

/// The nodes of one NUMERIC or GEO field's tree, listed so they can be scanned
/// one at a time across spec lock releases.
#[derive(Debug, Clone)]
pub struct NumericTreeKey {
    /// The tree the nodes belong to.
    pub id: NumericTreeId,
    /// [`NumericRangeTree::revision_id`] when the nodes were listed; it moves on
    /// every structural change, so an unchanged one means `nodes` is still every node.
    pub revision_id: u32,
    /// Every node of the tree, children before parents, as [`collect_numeric`]
    /// visits them.
    pub nodes: Vec<NodeIndex>,
}

/// List the nodes of every initialised NUMERIC and GEO tree in the spec.
pub fn numeric_tree_keys(spec: &IndexSpecReadGuard) -> Vec<NumericTreeKey> {
    spec.field_specs()
        .iter()
        .enumerate()
        .filter(|(_, fs)| {
            fs.types()
                .intersects(FieldSpecType::Numeric | FieldSpecType::Geo)
        })
        .filter_map(|(field_index, fs)| {
            let tree = fs.tree()?;
            Some(NumericTreeKey {
                id: NumericTreeId {
                    field_index,
                    unique_id: u32::from(tree.unique_id()),
                },
                revision_id: tree.revision_id(),
                nodes: tree
                    .indexed_post_order_iter()
                    .map(|(node_idx, _)| node_idx)
                    .collect(),
            })
        })
        .collect()
}

/// The doc IDs spanned by the ranges of the nodes `key`'s tree gained since `key`
/// listed its nodes (`listed`, the same set), when its structure changed since.
///
/// A leaf that splits copies every entry of its range into two new leaves, so a
/// cycle that cleans the listed nodes leaves those copies, dead entries included,
/// uncollected; the caller keeps the IDs they may hold for its next cycle. Empty
/// when nothing changed, or when the tree is not the one listed: a tree lives as
/// long as its field, so that means the spec was replaced and its deltas failed.
pub fn ranges_of_nodes_added_since(
    spec: &IndexSpecReadGuard,
    key: &NumericTreeKey,
    listed: &HashSet<NodeIndex>,
) -> Vec<RangeInclusive<DocId>> {
    let Some(tree) = key.id.tree(spec) else {
        return Vec::new();
    };
    if tree.revision_id() == key.revision_id {
        return Vec::new();
    }
    tree.indexed_post_order_iter()
        .filter(|(node_idx, _)| !listed.contains(node_idx))
        .filter_map(|(_, node)| node.range()?.entries().doc_id_range())
        .collect()
}

/// A numeric node's range index copied under the lock, for a scan without it.
pub struct NumericNodeSnapshot {
    position: u32,
    generation: u32,
    /// Whether the node was a leaf when copied; see
    /// [`NodeGcDelta::scanned_leaf`](numeric_range_tree::NodeGcDelta::scanned_leaf).
    leaf: bool,
    index: numeric_range_tree::NumericIndex,
}

impl NumericNodeSnapshot {
    /// The doc IDs the copied range spans; see
    /// [`NumericIndex::doc_id_range`](numeric_range_tree::NumericIndex::doc_id_range).
    pub fn doc_id_range(&self) -> Option<RangeInclusive<DocId>> {
        self.index.doc_id_range()
    }

    /// The doc IDs the copied range's last block spans; see
    /// [`NumericIndex::last_block_doc_id_range`](numeric_range_tree::NumericIndex::last_block_doc_id_range).
    pub fn last_block_doc_id_range(&self) -> Option<RangeInclusive<DocId>> {
        self.index.last_block_doc_id_range()
    }

    /// Run the GC scan on the copy, with the liveness source the copy was taken with;
    /// `Ok(None)` when no document was deleted from the range, the error when the
    /// copy would not decode, so the caller can keep the IDs [`Self::doc_id_range`]
    /// reports for another try.
    pub fn scan(self, live: impl DocLiveness) -> std::io::Result<Option<ScannedNumericNode>> {
        let Some(delta) = numeric_range_tree::scan_range_index_gc(&self.index, live, self.leaf)?
        else {
            return Ok(None);
        };
        Ok(Some(ScannedNumericNode {
            node: NumericNodeDelta {
                position: self.position,
                generation: self.generation,
                delta,
            },
            doc_id_range: self.index.doc_id_range(),
            last_block_doc_id_range: self.index.last_block_doc_id_range(),
        }))
    }
}

/// Copy the range index of node `node_idx` of the tree `id` names, every block of it: the
/// scan's cardinality repair has to see every surviving entry.
///
/// Returns `None` when the field's tree was replaced, the node was removed, has
/// no range, or `live` finds no gone document in the range's span.
pub fn snapshot_numeric_node(
    spec: &IndexSpecReadGuard,
    id: &NumericTreeId,
    node_idx: NodeIndex,
    live: &impl DocLiveness,
) -> Option<NumericNodeSnapshot> {
    let node = id.tree(spec)?.try_node(node_idx)?;
    let index = node.range()?.entries().gc_snapshot(live)?;
    Some(NumericNodeSnapshot {
        position: node_idx.position(),
        generation: node_idx.generation(),
        leaf: node.is_leaf(),
        index,
    })
}

/// Run the GC scan of node `node_idx` of the tree `id` names.
///
/// Returns `None` when the field's tree was replaced, the node was removed,
/// the scan found nothing to collect, or the scan failed.
pub fn scan_numeric_node(
    spec: &IndexSpecReadGuard,
    id: &NumericTreeId,
    node_idx: NodeIndex,
    live: impl DocLiveness + Copy,
) -> Option<NumericNodeDelta> {
    let delta = id.tree(spec)?.try_node(node_idx)?.scan_gc(live)?;
    Some(NumericNodeDelta {
        position: node_idx.position(),
        generation: node_idx.generation(),
        delta,
    })
}

/// Collect GC deltas for every numeric and geo field in the spec and write
/// them to the parent process.
///
/// For each NUMERIC or GEO field whose tree has been initialised, sends a
/// `Some` [`NumericField`], then one `Some` [`NumericNodeDelta`] per tree node
/// with GC work, followed by a `None` node terminator. A final `None` field
/// terminates the scanner.
///
/// Write errors are surfaced so the caller can terminate the child process.
///
/// `live` says which documents still exist; see [`DocLiveness`].
pub fn collect_numeric(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> io::Result<()> {
    for (field_index, tree) in spec
        .field_specs()
        .iter()
        .enumerate()
        .filter(|(_, fs)| {
            fs.types()
                .intersects(FieldSpecType::Numeric | FieldSpecType::Geo)
        })
        .filter_map(|(i, fs)| fs.tree().map(|tree| (i, tree)))
    {
        let field_index = ffi::t_fieldIndex::try_from(field_index)
            .expect("a spec has fewer fields than a field index can count");
        serialize(
            writer,
            Some(NumericField {
                field_index,
                unique_id: u32::from(tree.unique_id()),
            }),
        )?;

        for node_delta in tree
            .indexed_post_order_iter()
            .filter_map(|(node_idx, node)| {
                node.scan_gc(live).map(|delta| NumericNodeDelta {
                    position: node_idx.position(),
                    generation: node_idx.generation(),
                    delta,
                })
            })
        {
            serialize(writer, Some(node_delta))?;
        }

        serialize(writer, None::<NumericNodeDelta>)?;
    }

    serialize(writer, None::<NumericField>)
}

/// Apply one node's delta to `tree`.
///
/// Return [`GcApplyStats`] on success or `None` when the node is no longer present in the live tree.
pub(crate) fn apply_numeric_node(
    tree: &mut NumericRangeTree,
    node: NumericNodeDelta,
) -> Option<GcApplyStats> {
    apply_numeric_node_with(tree, node, None)
}

/// [`apply_numeric_node`] with a liveness source, so a last block that grew since the
/// scan is rescanned instead of denied; see
/// [`NumericRangeTree::apply_gc_to_node_with`].
pub(crate) fn apply_numeric_node_with(
    tree: &mut NumericRangeTree,
    node: NumericNodeDelta,
    live: Option<&dyn DocLiveness>,
) -> Option<GcApplyStats> {
    let result = tree.apply_gc_to_node_with(
        NodeIndex::from_raw_parts(node.position, node.generation),
        node.delta,
        live,
    )?;

    let info = result.index_gc_info;
    Some(GcApplyStats {
        records_removed: info.entries_removed,
        bytes_collected: info.bytes_freed,
        bytes_allocated: info.bytes_allocated,
        block_count_delta: info.block_count_delta,
        blocks_denied: info.ignored_last_block as u64,
        ..GcApplyStats::default()
    })
}

/// Apply every node delta in one field's stream, re-acquiring the spec lock per node.
///
/// Return the resolved tree from the first node, or `None` when the stream held no nodes.
fn apply_node_stream(
    reader: &mut impl Read,
    spec: &mut impl SpecWriteAccess,
    field_index: ffi::t_fieldIndex,
    unique_id: u32,
    stats: &mut GcApplyStats,
) -> Result<Option<NumericTreeId>, GcError> {
    let mut resolved_tree = None;

    while let Some(node) = deserialize(&mut *reader, "decoding numeric node")? {
        let node_stats = spec.with_write(|guard| {
            let resolved_tree = match resolved_tree {
                Some(resolved_tree) => resolved_tree,
                None => {
                    let tree = NumericTreeId::resolve(guard, field_index, unique_id)?;
                    resolved_tree = Some(tree);
                    tree
                }
            };

            Ok(apply_numeric_node(resolved_tree.tree_mut(guard)?, node))
        })?;

        match node_stats {
            Some(node_stats) => stats.record(node_stats),
            None => stats.numeric_nodes_missed += 1,
        }
    }

    Ok(resolved_tree)
}

/// Compact the tree if GC left it sparse, recording what the trim freed into `stats`.
pub(crate) fn trim_empty_nodes(
    spec: &mut impl SpecWriteAccess,
    resolved_tree: NumericTreeId,
    stats: &mut GcApplyStats,
) -> Result<(), GcError> {
    spec.with_write(|guard| {
        stats.record(compact_tree(guard, resolved_tree)?);
        Ok(())
    })
}

/// Compact the resolved tree if GC left it sparse, under the caller's write lock.
///
/// Returns what the compaction freed.
pub(crate) fn compact_tree(
    guard: &mut IndexSpecWriteGuard<'_>,
    resolved_tree: NumericTreeId,
) -> Result<GcApplyStats, GcError> {
    let result = resolved_tree.tree_mut(guard)?.compact_if_sparse();

    Ok(GcApplyStats {
        bytes_collected: result.inverted_index_size_delta.min(0).unsigned_abs() as usize,
        block_count_delta: result.block_count_delta.into(),
        ..GcApplyStats::default()
    })
}

/// Receive and apply one field's node deltas, then compact if `clean_numeric_empty_nodes`.
///
/// Return [`HandleOutcome::Done`] when the child sent the global terminator instead of a header.
pub fn handle_numeric_with(
    reader: &mut impl Read,
    spec: &mut impl SpecWriteAccess,
    stats: &mut GcApplyStats,
    clean_numeric_empty_nodes: bool,
) -> Result<HandleOutcome, GcError> {
    let Some(NumericField {
        field_index,
        unique_id,
    }) = deserialize::<Option<NumericField>>(reader, "decoding numeric field")?
    else {
        return Ok(HandleOutcome::Done);
    };

    let resolved_tree = apply_node_stream(reader, spec, field_index, unique_id, stats)?;

    if clean_numeric_empty_nodes && let Some(resolved_tree) = resolved_tree {
        trim_empty_nodes(spec, resolved_tree, stats)?;
    }

    Ok(HandleOutcome::Collected)
}

/// Handle one field off `fgc`'s pipe, flushing the tallied [`GcApplyStats`] to the spec and the GC.
///
/// Return [`HandleOutcome::Done`] once the child has sent every field.
pub fn handle_numeric(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    let mut spec = fgc.index_spec();
    let mut stats = GcApplyStats::default();
    let clean_numeric_empty_nodes = fgc.clean_numeric_empty_nodes();

    let result = handle_numeric_with(
        &mut fgc.reader(),
        &mut spec,
        &mut stats,
        clean_numeric_empty_nodes,
    );

    // No need to lock the spec and update the stats if there are none.
    if stats != GcApplyStats::default() {
        spec.with_write(|guard| {
            stats.apply(fgc, guard);
            Ok(())
        })?;
    }

    result
}
