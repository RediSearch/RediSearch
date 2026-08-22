/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection for numeric and geo inverted indexes.

use std::io::{self, Read, Write};

use field_spec::FieldSpecType;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use serde::{Deserialize, Serialize};

use numeric_range_tree::{NodeGcDelta, NodeIndex, NumericRangeTree};

use crate::util::{SpecWriteAccess, deserialize, serialize};
use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// A numeric tree error with a message explaining the specific issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{msg}")]
pub struct TreeError {
    msg: &'static str,
}

impl TreeError {
    /// Build a [`HandleError::Custom`] carrying `msg`.
    const fn new(msg: &'static str) -> HandleError<Self> {
        HandleError::Custom(Self { msg })
    }
}

/// A numeric or geo field scanned by the child process.
///
/// The child uses borrowed field-name bytes to avoid copies; the parent
/// deserializes the default owned form.
#[derive(Debug, Serialize, Deserialize)]
pub struct NumericField<T = Box<[u8]>> {
    /// Field name at scan time.
    pub field_name: T,
    /// [`NumericRangeTree::unique_id`] at scan time.
    pub unique_id: u32,
}

/// A single node delta in the numeric GC wire protocol.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NumericNodeDelta {
    pub position: u32,
    pub generation: u32,
    pub delta: NodeGcDelta,
}

/// A numeric tree resolved from a field header.
///
/// Resolving a field by name walks the spec's field list, so [`apply_node_stream`]
/// does it once and reuses the result for the rest of the field's node stream.
///
/// The index survives the lock releases between nodes because a spec's field array
/// is only ever appended to. [`Self::get_mut`] re-checks the tree's unique id
/// regardless, so a future violation of that invariant surfaces as a [`TreeError`]
/// instead of applying deltas to an unrelated tree.
#[derive(Clone, Copy)]
struct ResolvedTree {
    field_index: usize,
    unique_id: u32,
}

impl ResolvedTree {
    /// Find the field named `field_name` and resolve it, after checking that its
    /// numeric tree is the one the child scanned.
    ///
    /// Returns [`TreeError`] when no field matches, the field has no tree,
    /// or the tree's unique id differs from `unique_id`.
    fn resolve(
        guard: &mut IndexSpecWriteGuard<'_>,
        field_name: &[u8],
        unique_id: u32,
    ) -> Result<Self, HandleError<TreeError>> {
        let (field_index, fs) = guard
            .field_specs_mut()
            .iter_mut()
            .enumerate()
            .find(|(_, fs)| fs.field_name().secret_value().to_bytes() == field_name)
            .ok_or(TreeError::new(
                "no field in the spec matches the scanned field name",
            ))?;

        let tree = fs
            .tree()
            .ok_or(TreeError::new("the field has no numeric tree"))?;

        Self::check_unique_id(tree, unique_id)?;

        Ok(Self {
            field_index,
            unique_id,
        })
    }

    /// Borrow the resolved tree under the write lock, re-checking that the field
    /// still holds the tree the child scanned.
    fn get_mut<'g>(
        &self,
        guard: &'g mut IndexSpecWriteGuard<'_>,
    ) -> Result<&'g mut NumericRangeTree, HandleError<TreeError>> {
        let tree = guard
            .field_specs_mut()
            .get_mut(self.field_index)
            .and_then(|fs| fs.tree_mut())
            .ok_or(TreeError::new(
                "the numeric tree was dropped while its deltas were being applied",
            ))?;

        Self::check_unique_id(tree, self.unique_id)?;

        Ok(tree)
    }

    /// Confirm `tree` is the one the child scanned.
    fn check_unique_id(
        tree: &NumericRangeTree,
        unique_id: u32,
    ) -> Result<(), HandleError<TreeError>> {
        if u32::from(tree.unique_id()) != unique_id {
            return Err(TreeError::new(
                "the field's numeric tree is not the one the child scanned",
            ));
        }

        Ok(())
    }
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
pub fn collect_numeric(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    for (fs, tree) in spec
        .field_specs()
        .iter()
        .filter(|fs| {
            fs.types()
                .intersects(FieldSpecType::Numeric | FieldSpecType::Geo)
        })
        .filter_map(|fs| fs.tree().map(|tree| (fs, tree)))
    {
        let field_name = fs.field_name().secret_value().to_bytes();
        serialize(
            writer,
            Some(NumericField {
                field_name,
                unique_id: u32::from(tree.unique_id()),
            }),
        )?;

        for node_delta in tree.indexed_iter().filter_map(|(node_idx, node)| {
            node.scan_gc(&|id| spec.doc_exists(id))
                .map(|delta| NumericNodeDelta {
                    position: node_idx.position(),
                    generation: node_idx.generation(),
                    delta,
                })
        }) {
            serialize(writer, Some(node_delta))?;
        }

        serialize(writer, None::<NumericNodeDelta>)?;
    }

    serialize(writer, None::<NumericField<&[u8]>>)
}

/// Apply one node's delta to `tree`.
///
/// Return [`GcApplyStats`] on success or `None` when the node is no longer present in the live tree.
fn apply_numeric_node(tree: &mut NumericRangeTree, node: NumericNodeDelta) -> Option<GcApplyStats> {
    let result = tree.apply_gc_to_node(
        NodeIndex::from_raw_parts(node.position, node.generation),
        node.delta,
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
    field_name: &[u8],
    unique_id: u32,
    stats: &mut GcApplyStats,
) -> Result<Option<ResolvedTree>, HandleError<TreeError>> {
    let mut resolved_tree = None;

    while let Some(node) = deserialize(&mut *reader, "decoding numeric node")? {
        let node_stats = spec.with_write(|guard| {
            let resolved_tree = match resolved_tree {
                Some(resolved_tree) => resolved_tree,
                None => {
                    let tree = ResolvedTree::resolve(guard, field_name, unique_id)?;
                    resolved_tree = Some(tree);
                    tree
                }
            };

            Ok(apply_numeric_node(resolved_tree.get_mut(guard)?, node))
        })?;

        match node_stats {
            Some(node_stats) => stats.record(node_stats),
            None => stats.numeric_nodes_missed += 1,
        }
    }

    Ok(resolved_tree)
}

/// Compact the tree if GC left it sparse, recording what the trim freed into `stats`.
fn trim_empty_nodes(
    spec: &mut impl SpecWriteAccess,
    resolved_tree: ResolvedTree,
    stats: &mut GcApplyStats,
) -> Result<(), HandleError<TreeError>> {
    spec.with_write(|guard| {
        let result = resolved_tree.get_mut(guard)?.compact_if_sparse();

        stats.record(GcApplyStats {
            bytes_collected: result.inverted_index_size_delta.min(0).unsigned_abs() as usize,
            block_count_delta: result.block_count_delta.into(),
            ..GcApplyStats::default()
        });

        Ok(())
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
) -> Result<HandleOutcome, HandleError<TreeError>> {
    let Some(NumericField {
        field_name,
        unique_id,
    }) = deserialize::<Option<NumericField>, TreeError>(reader, "decoding numeric field")?
    else {
        return Ok(HandleOutcome::Done);
    };

    let resolved_tree = apply_node_stream(reader, spec, &field_name, unique_id, stats)?;

    if clean_numeric_empty_nodes && let Some(resolved_tree) = resolved_tree {
        trim_empty_nodes(spec, resolved_tree, stats)?;
    }

    Ok(HandleOutcome::Collected)
}

/// Handle one field off `fgc`'s pipe, flushing the tallied [`GcApplyStats`] to the spec and the GC.
///
/// Return [`HandleOutcome::Done`] once the child has sent every field.
pub fn handle_numeric(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError<TreeError>> {
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
