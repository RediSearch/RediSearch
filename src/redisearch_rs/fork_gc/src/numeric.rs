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
use serde::Serialize as _;

use numeric_range_tree::{Hll, NodeGcDelta, NodeIndex, NumericRangeTree};

use crate::util::SpecWriteAccess;
use crate::{ForkGC, Frame, GcApplyStats, HandleError, HandleOutcome};

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

pub type FieldHeader = (Box<[u8]>, u32);

/// A single node entry in the numeric GC wire protocol.
#[derive(Debug, PartialEq, Eq)]
pub struct NumericNodeDelta {
    pub position: u32,
    pub generation: u32,
    pub delta: NodeGcDelta,
}

impl NumericNodeDelta {
    /// Write this node entry to `writer`.
    pub fn encode(&self, writer: &mut impl Write) -> io::Result<()> {
        let mut delta_data = Vec::new();
        self.delta
            .delta
            .serialize(&mut rmp_serde::Serializer::new(&mut delta_data))
            .map_err(io::Error::other)?;

        let node_len = size_of_val(&self.position)
            + size_of_val(&self.generation)
            + delta_data.len()
            + size_of_val(&self.delta.registers_with_last_block)
            + size_of_val(&self.delta.registers_without_last_block);

        writer.write_all(&node_len.to_ne_bytes())?;
        writer.write_all(&self.position.to_ne_bytes())?;
        writer.write_all(&self.generation.to_ne_bytes())?;
        writer.write_all(&delta_data)?;
        writer.write_all(&self.delta.registers_with_last_block)?;
        writer.write_all(&self.delta.registers_without_last_block)
    }

    /// Read one node entry from `reader`.
    ///
    /// Returns `Ok(None)` when a [`Frame::Terminator`] is received (end of
    /// the node stream), or `Ok(Some(node))` for a valid entry.
    pub fn decode<R: Read>(reader: &mut R) -> Result<Option<Self>, HandleError<TreeError>> {
        // The individual body reads are consecutive bytes of one entry we have
        // already committed to, so a failure in any of them has the same
        // diagnosis: the entry was truncated. They share one message.
        let read_body = |reader: &mut R, buffer| {
            reader
                .read_exact(buffer)
                .map_err(|e| HandleError::codec("reading the numeric node entry", e))
        };

        let mut len_bytes = [0u8; size_of::<usize>()];
        reader
            .read_exact(&mut len_bytes)
            .map_err(|e| HandleError::codec("reading the numeric node length prefix", e))?;
        let node_len = usize::from_ne_bytes(len_bytes);

        if node_len == crate::frame::TERMINATOR {
            return Ok(None);
        }

        let minimum_node_len = size_of::<u32>() + size_of::<u32>() + Hll::size() * 2;
        let delta_data_len = node_len.checked_sub(minimum_node_len).ok_or_else(|| {
            HandleError::codec(
                "numeric node length too small",
                format!("{node_len} is below the minimum {minimum_node_len}"),
            )
        })?;

        let mut pos_bytes = [0u8; size_of::<u32>()];
        read_body(reader, &mut pos_bytes)?;
        let mut gen_bytes = [0u8; size_of::<u32>()];
        read_body(reader, &mut gen_bytes)?;
        let mut delta_data = vec![0u8; delta_data_len];
        read_body(reader, &mut delta_data)?;
        let mut registers_with_last_block = [0u8; Hll::size()];
        read_body(reader, &mut registers_with_last_block)?;
        let mut registers_without_last_block = [0u8; Hll::size()];
        read_body(reader, &mut registers_without_last_block)?;

        Ok(Some(NumericNodeDelta {
            position: u32::from_ne_bytes(pos_bytes),
            generation: u32::from_ne_bytes(gen_bytes),
            delta: NodeGcDelta {
                delta: rmp_serde::from_slice(&delta_data)
                    .map_err(|e| HandleError::codec("decoding the numeric node delta", e))?,
                registers_with_last_block,
                registers_without_last_block,
            },
        }))
    }
}

/// A numeric tree resolved from a field header.
///
/// Resolving a field by name walks the spec's field list, so [`apply_node_stream`]
/// does it once and reuses the result for the rest of the field's node stream.
///
/// The index stays valid because a spec's field array is only ever appended to.
#[derive(Clone, Copy)]
struct ResolvedTree {
    field_index: usize,
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

        if u32::from(tree.unique_id()) != unique_id {
            return Err(TreeError::new(
                "the field's numeric tree was replaced since the child scanned it",
            ));
        }

        Ok(Self { field_index })
    }

    /// Borrow the resolved tree under the write lock.
    fn get_mut<'g>(
        &self,
        guard: &'g mut IndexSpecWriteGuard<'_>,
    ) -> Result<&'g mut NumericRangeTree, HandleError<TreeError>> {
        guard
            .field_specs_mut()
            .get_mut(self.field_index)
            .and_then(|fs| fs.tree_mut())
            .ok_or(TreeError::new(
                "the numeric tree was dropped while its deltas were being applied",
            ))
    }
}

/// Collect GC deltas for every numeric and geo field in the spec and write
/// them to the parent process.
///
/// For each NUMERIC or GEO field whose tree has been initialised, sends:
///  1. A [`Frame::Data`] carrying the field name, followed by the field's
///     unique tree ID as a raw native-endian `u32`.
///  2. One [`NumericNodeDelta`] per tree node with GC work.
///  3. A [`Frame::Terminator`] ending the node stream.
///
/// A final [`Frame::Terminator`] is written once all fields are processed.
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
        // Send field header: Frame::Data(field_name) + raw u32 unique_id.
        let field_name = fs.field_name().secret_value().to_bytes();
        Frame::data(field_name).encode(writer)?;
        writer.write_all(&u32::from(tree.unique_id()).to_ne_bytes())?;

        for (node_idx, delta) in tree
            .indexed_iter()
            .filter_map(|(idx, node)| node.scan_gc(&|id| spec.doc_exists(id)).map(|d| (idx, d)))
        {
            NumericNodeDelta {
                position: node_idx.position(),
                generation: node_idx.generation(),
                delta,
            }
            .encode(writer)?;
        }

        Frame::Terminator.encode(writer)?;
    }

    // Global terminator: tells the parent no more fields follow.
    Frame::Terminator.encode(writer)
}

/// Read one field header from `reader`.
///
/// Return `None` when the child sent the global terminator instead.
pub fn receive_field_header(
    reader: &mut impl Read,
) -> Result<Option<FieldHeader>, HandleError<TreeError>> {
    let frame = Frame::decode(reader)
        .map_err(|e| HandleError::codec("reading the numeric field-name frame", e))?;

    let field_name = match frame {
        Frame::Terminator => return Ok(None),
        Frame::Data(name) => name.into_inner(),
        Frame::Empty => {
            return Err(HandleError::codec(
                "expected a field-name or terminator frame for numeric",
                "got an empty frame",
            ));
        }
    };

    let mut id_bytes = [0u8; size_of::<u32>()];
    reader
        .read_exact(&mut id_bytes)
        .map_err(|e| HandleError::codec("reading the numeric field unique id", e))?;

    Ok(Some((field_name, u32::from_ne_bytes(id_bytes))))
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
        numeric_nodes_missed: 0,
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

    while let Some(node) = NumericNodeDelta::decode(reader)? {
        let node_stats = spec.try_with_write(|guard| {
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
    spec.try_with_write(|guard| {
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
    let Some((field_name, unique_id)) = receive_field_header(reader)? else {
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
        spec.with_write(|guard| stats.apply(fgc, guard));
    }

    result
}
