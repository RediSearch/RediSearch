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
use index_spec::IndexSpecReadGuard;
use serde::Serialize as _;

use numeric_range_tree::{Hll, NodeGcDelta};

use crate::Frame;

/// A single node entry in the numeric GC wire protocol.
pub struct NumericNodeDelta {
    pub position: u32,
    pub generation: u32,
    pub delta: NodeGcDelta,
}

impl NumericNodeDelta {
    /// Write this node entry to `writer`.
    pub fn encode(self, writer: &mut impl Write) -> io::Result<()> {
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
    pub fn decode(reader: &mut impl Read) -> io::Result<Option<Self>> {
        let mut len_bytes = [0u8; size_of::<usize>()];
        reader.read_exact(&mut len_bytes)?;
        let node_len = usize::from_ne_bytes(len_bytes);

        if node_len == crate::frame::TERMINATOR {
            return Ok(None);
        }

        let delta_data_len = node_len
            .checked_sub(size_of::<u32>() + size_of::<u32>() + Hll::size() * 2)
            .ok_or_else(|| io::Error::other("numeric node length too small"))?;

        let mut pos_bytes = [0u8; size_of::<u32>()];
        reader.read_exact(&mut pos_bytes)?;
        let mut gen_bytes = [0u8; size_of::<u32>()];
        reader.read_exact(&mut gen_bytes)?;
        let mut delta_data = vec![0u8; delta_data_len];
        reader.read_exact(&mut delta_data)?;
        let mut registers_with_last_block = [0u8; Hll::size()];
        reader.read_exact(&mut registers_with_last_block)?;
        let mut registers_without_last_block = [0u8; Hll::size()];
        reader.read_exact(&mut registers_without_last_block)?;

        Ok(Some(NumericNodeDelta {
            position: u32::from_ne_bytes(pos_bytes),
            generation: u32::from_ne_bytes(gen_bytes),
            delta: NodeGcDelta {
                delta: rmp_serde::from_slice(&delta_data).map_err(io::Error::other)?,
                registers_with_last_block,
                registers_without_last_block,
            },
        }))
    }
}

/// Collect GC deltas for every numeric and geo field in the spec and write
/// them to the parent process.
///
/// For each NUMERIC or GEO field whose tree has been initialised, sends:
///  1. A [`Frame::Data`] carrying the field name, followed by the field's
///     unique tree ID as a raw native-endian `u64`.
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
        // Send field header: Frame::Data(field_name) + raw u64 unique_id.
        let field_name = fs.field_name().into_secret_value().to_bytes();
        Frame::data(field_name).encode(writer)?;
        // The C side stores the u32 tree ID in a u64 before sending (zero-extends).
        let unique_id: u64 = u32::from(tree.unique_id()).into();
        writer.write_all(&unique_id.to_ne_bytes())?;

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
