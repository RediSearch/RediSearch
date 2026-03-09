/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Debug and introspection utilities for the numeric range tree.
//!
//! This module provides functions to dump tree state for debugging purposes.
//! These are used by the FT.DEBUG commands (NUMIDX_SUMMARY, DUMP_NUMIDX, DUMP_NUMIDXTREE).

use inverted_index::{IndexReader, RSIndexResult};
use redis_reply::{ArrayBuilder, MapBuilder, RedisModuleCtx, Replier};

use crate::{NodeIndex, NumericIndex, NumericRange, NumericRangeNode, NumericRangeTree};

/// Block efficiency statistics for computing average memory efficiency.
#[derive(Debug, Default)]
struct BlocksEfficiencyStats {
    /// Sum of (numEntries / numBlocks) across all ranges.
    total_efficiency: f64,
}

/// Reply with a summary of the numeric range tree.
///
/// This implements the FT.DEBUG NUMIDX_SUMMARY command response format.
/// When `tree` is `None` (index not yet created), all values are reported as zero.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_summary(ctx: *mut RedisModuleCtx, tree: Option<&NumericRangeTree>) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut arr = replier.array();

    arr.simple_string(c"numRanges");
    arr.long_long(tree.map_or(0, |t| t.num_ranges() as i64));
    arr.simple_string(c"numLeaves");
    arr.long_long(tree.map_or(0, |t| t.num_leaves() as i64));
    arr.simple_string(c"numEntries");
    arr.long_long(tree.map_or(0, |t| t.num_entries() as i64));
    arr.simple_string(c"lastDocId");
    arr.long_long(tree.map_or(0, |t| t.last_doc_id() as i64));
    arr.simple_string(c"revisionId");
    arr.long_long(tree.map_or(0, |t| t.revision_id() as i64));
    arr.simple_string(c"emptyLeaves");
    arr.long_long(tree.map_or(0, |t| t.empty_leaves() as i64));
    arr.simple_string(c"RootMaxDepth");
    arr.long_long(tree.map_or(0, |t| t.root().max_depth() as i64));
    arr.simple_string(c"MemoryUsage");
    arr.long_long(tree.map_or(0, |t| t.mem_usage() as i64));
}

/// Reply with a dump of the numeric index entries.
///
/// This implements the FT.DEBUG DUMP_NUMIDX command response format.
/// Each range in the tree is dumped as an array of document IDs.
/// If `with_headers` is true, each range's entries are prefixed with header info.
/// When `tree` is `None` (index not yet created), an empty array is returned.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_dump_index(
    ctx: *mut RedisModuleCtx,
    tree: Option<&NumericRangeTree>,
    with_headers: bool,
) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut arr = replier.array();

    if let Some(tree) = tree {
        for node in tree.iter() {
            if let Some(range) = node.range() {
                if with_headers {
                    let mut fixed_arr = arr.fixed_array(2);
                    range
                        .entries()
                        .summary()
                        .reply_with_inverted_index_header(&mut fixed_arr);
                    reply_range_entries(&mut fixed_arr.array(), range);
                } else {
                    reply_range_entries(&mut arr.array(), range);
                }
            }
        }
    }
    // None → empty array (no iterations)
}

/// Reply with a dump of the numeric index tree structure.
///
/// This implements the FT.DEBUG DUMP_NUMIDXTREE command response format.
/// The tree structure is dumped as a nested map.
/// If `minimal` is true, range entries are omitted.
/// When `tree` is `None` (index not yet created), all values are zero with an empty root.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_dump_tree(
    ctx: *mut RedisModuleCtx,
    tree: Option<&NumericRangeTree>,
    minimal: bool,
) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut map = replier.map();

    map.kv_long_long(c"numRanges", tree.map_or(0, |t| t.num_ranges() as i64));
    map.kv_long_long(c"numEntries", tree.map_or(0, |t| t.num_entries() as i64));
    map.kv_long_long(c"lastDocId", tree.map_or(0, |t| t.last_doc_id() as i64));
    map.kv_long_long(c"revisionId", tree.map_or(0, |t| t.revision_id() as i64));
    map.kv_long_long(
        c"uniqueId",
        tree.map_or(0, |t| u32::from(t.unique_id()) as i64),
    );
    map.kv_long_long(c"emptyLeaves", tree.map_or(0, |t| t.empty_leaves() as i64));

    let stats = if let Some(tree) = tree {
        let mut root_map = map.kv_map(c"root");
        reply_node_debug(&mut root_map, tree, tree.root_index(), minimal)
    } else {
        map.kv_fixed_map(c"root", 0);
        BlocksEfficiencyStats::default()
    };

    {
        let mut stats_map = map.kv_fixed_map(c"Tree stats", 1);
        let num_ranges = tree.map_or(0, |t| t.num_ranges());
        let avg_efficiency = if num_ranges > 0 {
            stats.total_efficiency / num_ranges as f64
        } else {
            0.0
        };
        stats_map.kv_double(
            c"Average memory efficiency (numEntries/size)/numRanges",
            avg_efficiency,
        );
    }
}

/// Reply with the entries (doc_ids) from a range.
fn reply_range_entries(entries_arr: &mut ArrayBuilder<'_>, range: &NumericRange) {
    let mut reader = range.reader();
    let mut result = RSIndexResult::numeric(0.0);
    while reader.next_record(&mut result).unwrap_or(false) {
        entries_arr.long_long(result.doc_id as i64);
    }
}

/// Reply with debug info for a node (recursive).
fn reply_node_debug(
    map: &mut MapBuilder<'_>,
    tree: &NumericRangeTree,
    node_idx: NodeIndex,
    minimal: bool,
) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();
    let node = tree.node(node_idx);

    if let Some(range) = node.range() {
        if minimal {
            map.kv_empty_array(c"range");
        } else {
            let mut range_arr = map.kv_array(c"range");
            let range_stats = reply_range_debug(&mut range_arr, range);
            stats.total_efficiency += range_stats.total_efficiency;
        }
    }

    if let NumericRangeNode::Internal(internal) = node {
        map.kv_double(c"value", internal.value);
        map.kv_long_long(c"maxDepth", internal.max_depth as i64);

        {
            let mut left_map = map.kv_map(c"left");
            let left_stats = reply_node_debug(&mut left_map, tree, internal.left_index(), minimal);
            stats.total_efficiency += left_stats.total_efficiency;
        }

        {
            let mut right_map = map.kv_map(c"right");
            let right_stats =
                reply_node_debug(&mut right_map, tree, internal.right_index(), minimal);
            stats.total_efficiency += right_stats.total_efficiency;
        }
    }

    stats
}

/// Reply with debug info for a range.
fn reply_range_debug(arr: &mut ArrayBuilder<'_>, range: &NumericRange) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();

    arr.simple_string(c"minVal");
    arr.double(range.min_val());
    arr.simple_string(c"maxVal");
    arr.double(range.max_val());
    arr.simple_string(c"invertedIndexSize [bytes]");
    arr.double(range.memory_usage() as f64);
    arr.simple_string(c"card");
    arr.long_long(range.cardinality() as i64);

    arr.simple_string(c"entries");
    let inv_idx_stats = {
        let mut entries_arr = arr.array();
        reply_numeric_index_debug(&mut entries_arr, range.entries())
    };
    stats.total_efficiency += inv_idx_stats.total_efficiency;

    stats
}

/// Reply with debug info for a numeric index.
fn reply_numeric_index_debug(
    arr: &mut ArrayBuilder<'_>,
    index: &NumericIndex,
) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();
    let summary = index.summary();

    stats.total_efficiency = summary.block_efficiency;

    arr.simple_string(c"numDocs");
    arr.long_long(summary.number_of_docs as i64);
    arr.simple_string(c"numEntries");
    arr.long_long(summary.number_of_entries as i64);
    arr.simple_string(c"lastId");
    arr.long_long(summary.last_doc_id as i64);
    arr.simple_string(c"size");
    arr.long_long(summary.number_of_blocks as i64);
    arr.simple_string(c"blocks_efficiency (numEntries/size)");
    arr.double(summary.block_efficiency);

    arr.simple_string(c"values");

    {
        let mut values_arr = arr.array();
        let mut reader = index.reader();
        let mut result = RSIndexResult::numeric(0.0);
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: We know the result contains numeric data
            let value = unsafe { result.as_numeric_unchecked() };
            values_arr.simple_string(c"value");
            values_arr.double(value);
            values_arr.simple_string(c"docId");
            values_arr.long_long(result.doc_id as i64);
        }
    }

    stats
}
