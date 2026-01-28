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
use redis_reply::{ArrayBuilder, FixedArrayBuilder, MapBuilder, RedisModuleCtx, Replier};

use crate::{NumericIndex, NumericRange, NumericRangeNode, NumericRangeReader, NumericRangeTree};

/// Block efficiency statistics for computing average memory efficiency.
#[derive(Debug, Default)]
struct BlocksEfficiencyStats {
    /// Sum of (numEntries / numBlocks) across all ranges.
    total_efficiency: f64,
}

/// Reply with a summary of the numeric range tree.
///
/// This implements the FT.DEBUG NUMIDX_SUMMARY command response format.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_summary(ctx: *mut RedisModuleCtx, tree: &NumericRangeTree) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut arr = replier.array();

    arr.kv_long_long(c"numRanges", tree.num_ranges() as i64);
    arr.kv_long_long(c"numLeaves", tree.num_leaves() as i64);
    arr.kv_long_long(c"numEntries", tree.num_entries() as i64);
    arr.kv_long_long(c"lastDocId", tree.last_doc_id() as i64);
    arr.kv_long_long(c"revisionId", tree.revision_id() as i64);
    arr.kv_long_long(c"emptyLeaves", tree.empty_leaves() as i64);
    arr.kv_long_long(c"RootMaxDepth", tree.root().max_depth() as i64);
    arr.kv_long_long(c"MemoryUsage", tree.mem_usage() as i64);
}

/// Reply with a dump of the numeric index entries.
///
/// This implements the FT.DEBUG DUMP_NUMIDX command response format.
/// Each range in the tree is dumped as an array of document IDs.
/// If `with_headers` is true, each range's entries are prefixed with header info.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_dump_index(
    ctx: *mut RedisModuleCtx,
    tree: &NumericRangeTree,
    with_headers: bool,
) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut arr = replier.array();

    for node in tree.iter() {
        if let Some(range) = node.range() {
            if with_headers {
                let mut fixed_arr = arr.nested_fixed_array(2);
                reply_inverted_index_header(&mut fixed_arr, range.entries());
                reply_range_entries(&mut fixed_arr.nested_array(), range);
            } else {
                reply_range_entries(&mut arr.nested_array(), range);
            }
        }
    }
}

/// Reply with a dump of the numeric index tree structure.
///
/// This implements the FT.DEBUG DUMP_NUMIDXTREE command response format.
/// The tree structure is dumped as a nested map.
/// If `minimal` is true, range entries are omitted.
///
/// # Safety
///
/// - `ctx` must be a valid Redis module context.
pub unsafe fn debug_dump_tree(ctx: *mut RedisModuleCtx, tree: &NumericRangeTree, minimal: bool) {
    // SAFETY: ctx is valid per function docs
    let mut replier = unsafe { Replier::new(ctx) };
    let mut map = replier.map();

    map.kv_long_long(c"numRanges", tree.num_ranges() as i64);
    map.kv_long_long(c"numEntries", tree.num_entries() as i64);
    map.kv_long_long(c"lastDocId", tree.last_doc_id() as i64);
    map.kv_long_long(c"revisionId", tree.revision_id() as i64);
    map.kv_long_long(c"uniqueId", tree.unique_id() as i64);
    map.kv_long_long(c"emptyLeaves", tree.empty_leaves() as i64);

    let stats = {
        let mut root_map = map.kv_map(c"root");
        reply_node_debug(&mut root_map, tree.root(), minimal)
    };

    {
        let mut stats_map = map.kv_fixed_map(c"Tree stats", 1);
        let num_ranges = tree.num_ranges();
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

/// Reply with inverted index header information (adds 1 element to parent).
fn reply_inverted_index_header(parent: &mut FixedArrayBuilder<'_>, entries: &NumericIndex) {
    let summary = match entries {
        NumericIndex::Uncompressed(idx) => idx.summary(),
        NumericIndex::Compressed(idx) => idx.summary(),
    };

    let mut header_arr = parent.nested_array();

    header_arr.kv_long_long(c"numDocs", summary.number_of_docs as i64);
    header_arr.kv_long_long(c"numEntries", summary.number_of_entries as i64);
    header_arr.kv_long_long(c"lastId", summary.last_doc_id as i64);
    header_arr.kv_long_long(c"flags", summary.flags as i64);
    header_arr.kv_long_long(c"numberOfBlocks", summary.number_of_blocks as i64);

    if summary.has_efficiency {
        header_arr.kv_double(
            c"blocks_efficiency (numEntries/numberOfBlocks)",
            summary.block_efficiency,
        );
    }
}

/// Reply with the entries (doc_ids) from a range.
fn reply_range_entries(entries_arr: &mut ArrayBuilder<'_>, range: &NumericRange) {
    let mut last_doc_id: u64 = 0;

    let reader = range.reader();
    let mut result = RSIndexResult::numeric(0.0);

    match reader {
        NumericRangeReader::Uncompressed(mut r) => {
            while r.next_record(&mut result).unwrap_or(false) {
                if result.doc_id != last_doc_id {
                    entries_arr.long_long(result.doc_id as i64);
                    last_doc_id = result.doc_id;
                }
            }
        }
        NumericRangeReader::Compressed(mut r) => {
            while r.next_record(&mut result).unwrap_or(false) {
                if result.doc_id != last_doc_id {
                    entries_arr.long_long(result.doc_id as i64);
                    last_doc_id = result.doc_id;
                }
            }
        }
    }
}

/// Reply with debug info for a node (recursive).
fn reply_node_debug(
    map: &mut MapBuilder<'_>,
    node: &NumericRangeNode,
    minimal: bool,
) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();

    if let Some(range) = node.range() {
        if minimal {
            map.kv_empty_array(c"range");
        } else {
            let mut range_arr = map.kv_array(c"range");
            let range_stats = reply_range_debug(&mut range_arr, range);
            stats.total_efficiency += range_stats.total_efficiency;
        }
    }

    if !node.is_leaf() {
        map.kv_double(c"value", node.split_value());
        map.kv_long_long(c"maxDepth", node.max_depth() as i64);

        if let Some(left) = node.left() {
            let mut left_map = map.kv_map(c"left");
            let left_stats = reply_node_debug(&mut left_map, left, minimal);
            stats.total_efficiency += left_stats.total_efficiency;
        } else {
            map.kv_empty_map(c"left");
        }

        if let Some(right) = node.right() {
            let mut right_map = map.kv_map(c"right");
            let right_stats = reply_node_debug(&mut right_map, right, minimal);
            stats.total_efficiency += right_stats.total_efficiency;
        } else {
            map.kv_empty_map(c"right");
        }
    }

    stats
}

/// Reply with debug info for a range.
fn reply_range_debug(arr: &mut ArrayBuilder<'_>, range: &NumericRange) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();

    arr.kv_double(c"minVal", range.min_val());
    arr.kv_double(c"maxVal", range.max_val());
    arr.kv_double(c"invertedIndexSize [bytes]", range.memory_usage() as f64);
    arr.kv_long_long(c"card", range.cardinality() as i64);

    arr.simple_string(c"entries");
    let inv_idx_stats = {
        let mut entries_arr = arr.nested_array();
        reply_inverted_index_debug(&mut entries_arr, range)
    };
    stats.total_efficiency += inv_idx_stats.total_efficiency;

    stats
}

/// Reply with debug info for an inverted index.
fn reply_inverted_index_debug(
    arr: &mut ArrayBuilder<'_>,
    range: &NumericRange,
) -> BlocksEfficiencyStats {
    let mut stats = BlocksEfficiencyStats::default();
    let entries = range.entries();

    let summary = match entries {
        NumericIndex::Uncompressed(idx) => idx.summary(),
        NumericIndex::Compressed(idx) => idx.summary(),
    };

    stats.total_efficiency = summary.block_efficiency;

    arr.kv_long_long(c"numDocs", summary.number_of_docs as i64);
    arr.kv_long_long(c"numEntries", summary.number_of_entries as i64);
    arr.kv_long_long(c"lastId", summary.last_doc_id as i64);
    arr.kv_long_long(c"size", summary.number_of_blocks as i64);
    arr.kv_double(
        c"blocks_efficiency (numEntries/size)",
        summary.block_efficiency,
    );

    arr.simple_string(c"values");

    {
        let mut values_arr = arr.nested_array();
        let reader = range.reader();
        let mut result = RSIndexResult::numeric(0.0);

        match reader {
            NumericRangeReader::Uncompressed(mut r) => {
                while r.next_record(&mut result).unwrap_or(false) {
                    // SAFETY: We know the result contains numeric data
                    let value = unsafe { result.as_numeric_unchecked() };
                    values_arr.kv_double(c"value", value);
                    values_arr.kv_long_long(c"docId", result.doc_id as i64);
                }
            }
            NumericRangeReader::Compressed(mut r) => {
                while r.next_record(&mut result).unwrap_or(false) {
                    // SAFETY: We know the result contains numeric data
                    let value = unsafe { result.as_numeric_unchecked() };
                    values_arr.kv_double(c"value", value);
                    values_arr.kv_long_long(c"docId", result.doc_id as i64);
                }
            }
        }
    }

    stats
}
