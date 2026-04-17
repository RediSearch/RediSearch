#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "inverted_index.h"

/**
 * Enum to hold either compressed or uncompressed numeric index.
 */
typedef struct InvertedIndexNumeric InvertedIndexNumeric;

/**
 * A numeric range is a leaf-level storage unit in the numeric range tree.
 *
 * It stores document IDs and their associated numeric values in an inverted index,
 * along with metadata for range queries and cardinality estimation.
 *
 * # Structure
 *
 * - **Bounds** (`min_val`, `max_val`): Track the actual value range for overlap
 *   and containment tests during queries.
 * - **Cardinality** (`hll`): HyperLogLog estimator for the number of distinct
 *   values, used to decide when to split.
 * - **Entries** (`entries`): Inverted index storing (docId, value) pairs.
 *
 * # Initialization
 *
 * New ranges start with inverted bounds (`min_val = +∞`, `max_val = -∞`) so
 * the first added value correctly sets both bounds.
 */
typedef struct NumericRange NumericRange;

/**
 * A numeric range tree for efficient range queries over numeric values.
 *
 * The tree organizes documents by their numeric field values into a balanced
 * binary tree of ranges. Each leaf node contains a range of values, and
 * internal nodes may optionally retain their ranges for query efficiency.
 *
 * # Arena Storage
 *
 * All nodes are stored in a [`NodeArena`]. Children are referenced by
 * [`NodeIndex`] instead of `Box<NumericRangeNode>`. This provides better
 * cache locality, eliminates per-node heap allocation overhead, and makes
 * pruning cheaper (index swaps and a single `realloc` rather than a dealloc
 * for every deleted node).
 *
 * # Splitting Strategy
 *
 * Nodes split based on two conditions:
 *
 * - **Cardinality threshold**: When the HyperLogLog-estimated cardinality exceeds
 *   a depth-dependent limit. The threshold is [`Self::MINIMUM_RANGE_CARDINALITY`] at depth 0,
 *   growing by a factor of [`Self::CARDINALITY_GROWTH_FACTOR`] per depth level until capped
 *   at [`Self::MAXIMUM_RANGE_CARDINALITY`].
 *
 * - **Size overflow**: When entry count exceeds [`Self::MAXIMUM_RANGE_SIZE`] and
 *   cardinality > 1. This handles cases where many documents share few values.
 *   The cardinality check prevents splitting single-value ranges indefinitely.
 *
 * # Balancing
 *
 * The tree uses AVL-like single rotations when depth imbalance exceeds
 * [`Self::MAXIMUM_DEPTH_IMBALANCE`].
 */
typedef struct NumericRangeTree NumericRangeTree;
/**
 * Minimum cardinality before considering splitting (at depth 0).
 *
 * At depth 0, we require at least this many distinct values before splitting.
 * This prevents excessive splitting for low-cardinality fields.
 */
#define NumericRangeTree_MINIMUM_RANGE_CARDINALITY 16
/**
 * Maximum cardinality threshold for splitting.
 *
 * Once the split threshold reaches this value, it stays constant regardless
 * of depth. This caps the maximum number of distinct values in any leaf range.
 */
#define NumericRangeTree_MAXIMUM_RANGE_CARDINALITY 2500
/**
 * Maximum number of entries in a range before forcing a split (if cardinality > 1).
 *
 * Even if cardinality is below the threshold, we split if a range accumulates
 * too many entries. This handles cases where many documents share few values.
 * The cardinality > 1 check prevents splitting single-value ranges indefinitely.
 */
#define NumericRangeTree_MAXIMUM_RANGE_SIZE 10000
/**
 * Maximum depth imbalance before rebalancing.
 *
 * We use AVL-like rotations when one subtree's depth exceeds the other by
 * more than this value.
 */
#define NumericRangeTree_MAXIMUM_DEPTH_IMBALANCE 2
/**
 * Cardinality growth factor per depth level.
 *
 * The split cardinality threshold multiplies by this factor for each depth
 * level, capped at [`Self::MAXIMUM_RANGE_CARDINALITY`].
 */
#define NumericRangeTree_CARDINALITY_GROWTH_FACTOR 4

/**
 * A node in the numeric range tree.
 *
 * Nodes are either:
 * - **Leaf nodes**: Have a range but no children.
 * - **Internal nodes**: Have both children, a split value, depth tracking,
 *   and optionally retain a range for query efficiency.
 *
 * When part of a [`NumericRangeTree`](crate::NumericRangeTree), nodes are
 * stored in a [`generational_slab::Slab`] arena and referenced by [`NodeIndex`].
 */
typedef struct NumericRangeNode NumericRangeNode;

/**
 * Result of adding a value to the tree.
 *
 * This captures the changes that occurred during the add operation,
 * including memory growth and structural changes. The delta fields use
 * signed types to support both growth (positive) and shrinkage (negative)
 * during operations like trimming empty leaves.
 */
typedef struct AddResult {
  int64_t size_delta;
  int32_t num_records_delta;
  bool changed;
  int32_t num_ranges_delta;
  int32_t num_leaves_delta;
} AddResult;

/**
 * Result of applying GC to a single node.
 *
 * Returned by [`NumericRangeTree::apply_gc_to_node`].
 */
typedef struct SingleNodeGcResult {
  struct II_GCScanStats index_gc_info;
  bool became_empty;
} SingleNodeGcResult;

/**
 * Returned by [`NumericRangeTree::compact_if_sparse`].
 */
typedef struct CompactIfSparseResult {
  int64_t inverted_index_size_delta;
  int64_t node_size_delta;
} CompactIfSparseResult;

/**
 * Result of trimming empty leaves from the tree.
 *
 * Similar to [`AddResult`] but without `num_records_delta`, since trimming
 * only removes empty nodes and does not change the number of entries
 * (entries are removed by GC before trimming).
 */
typedef struct TrimEmptyLeavesResult {
  int64_t size_delta;
  bool changed;
  int32_t num_ranges_delta;
  int32_t num_leaves_delta;
} TrimEmptyLeavesResult;
