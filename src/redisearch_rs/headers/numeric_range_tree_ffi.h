#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redisearch.h"
#include "redismodule.h"
#include "numeric_range_tree.h"

/**
 * Status of a [`NumericRangeTree_ApplyGcEntry`] call.
 */
typedef enum ApplyGcEntryStatus {
  Ok,
  NodeNotFound,
  DeserializationError,
} ApplyGcEntryStatus;

/**
 * A single node's GC scan result, returned by [`NumericGcScanner_Next`].
 *
 * The `data` pointer points into the scanner's internal buffer and is valid
 * until the next call to [`NumericGcScanner_Next`] or [`NumericGcScanner_Free`].
 */
typedef struct NumericGcNodeEntry NumericGcNodeEntry;

/**
 * Type alias for the tree iterator, providing a C-friendly name.
 *
 * The iterator holds references to nodes in the tree. The tree must not be
 * freed or mutated while this iterator exists.
 */
typedef struct NumericRangeTreeIterator NumericRangeTreeIterator;

/**
 * Opaque streaming scanner that yields one node's GC delta at a time.
 *
 * Created by [`NumericGcScanner_New`], advanced by [`NumericGcScanner_Next`],
 * and freed by [`NumericGcScanner_Free`].
 *
 * Each call to `Next` scans the next node in DFS order via
 * [`NumericRangeNode::scan_gc`][numeric_range_tree::NumericRangeNode::scan_gc]
 * and serializes the delta + HLL registers into an internal buffer.
 * The caller can then write the entry data to the pipe immediately,
 * avoiding buffering all deltas in memory.
 */
typedef struct NumericGcScanner NumericGcScanner;

/**
 * Result of [`NumericRangeTree_Find`] - an array of range pointers.
 *
 * The caller is responsible for freeing this result using
 * [`NumericRangeTreeFindResult_Free`]. The ranges themselves are owned by
 * the tree and must not be freed individually.
 */
typedef struct NumericRangeTreeFindResult {
  const const struct NumericRange * *ranges;
  uintptr_t len;
} NumericRangeTreeFindResult;

/**
 * Result of [`NumericRangeTree_ApplyGcEntry`].
 *
 * Wraps [`SingleNodeGcResult`] with a [`status`](ApplyGcEntryStatus) field
 * so C callers can distinguish success, node-not-found, and deserialization
 * errors.
 */
typedef struct ApplyGcEntryResult {
  struct SingleNodeGcResult gc_result;
  enum ApplyGcEntryStatus status;
} ApplyGcEntryResult;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Get the revision ID of the tree.
 *
 * The revision ID changes whenever the tree structure is modified (nodes split, etc.).
 * This is used by iterators to detect concurrent modifications.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 */
uint32_t NumericRangeTree_GetRevisionId(const struct NumericRangeTree *t);

/**
 * Create a new iterator over all nodes in the tree.
 *
 * The iterator performs a depth-first traversal, visiting each node exactly once.
 * Use [`NumericRangeTreeIterator_Next`] to advance the iterator.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`crate::NewNumericRangeTree`] and cannot be NULL.
 * - `t` must not be freed while the iterator lives.
 * - The tree must not be mutated while the iterator lives.
 */
struct NumericRangeTreeIterator *NumericRangeTreeIterator_New(const struct NumericRangeTree *t);

/**
 * Get the [`NumericRange`] from a node, if present.
 *
 * Returns a pointer to the range, or NULL if the node has no range
 * (e.g., an internal node whose range has been trimmed).
 *
 * The returned pointer is valid until the tree is modified or freed.
 * Do NOT free the returned pointer - it points to memory owned by the tree.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `node` must point to a valid [`NumericRangeNode`] obtained from
 *   [`crate::iterator::NumericRangeTreeIterator_Next`] and cannot be NULL.
 * - The tree from which this node came must still be valid.
 */
const struct NumericRange *NumericRangeNode_GetRange(const struct NumericRangeNode *node);

/**
 * Reply with a summary of the numeric range tree (for NUMIDX_SUMMARY).
 *
 * This outputs the tree statistics in the format expected by FT.DEBUG NUMIDX_SUMMARY.
 * When `t` is NULL (index not yet created), all values are reported as zero.
 *
 * # Safety
 *
 * - `ctx` must be a valid Redis module context.
 * - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
 */
void NumericRangeTree_DebugSummary(RedisModuleCtx *ctx, const struct NumericRangeTree *t);

/**
 * Conditionally trim empty leaves and compact the node slab.
 *
 * Checks if the number of empty leaves exceeds half the total number of
 * leaves. If so, trims empty leaves, compacts the slab to reclaim freed
 * slots, and returns the number of bytes freed. Returns 0 if no trimming
 * was needed.
 *
 * # Safety
 *
 * - `t` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
 * - No iterators should be active on this tree while calling this function.
 */
struct CompactIfSparseResult NumericRangeTree_CompactIfSparse(struct NumericRangeTree *t);

/**
 * Get the estimated cardinality (number of distinct values) for a range.
 *
 * This uses HyperLogLog estimation and may have some error margin.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `range` must point to a valid [`NumericRange`] obtained from
 *   [`crate::node::NumericRangeNode_GetRange`] and cannot be NULL.
 * - The tree from which this range came must still be valid.
 */
uintptr_t NumericRange_GetCardinality(const struct NumericRange *range);

/**
 * Get the minimum value in a range.
 *
 * # Safety
 *
 * - `range` must point to a valid [`NumericRange`] and cannot be NULL.
 */
double NumericRange_MinVal(const struct NumericRange *range);

/**
 * Increment the revision ID.
 *
 * This method is never needed in production code: the tree
 * revision ID is automatically incremented when the tree structure changes.
 *
 * This method is provided primarily for testing purposes—e.g. to force the invalidation
 * of an iterator built on top of this tree in GC tests.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 * - The caller must have unique access to `t`.
 */
uint32_t NumericRangeTree_IncrementRevisionId(struct NumericRangeTree *t);

/**
 * Reply with a dump of the numeric index entries (for DUMP_NUMIDX).
 *
 * This outputs all entries from all ranges in the tree. If `with_headers` is true,
 * each range's entries are prefixed with header information (numDocs, numEntries, etc).
 * When `t` is NULL (index not yet created), an empty array is returned.
 *
 * # Safety
 *
 * - `ctx` must be a valid Redis module context.
 * - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
 */
void NumericRangeTree_DebugDumpIndex(RedisModuleCtx *ctx, const struct NumericRangeTree *t, bool with_headers);

/**
 * Advance the iterator and return the next node.
 *
 * Returns a pointer to the next [`NumericRangeNode`] in the traversal,
 * or NULL if the iteration is complete.
 *
 * The returned pointer is valid until the tree is modified or freed.
 * Do NOT free the returned pointer - it points to memory owned by the tree.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid [`NumericRangeTreeIterator`] obtained from
 *   [`NumericRangeTreeIterator_New`] and cannot be NULL.
 * - The tree from which this iterator was created must still be valid.
 */
const struct NumericRangeNode *NumericRangeTreeIterator_Next(struct NumericRangeTreeIterator *it);

/**
 * Get the maximum value in a range.
 *
 * # Safety
 *
 * - `range` must point to a valid [`NumericRange`] and cannot be NULL.
 */
double NumericRange_MaxVal(const struct NumericRange *range);

/**
 * Get the unique ID of the tree.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 */
uint32_t NumericRangeTree_GetUniqueId(const struct NumericRangeTree *t);

/**
 * Get the inverted index size in bytes.
 *
 * # Safety
 *
 * - `range` must point to a valid [`NumericRange`] and cannot be NULL.
 */
uintptr_t NumericRange_InvertedIndexSize(const struct NumericRange *range);

/**
 * Get the number of entries in the tree.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 */
uintptr_t NumericRangeTree_GetNumEntries(const struct NumericRangeTree *t);

/**
 * Reply with a dump of the numeric index tree structure (for DUMP_NUMIDXTREE).
 *
 * This outputs the tree structure as a nested map. If `minimal` is true,
 * range entry details are omitted (only tree structure is shown).
 * When `t` is NULL (index not yet created), all values are zero with an empty root.
 *
 * # Safety
 *
 * - `ctx` must be a valid Redis module context.
 * - `t` must be either NULL or a valid pointer to a [`NumericRangeTree`].
 */
void NumericRangeTree_DebugDumpTree(RedisModuleCtx *ctx, const struct NumericRangeTree *t, bool minimal);

/**
 * Free a [`NumericRangeTreeIterator`].
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `it` must point to a valid [`NumericRangeTreeIterator`] obtained from
 *   [`NumericRangeTreeIterator_New`], or be NULL (in which case this is a no-op).
 * - After calling this function, `it` must not be used again.
 */
void NumericRangeTreeIterator_Free(struct NumericRangeTreeIterator *it);

/**
 * Get the number of ranges in the tree.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 */
uintptr_t NumericRangeTree_GetNumRanges(const struct NumericRangeTree *t);

/**
 * Get the inverted index entries from a range.
 *
 * Returns a pointer to the [`InvertedIndexNumeric`] (which is a `NumericIndex` enum)
 * stored inside the range. The returned pointer is valid until the tree is modified or freed.
 *
 * # Safety
 *
 * - `range` must point to a valid [`NumericRange`] and cannot be NULL.
 * - The returned pointer points to memory owned by the range; do not free it.
 */
const struct InvertedIndexNumeric *NumericRange_GetEntries(const struct NumericRange *range);

/**
 * Create a new [`NumericRangeTree`].
 *
 * Returns an opaque pointer to the newly created tree.
 * To free the tree, use [`NumericRangeTree_Free`].
 *
 * If `compress_floats` is true, the tree will use float compression which
 * attempts to store f64 values as f32 when precision loss is acceptable (< 0.01).
 * This corresponds to the `RSGlobalConfig.numericCompress` setting.
 */
struct NumericRangeTree *NewNumericRangeTree(bool compress_floats);

/**
 * Create a new [`NumericGcScanner`] for streaming GC scans.
 *
 * The scanner traverses the tree in pre-order DFS, scanning one node at a
 * time. Call [`NumericGcScanner_Next`] to advance.
 *
 * # Safety
 *
 * - `sctx` must point to a valid [`RedisSearchCtx`] and cannot be NULL.
 * - `tree` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 * - Both `sctx` and `tree` must remain valid for the lifetime of the scanner.
 */
struct NumericGcScanner *NumericGcScanner_New(RedisSearchCtx *sctx, struct NumericRangeTree *tree);

/**
 * Get the total size of inverted indexes in the tree.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 */
uintptr_t NumericRangeTree_GetInvertedIndexesSize(const struct NumericRangeTree *t);

/**
 * Add a (docId, value) pair to the tree.
 *
 * If `isMulti` is non-zero, duplicate document IDs are allowed.
 * `maxDepthRange` specifies the maximum depth at which to retain ranges on inner nodes.
 *
 * Returns information about what changed during the add operation.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`NewNumericRangeTree`] and cannot be NULL.
 */
struct AddResult _NumericRangeTree_Add(struct NumericRangeTree *t, t_docId doc_id, double value, int isMulti, uintptr_t maxDepthRange);

/**
 * Get the root node of the tree.
 *
 * # Safety
 *
 * - `t` must point to a valid [`NumericRangeTree`] and cannot be NULL.
 * - The returned pointer is valid until the tree is modified or freed.
 */
const struct NumericRangeNode *NumericRangeTree_GetRoot(const struct NumericRangeTree *t);

/**
 * Create an [`IndexReader`] for iterating over a [`NumericRange`]'s entries.
 *
 * This is the primary way to iterate over numeric index entries from C code.
 * The returned reader can be used with `IndexReader_Next()`, `IndexReader_Seek()`, etc.
 * from `inverted_index_ffi`.
 *
 * If `filter` is NULL, all entries are returned. Otherwise, entries are filtered
 * according to the numeric filter (or geo filter if the filter's `geo_filter` is set).
 *
 * # Safety
 *
 * - `range` must point to a valid [`NumericRange`] and cannot be NULL.
 * - `filter` may be NULL for no filtering, or must point to a valid [`NumericFilter`].
 * - The returned reader holds a reference to the range's inverted index. The range
 *   must not be freed or modified while the reader exists.
 * - The filter (if non-NULL) must remain valid for the lifetime of the reader.
 * - Free the returned reader with `IndexReader_Free()` when done.
 */
struct IndexReader *NumericRange_NewIndexReader(const struct NumericRange *range, const struct NumericFilter *filter);

/**
 * Free a [`NumericRangeTree`] and all its contents.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`NewNumericRangeTree`], or be NULL (in which case this is a no-op).
 * - After calling this function, `t` must not be used again.
 * - Any iterators obtained from this tree must be freed before calling this.
 */
void NumericRangeTree_Free(struct NumericRangeTree *t);

/**
 * Advance the scanner to the next node with GC work.
 *
 * Scans nodes in DFS order, skipping those without GC work. When a node
 * with work is found, its delta and HLL registers are serialized into the
 * scanner's internal buffer.
 *
 * Returns `true` if an entry was produced (and `*entry` is populated),
 * `false` when all nodes have been visited.
 *
 * The `entry.data` pointer is valid until the next call to `Next` or `Free`.
 *
 * # Wire format for `entry.data`
 *
 * ```text
 * [delta_msgpack][64-byte hll_with][64-byte hll_without]
 * ```
 *
 * # Safety
 *
 * - `scanner` must be a valid pointer returned by [`NumericGcScanner_New`].
 * - `entry` must be a valid pointer to a [`NumericGcNodeEntry`].
 */
bool NumericGcScanner_Next(struct NumericGcScanner *scanner, struct NumericGcNodeEntry *entry);

/**
 * Get the total memory usage of the tree in bytes.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`NewNumericRangeTree`] and cannot be NULL.
 */
uintptr_t NumericRangeTree_MemUsage(const struct NumericRangeTree *t);

/**
 * Find all numeric ranges that match the given filter.
 *
 * Returns a [`NumericRangeTreeFindResult`] containing pointers to the matching
 * ranges. The ranges are owned by the tree and must not be freed individually.
 * The result itself must be freed using [`NumericRangeTreeFindResult_Free`].
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`NewNumericRangeTree`] and cannot be NULL.
 * - `nf` must point to a valid [`NumericFilter`] and cannot be NULL.
 */
struct NumericRangeTreeFindResult NumericRangeTree_Find(const struct NumericRangeTree *t, const struct NumericFilter *nf);

/**
 * Free a [`NumericGcScanner`].
 *
 * # Safety
 *
 * - `scanner` must be a valid pointer returned by [`NumericGcScanner_New`],
 *   or NULL (in which case this is a no-op).
 */
void NumericGcScanner_Free(struct NumericGcScanner *scanner);

/**
 * Free a [`NumericRangeTreeFindResult`].
 *
 * This frees the array allocation but NOT the ranges themselves (they are
 * owned by the tree).
 *
 * # Safety
 *
 * - `result` must have been obtained from [`NumericRangeTree_Find`].
 * - After calling this function, the result must not be used again.
 */
void NumericRangeTreeFindResult_Free(struct NumericRangeTreeFindResult result);

/**
 * Trim empty leaves from the tree (garbage collection).
 *
 * Removes leaf nodes that have no documents and prunes the tree structure
 * accordingly.
 *
 * # Safety
 *
 * The following invariants must be upheld when calling this function:
 * - `t` must point to a valid [`NumericRangeTree`] obtained from
 *   [`NewNumericRangeTree`] and cannot be NULL.
 * - No iterators should be active on this tree while calling this function.
 */
struct TrimEmptyLeavesResult NumericRangeTree_TrimEmptyLeaves(struct NumericRangeTree *t);

/**
 * Get the base size of a NumericRangeTree struct (not including contents).
 *
 * This is used for memory overhead calculations.
 */
uintptr_t NumericRangeTree_BaseSize(void);

/**
 * Parse a serialized GC entry and apply it to the specified node.
 *
 * The entry data must have the wire format produced by [`NumericGcScanner_Next`]:
 * ```text
 * [delta_msgpack][64-byte hll_with][64-byte hll_without]
 * ```
 *
 * Returns an [`ApplyGcEntryResult`] whose [`status`](ApplyGcEntryStatus)
 * indicates success, node-not-found, or deserialization error.
 *
 * # Safety
 *
 * - `tree` must point to a valid mutable [`NumericRangeTree`] and cannot be NULL.
 * - `entry_data` must point to a valid byte buffer of at least `entry_len` bytes.
 */
struct ApplyGcEntryResult NumericRangeTree_ApplyGcEntry(struct NumericRangeTree *tree, uint32_t node_position, uint32_t node_generation, const uint8_t *entry_data, uintptr_t entry_len);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
