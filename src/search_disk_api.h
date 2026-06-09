/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include "redisearch.h"
#include "VecSim/vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations to avoid circular dependencies
typedef struct QueryIterator QueryIterator;
typedef struct NumericFilter NumericFilter;
typedef struct QueryError QueryError;

// Forward declaration for HiddenString
typedef struct HiddenString HiddenString;

// Helper opaque types for the disk API
typedef const void* RedisSearchDisk;
typedef const void* RedisSearchDiskIndexSpec;
typedef const void* RedisSearchDiskInvertedIndex;
typedef const void* RedisSearchDiskIterator;
typedef void* RedisSearchDiskAsyncReadPool;
// Opaque handle to a temporary RDB state object.
//
// Per-field vector in-memory state is NOT carried here — it rides inline
// with each field's own RDB encoding (FieldSpec_RdbSave / FieldSpec_RdbLoad)
// and is deserialized directly into an unbound VecSimIndex written to
// fs->vectorOpts.vecSimIndex; storage is bound to that handle in place at
// LOADING_ENDED.
typedef const void* RedisSearchDiskRdbState;

// Opaque handle for the underlying storage-layer write batch.
//
// Allocated and freed by the storage implementation behind this API; the only
// C-visible operations are the create / commit / abort / stage entry points on
// `IndexDiskAPI` and `DocTableDiskAPI`. C-side callers pass the handle through
// the thin wrappers in `search_disk.h` (`SearchDisk_CreateWriteBatch`,
// `SearchDisk_CommitWriteBatch`, `SearchDisk_AbortWriteBatch`, etc.).
typedef struct SearchDiskWriteBatchHandle SearchDiskWriteBatchHandle;

// Callback function to allocate memory for the key in the scope of the search module memory
typedef char* (*AllocateKeyCallback)(const void*, size_t len);

// Callback function to allocate a new RSDocumentMetadata with ref_count=1 and keyPtr set
typedef RSDocumentMetadata* (*AllocateDMDCallback)(const void* key_data, size_t key_len);

// Callback functions for applying text compaction delta updates.
// The C side owns private_data/update_ctx semantics; Rust treats them as opaque.
//
// Lifecycle, per compaction, in calling order:
//
//   compactionStarted(private_data)            // once at compaction start
//   beginUpdate(private_data) -> update_ctx    // once before delta apply
//   decrementTrieTermCount(update_ctx, ...)    // 0..N times
//   decrementNumTerms(update_ctx, ...)         // 0 or 1 time
//   endUpdate(update_ctx)                      // once after delta apply
//   compactionCompleted(private_data)          // once at compaction end
//
// `compactionStarted` / `compactionCompleted` bracket the whole compaction
// and are intended for coarse-grained protection that must hold for its full
// duration (e.g. blocking the snapshot fork). `beginUpdate` / `endUpdate`
// bracket just the delta-apply window and are intended for fine-grained
// protection (e.g. the IndexSpec wrlock around the trie/numTerms updates).
typedef struct SearchDiskCompactionCallbacks {
  // Called once at the start of a parent compaction, before any
  // beginUpdate/endUpdate pair. Implementations may take long-lived locks
  // here; the matching release goes in `compactionCompleted`.
  void (*compactionStarted)(void *private_data);

  // Opens an update session and returns opaque update context.
  // Implementations may acquire internal locks here.
  void *(*beginUpdate)(void *private_data);

  // Decrement term doc count in the serving trie.
  bool (*decrementTrieTermCount)(
      void *update_ctx,
      const char *term,
      size_t term_len,
      size_t doc_count_decrement);

  // Decrement numTerms in scoring stats.
  void (*decrementNumTerms)(void *update_ctx, uint64_t num_terms_removed);

  // Closes an update session.
  // Implementations may release internal locks here.
  void (*endUpdate)(void *update_ctx);

  // Called once at the end of a parent compaction, after every
  // beginUpdate/endUpdate pair has returned. Pairs with `compactionStarted`.
  void (*compactionCompleted)(void *private_data);
} SearchDiskCompactionCallbacks;

// Result of polling the async read pool
typedef struct AsyncPollResult {
  uint16_t ready_count;   // Number of successful reads in results buffer
  uint16_t failed_count;  // Number of failed reads in failed_user_data buffer
  uint16_t pending_count; // Number of reads still in flight
} AsyncPollResult;

// Result structure containing both DMD and user data (for successful reads only)
typedef struct AsyncReadResult {
  RSDocumentMetadata *dmd;  // Pointer to allocated DMD (caller must free with DMD_Return)
  uint64_t user_data;       // Generic user data passed to addAsyncRead (e.g., index, pointer, flags)
} AsyncReadResult;

typedef struct BasicDiskAPI {
  /**
   * @brief Open the disk storage context
   * @param ctx Redis module context
   * @param buffer_percentage Percentage of available memory to use for write buffer (0-100)
   * @param logObfuscation true to enable obfuscation, false to disable
   * @param dropReadCache When true, hints the OS to evict pages after reading
   * @param useDirectReads When true, opens files with O_DIRECT to bypass the OS page cache
   * @return Pointer to the disk context, or NULL on error
   */
  RedisSearchDisk *(*open)(RedisModuleCtx *ctx, int buffer_percentage, bool logObfuscation, bool dropReadCache, bool useDirectReads);
  void (*close)(RedisModuleCtx *ctx, RedisSearchDisk *disk);

  /**
   * @brief Enable or disable obfuscation of index names and field names in Disk log output
   * @param disk Pointer to the disk
   * @param enable true to enable obfuscation, false to disable
   */
  void (*setLogObfuscation)(RedisSearchDisk *disk, bool enable);

  /**
   * @brief Open an index spec
   * @param ctx Redis module context for BigModule APIs (required for getting DB path)
   * @param disk Pointer to the disk
   * @param indexName Name of the index
   * @param obfuscatedName Obfuscated name of the index (for logging)
   * @param obfuscatedNameLen Length of the obfuscated name
   * @param type Document type
   * @param deleteBeforeOpen If true, delete any existing data before opening
   * @param callbacks Callback table for applying compaction delta updates during GC.
   *                  Bound to the IndexSpec for its lifetime; must outlive the IndexSpec.
   * @param private_data Opaque pointer passed back into every callback. Bound to the
   *                     IndexSpec for its lifetime.
   * @return Pointer to the index spec, or NULL on error
   *
   * @note This opens the database but does NOT register it with Redis. Call registerIndex after this
   *       to register with BigModule APIs.
   */
  RedisSearchDiskIndexSpec *(*openIndexSpec)(RedisModuleCtx *ctx, RedisSearchDisk *disk, const HiddenString *indexName, const char *obfuscatedName, size_t obfuscatedNameLen, DocumentType type, bool deleteBeforeOpen, const SearchDiskCompactionCallbacks *callbacks, void *private_data);
  /**
   * @brief Close an index spec
   * @param disk Pointer to the disk context (for cleanup of index metrics)
   * @param index Pointer to the index spec
   *
   * @note This closes the database but does NOT unregister from Redis. Call unregisterIndex
   *       before this to unregister from BigModule APIs.
   */
  void (*closeIndexSpec)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);
  /**
   * @brief Register an index's database with Redis BigModule APIs
   * @param ctx Redis module context (required, must be valid)
   * @param index Pointer to the index spec
   *
   * @note Must be called from the main thread with a valid RedisModuleCtx.
   *       Call this after openIndexSpec to register the database with Redis.
   */
  void (*registerIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);
  /**
   * @brief Unregister an index's database from Redis BigModule APIs
   * @param ctx Redis module context (required, must be valid)
   * @param index Pointer to the index spec
   *
   * @note Must be called from the main thread with a valid RedisModuleCtx.
   *       Call this before closeIndexSpec to unregister the database from Redis.
   */
  void (*unregisterIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);
  /**
   * @brief Save the index spec's disk-related state to RDB.
   *
   * Writes the IndexSpec's partial RDB state. Per-field vector blobs are written inline with each
   * field's own RDB encoding by FieldSpec_RdbSave on the C side — they are
   * NOT part of this payload.
   *
   * @param rdb RedisModuleIO RDB save stream
   * @param index Pointer to the index spec
   */
  void (*indexSpecRdbSave)(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Check if async I/O is supported by the underlying storage engine
   * @param disk Pointer to the disk
   * @return true if async I/O operations are available, false otherwise
   */
  bool (*isAsyncIOSupported)(RedisSearchDisk *disk);

  /**
   * @brief Set throttle callbacks for vector disk tiered indexes to pause/resume CMD_DENYOOM commands.
   * @param enable Callback to pause CMD_DENYOOM commands (wraps RedisModule_EnablePostponeClients)
   * @param disable Callback to resume CMD_DENYOOM commands (wraps RedisModule_DisablePostponeClients)
   */
  void (*setThrottleCallbacks)(ThrottleCB enable, ThrottleCB disable);

  /**
   * @brief Load the spec's disk-related RDB data into a temporary in-memory object.
   *
   * Called during RDB load when the IndexSpec cannot be created yet (e.g., during replication
   * before SST files arrive). The returned state is consumed by ownership in
   * openIndexSpecWithRdbState, or freed on abort paths with freeRdbState.
   *
   * Per-field vector in-memory state is NOT carried here — it rides inline
   * with each field's own RDB encoding (FieldSpec_RdbLoad) and is
   * deserialized directly into an unbound VecSimIndex written to
   * fs->vectorOpts.vecSimIndex.
   *
   * @param rdb The RedisModuleIO handle for RDB operations
   * @return Pointer to the temporary RDB state, or NULL on error
   */
  RedisSearchDiskRdbState *(*loadRdbToTempObject)(RedisModuleIO *rdb);

  /**
   * @brief Create an IndexSpec from a previously loaded RDB state.
   *
   * Called after SST files are ready (e.g., after FULL_REPLICATION_FINISHED event).
   *
   * Consumes `rdbState` unconditionally — the state is freed by this call
   * regardless of whether IndexSpec creation succeeds or fails. The caller
   * MUST null its pointer after this call, on both paths.
   *
   * @param disk Pointer to the disk context
   * @param indexName Name of the index
   * @param obfuscatedName Obfuscated name of the index (for logging)
   * @param obfuscatedNameLen Length of the obfuscated name
   * @param type Document type for this index
   * @param rdbState Temporary RDB state from loadRdbToTempObject (will be consumed)
   * @param callbacks Callback table for applying compaction delta updates during GC.
   *                  Bound to the IndexSpec for its lifetime; must outlive the IndexSpec.
   * @param private_data Opaque pointer passed back into every callback. Bound to the
   *                     IndexSpec for its lifetime.
   * @return Pointer to the created IndexSpec, or NULL on error
   */
  RedisSearchDiskIndexSpec *(*openIndexSpecWithRdbState)(RedisModuleCtx *ctx,
                                                          RedisSearchDisk *disk,
                                                          const HiddenString *indexName,
                                                          const char *obfuscatedName,
                                                          size_t obfuscatedNameLen,
                                                          DocumentType type,
                                                          RedisSearchDiskRdbState *rdbState,
                                                          const SearchDiskCompactionCallbacks *callbacks,
                                                          void *private_data);

  /**
   * @brief Free a temporary RDB state object.
   *
   * Use on abort paths where openIndexSpecWithRdbState was never called (that
   * function already consumes the state on both success and failure).
   *
   * @param rdbState The state to free (may be NULL)
   */
  void (*freeRdbState)(RedisSearchDiskRdbState *rdbState);


  /**
   * @brief Update the buffer budget and WBM in response to RAM configuration changes.
   *
   * This function requests a new buffer budget from Redis via BigWriteBufferBudgetInit
   * and updates the WriteBufferManager with the new size.
   *
   * @param ctx Redis module context
   * @param disk Pointer to the disk context
   * @param percentage Percentage of available memory to request (0-100)
   * @return The new buffer budget in bytes, or 0 on error. Use this value to update
   *         existing indexes via updateWriteBufferSize.
   */
  size_t (*updateBufferBudget)(RedisModuleCtx *ctx, RedisSearchDisk *disk, int percentage);
} BasicDiskAPI;

typedef struct IndexDiskAPI {
  /**
   * @brief Request the index to be deleted, once closeIndexSpec is called the index will be deleted from the disk.
   *
   * @param index Pointer to the index
   */
  void (*markToBeDeleted)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Opens a new write batch bound to the given index.
   *
   * The returned batch accumulates `indexTerm` / `indexTags` / `putDocument` writes
   * until the caller commits it (via `commitWriteBatch`) or aborts it (via
   * `abortWriteBatch`). The handle remains valid after commit/abort and must
   * eventually be released via `freeWriteBatch`. The batch must not outlive `index`.
   *
   * @param index Pointer to the index this batch will write to
   * @return Pointer to the new batch, or NULL if the index pointer is invalid
   */
  SearchDiskWriteBatchHandle *(*createWriteBatch)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Atomically commits all writes staged on `batch` to the database.
   *
   * Leaves `batch` valid and empty; a subsequent commit/abort is a no-op. The
   * caller still owns the handle and must release it via `freeWriteBatch`.
   *
   * @param batch Pointer to the batch returned by `createWriteBatch`
   * @return true if the commit succeeded, false otherwise
   */
  bool (*commitWriteBatch)(SearchDiskWriteBatchHandle *batch);

  /**
   * @brief Discards all writes staged on `batch` without touching the database.
   *
   * Leaves `batch` valid and empty. The caller still owns the handle and must
   * release it via `freeWriteBatch`.
   *
   * @param batch Pointer to the batch returned by `createWriteBatch`
   */
  void (*abortWriteBatch)(SearchDiskWriteBatchHandle *batch);

  /**
   * @brief Releases the heap allocation backing `batch`.
   *
   * Null-safe: passing NULL is a no-op so callers can invoke this
   * unconditionally from cleanup paths. Staged writes that were never committed
   * are discarded.
   *
   * @param batch Pointer to the batch returned by `createWriteBatch`, or NULL
   */
  void (*freeWriteBatch)(SearchDiskWriteBatchHandle *batch);

  /**
   * @brief Indexes a term for fulltext search
   *
   * Stages an inverted-index write for the specified term into `batch`. The
   * write is not visible to the database until the batch is committed via
   * `commitWriteBatch`. Used for fulltext field indexing.
   *
   * Callers must check the return value — a `false` indicates the implementation
   * rejected this particular term (e.g. invalid UTF-8) and skipped staging it.
   * Other staged writes on the same batch are unaffected.
   *
   * @param index Pointer to the index
   * @param batch Open write batch to append the write to (must have been returned by `createWriteBatch(index)`)
   * @param term Term to associate the document with
   * @param termLen Length of the term
   * @param docId Document ID to index
   * @param fieldMask Field mask indicating which fields are present in the document
   * @param freq Frequency of the term in the document
   * @param offsets Pointer to varint-encoded term offset data (can be NULL)
   * @param offsetsLen Length of the offsets data in bytes
   * @return true if the write was staged successfully, false if the input was rejected
   */
  bool (*indexTerm)(RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq, const uint8_t *offsets, size_t offsetsLen);

  /**
   * @brief Indexes multiple tag values for a document
   *
   * Stages inverted-index writes for each tag value into `batch`. The writes are
   * not visible to the database until the batch is committed via `commitWriteBatch`.
   * Used for tag field indexing. Creates a new column family if this is the first
   * time indexing this tag field, and registers it with Redis BigModule.
   *
   * On a partial failure (e.g. a rejected tag value), the call short-circuits
   * and returns `false`. Tags already staged in this call remain on the batch;
   * the caller is expected to surface the failure to its add-document context
   * so the batch is later aborted by the OSS indexing flow.
   *
   * @param ctx Redis module context for BigModule APIs (used to register new CFs)
   * @param index Pointer to the index
   * @param batch Open write batch to append the writes to (must have been returned by `createWriteBatch(index)`)
   * @param values Array of tag values to associate the document with.
   *               NOTE: The array may contain NULL entries (e.g., from tokenization).
   *               Implementations must check for NULL before dereferencing each entry.
   * @param numValues Number of tag values in the array
   * @param docId Document ID to index
   * @param fieldIndex Field index for the tag field
   * @return true if all writes were staged successfully, false if any value was rejected
   */
  bool (*indexTags)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, const char **values, size_t numValues, t_docId docId, t_fieldIndex fieldIndex);

  /**
   * @brief Stages a numeric value for a document on a write batch.
   *
   * Routes the value to the tightest-upper-bound bucket in the field's in-memory
   * ordered map and appends a single-entry Merge operand to `batch` at the
   * corresponding key. The write is not durable until the batch is committed
   * via `commitWriteBatch`.
   *
   * Creates a new column family the first time `fieldIndex` is indexed and
   * registers it with Redis BigModule via `ctx`, matching the tag-indexing
   * lifecycle — so dynamically-added CFs are visible to checkpoint/SST
   * replication without waiting for a reopen.
   *
   * Called once per `(doc, value)` — the OSS bulk indexer loops over multi-value
   * numeric fields.
   *
   * @param ctx Redis module context for BigModule APIs (used to register new CFs)
   * @param index Pointer to the index
   * @param batch Open write batch to append the write to (must have been returned by `createWriteBatch(index)`)
   * @param docId Document ID to index
   * @param value Numeric value to associate with the document
   * @param fieldIndex Field index for the numeric field
   * @return true if the write was staged successfully, false if the input was rejected
   */
  bool (*indexNumeric)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, t_docId docId, double value, t_fieldIndex fieldIndex);

  /**
   * @brief Deletes a document by its doc ID directly, removing it from the doc table and marking its ID as deleted
   *
   * Used by the metadata unlink callback where the docId is already known
   * (no key-to-docId lookup needed).
   *
   * @param handle Handle to the document table
   * @param docId Document ID to delete
   * @param oldLen Optional pointer to receive the old document length (can be NULL)
   * @return true if the document was found and deleted, false if not found
   */
  bool (*deleteDocumentById)(RedisSearchDiskIndexSpec* handle, t_docId docId, uint32_t *oldLen);

   /**
   * @brief Creates a new iterator for the inverted index
   *
   * @param index Pointer to the index
   * @param term Pointer to the query term (contains term string, idf, bm25_idf)
   * @param fieldMask Field mask indicating which fields are present in the document
   * @param weight Weight for the iterator (used in scoring)
   * @param needsOffsets Whether the query needs term offset data (for scoring or phrase matching)
   * @param status QueryError to populate with the cause when creation fails (may be NULL)
   * @return Pointer to the created iterator, or NULL if creation failed
   */
  QueryIterator *(*newTermIterator)(RedisSearchDiskIndexSpec* index, RSQueryTerm* term, t_fieldMask fieldMask, double weight, bool needsOffsets, QueryError* status);

  /**
   * @brief Creates a new iterator for a tag index
   *
   * @param index Pointer to the index
   * @param tok Pointer to the token (contains tag string and length)
   * @param fieldIndex Field index for the tag field
   * @param weight Weight for the iterator (used in scoring)
   * @param status QueryError to populate with the cause when creation fails (may be NULL)
   * @return Pointer to the created iterator, or NULL if creation failed
   */
  QueryIterator *(*newTagIterator)(RedisSearchDiskIndexSpec* index, const RSToken* tok, t_fieldIndex fieldIndex, double weight, QueryError* status);

  /**
   * @brief Creates a new iterator over a numeric range on the disk-backed index
   *
   * Enumerates the in-memory ordered map's buckets that overlap `filter`'s range
   * and opens one Speedb-snapshot-backed iterator per candidate bucket, with a
   * per-yielded-entry value filter of `effective_range ∩ filter_range`. The
   * candidate iterators are heap-merged by `doc_id` via a union iterator, which
   * also collapses any transient duplicates that the in-flight split protocol
   * may produce.
   *
   * @param index Pointer to the index
   * @param filter Pointer to the numeric filter (min, max, inclusivity flags, field spec)
   * @param fieldIndex Field index for the numeric field
   * @param status QueryError to populate with the cause when creation fails (may be NULL)
   * @return Pointer to the created iterator, or NULL if no buckets overlap the filter
   */
  QueryIterator *(*newNumericIterator)(RedisSearchDiskIndexSpec *index, const NumericFilter *filter, t_fieldIndex fieldIndex, QueryError* status);

  /**
   * @brief Run a GC compaction cycle on the disk index.
   *
   * Synchronously runs a full compaction on the inverted index column family,
   * removing entries for deleted documents. The in-memory delta is applied via
   * the `SearchDiskCompactionCallbacks` table bound to the IndexSpec at
   * openIndexSpec time.
   *
   * @param index Pointer to the disk index
   *
   * @return Number of deletedIDs removed from the disk index
   */
  size_t (*runGC)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get the total disk usage for this index.
   *
   * @param index Pointer to the disk index
   * @return Total disk usage in bytes
   */
  uint64_t (*getDiskUsage)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Flush all to disk
   *
   * Forces all memory to be flushed to disk
   *
   * @param index Pointer to the disk index
   */
  void (*flush)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Update the write buffer size for this index's database
   *
   * Dynamically changes the write_buffer_size option for all column families
   * in this index's database. Should be called after updateBufferBudget to
   * propagate the new per-index buffer size (budget / divisor).
   *
   * @param index Pointer to the disk index
   * @param new_budget New total buffer budget in bytes (will be divided internally)
   */
  void (*updateWriteBufferSize)(RedisSearchDiskIndexSpec *index, size_t new_budget);

  /**
   * @brief Master-side SST replication PRE_CHECKPOINT hook.
   *
   * Called once per index before the replication checkpoint is taken.
   * Caller holds the IndexSpec read lock for the duration of this call.
   *
   * POST_CHECKPOINT has no matching disk hook - OSS handles it on its own.
   *
   * @param index Pointer to the disk index spec
   */
  void (*preCheckpoint)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Master-side SST replication PRE_FORK hook.
   *
   * Called once per index before the replication snapshot fork. Caller holds
   * both the per-spec fork lock and the IndexSpec read lock for the duration
   * of this call.
   *
   * @param index Pointer to the disk index spec
   */
  void (*preFork)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Master-side SST replication POST_FORK hook.
   *
   * Called once per index after the snapshot fork. Caller releases the fork lock
   * and the read lock after this call returns.
   *
   * @param index Pointer to the disk index spec
   */
  void (*postFork)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Master-side SST replication ABORT hook.
   *
   * Called once per index when the replication cycle is aborted at any point
   * between PRE_CHECKPOINT and POST_FORK. The disk implementation is free to
   * undo whatever state it set up in the preceding `pre*` hook. Caller releases
   * any locks still held for the cycle after this call returns.
   *
   * @param index Pointer to the disk index spec
   */
  void (*replicationAbort)(RedisSearchDiskIndexSpec *index);
} IndexDiskAPI;

typedef struct DocTableDiskAPI {
  /**
   * @brief Stages a new document insert into `batch`.
   *
   * Assigns a new document ID and stages the document metadata write on `batch`. If
   * `oldDocId` is provided (non-zero), staging also queues the deletion of the old
   * doc-table entry, and the old doc is marked deleted in the in-memory bitmap only
   * once the batch commits successfully.
   *
   * The new document ID is assigned synchronously and returned; even if the batch is
   * later aborted, the ID is not reused.
   *
   * Returns 0 on failure (input rejected, internal staging error). The caller
   * is expected to mark the add-document context errored so the batch is later
   * aborted by the OSS indexing flow.
   *
   * @param handle Handle to the document table
   * @param batch Open write batch to append the write to (must have been returned by `createWriteBatch(handle)`)
   * @param key Document key
   * @param keyLen Length of the document key
   * @param score Document score (for ranking)
   * @param flags Document flags
   * @param maxTermFreq Maximum frequency of any single term in the document
   * @param docLen Sum of the frequencies of all terms in the document
   * @param oldLen Pointer to an integer to store the length of the deleted document
   * @param documentTtl Document expiration time (must be positive if Document_HasExpiration flag is set; must be 0 and is ignored if the flag is not set)
   * @param oldDocId Old document ID from DocIdMeta (0 if new document)
   * @return New document ID, or 0 on error
   */
  t_docId (*putDocument)(RedisSearchDiskIndexSpec* handle, SearchDiskWriteBatchHandle *batch, const char* key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen, t_expirationTimePoint documentTtl, t_docId oldDocId);

  /**
   * @brief Returns whether the docId is in the deleted set
   *
   * @param handle Handle to the document table
   * @param docId Document ID to check
   * @return true if deleted, false if not deleted or on error
   */
  bool (*isDocIdDeleted)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  /**
   * @brief Gets document metadata by document ID
   *
   * @param handle Handle to the document table
   * @param docId Document ID
   * @param dmd Pointer to the document metadata structure to populate
   * @param allocate_key Callback to allocate memory for the key
   * @param expiration_point Current time for expiration check, or NULL to skip expiration check.
   * @return true if found and not expired, false if not found, expired, or on error
   */
  bool (*getDocumentMetadata)(RedisSearchDiskIndexSpec* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocate_key, const t_expirationTimePoint* expiration_point);

  /**
   * @brief Gets the maximum document ID assigned in the index
   *
   * @param handle Handle to the document table
   * @return The maximum document ID, or 0 if the index is empty
   */
  t_docId (*getMaxDocId)(RedisSearchDiskIndexSpec* handle);

  /**
   * @brief Gets the count of deleted document IDs
   *
   * @param handle Handle to the document table
   * @return The number of deleted document IDs
   */
  uint64_t (*getDeletedIdsCount)(RedisSearchDiskIndexSpec* handle);

  /**
   * @brief Gets all deleted document IDs (used for debugging)
   *
   * Fills the provided buffer with deleted document IDs. The caller must ensure
   * the buffer is large enough to hold all deleted IDs (use getDeletedIdsCount first).
   *
   * @param handle Handle to the document table
   * @param buffer Buffer to fill with deleted document IDs
   * @param buffer_size Size of the buffer (number of t_docId elements)
   * @return The number of IDs written to the buffer
   */
  size_t (*getDeletedIds)(RedisSearchDiskIndexSpec* handle, t_docId* buffer, size_t buffer_size);

  /**
   * @brief Creates an async read pool for batched document metadata reads
   *
   * The pool allows adding async read requests up to a maximum concurrency limit,
   * and polling for completed results. This enables I/O parallelism for query processing.
   *
   * @param handle Handle to the index
   * @param max_concurrent Maximum number of concurrent pending reads
   * @return Opaque handle to the pool, or NULL on error. Must be freed with freeAsyncReadPool.
   */
  RedisSearchDiskAsyncReadPool (*createAsyncReadPool)(RedisSearchDiskIndexSpec* handle, uint16_t max_concurrent);

  /**
   * @brief Adds an async read request to the pool for the given document ID
   *
   * @param pool Pool handle from createAsyncReadPool
   * @param docId Document ID to read
   * @param user_data Generic user data to associate with this read (returned in AsyncReadResult)
   * @return true if the request was added, false if the pool is at capacity
   */
  bool (*addAsyncRead)(RedisSearchDiskAsyncReadPool pool, t_docId docId, uint64_t user_data);

  /**
   * @brief Polls the pool for ready results
   *
   * Checks for completed async reads and fills two buffers:
   * - results: successful reads with valid DMDs
   * - failed_user_data: user_data pointers for reads that failed or found no document
   *
   * Both buffers are required and must have capacity > 0. Polling stops when either buffer
   * is full, so callers should size buffers appropriately for their use case.
   *
   * @param pool Pool handle from createAsyncReadPool
   * @param timeout_ms 0 for non-blocking, >0 to wait up to that many milliseconds
   * @param results Buffer to fill with successful AsyncReadResult structures (DMD + user_data)
   * @param results_capacity Size of the results buffer (must be > 0)
   * @param failed_user_data Buffer to fill with user_data from failed reads (not found/error)
   * @param failed_capacity Size of the failed_user_data buffer (must be > 0)
   * @param expiration_point Current time for expiration check.
   * @param allocate_dmd Callback to allocate a new RSDocumentMetadata with ref_count=1 and keyPtr
   * @return AsyncPollResult with counts of ready, failed, and pending reads
   */
  AsyncPollResult (*pollAsyncReads)(RedisSearchDiskAsyncReadPool pool, uint32_t timeout_ms,
                                    AsyncReadResult* results, uint16_t results_capacity,
                                    uint64_t* failed_user_data, uint16_t failed_capacity,
                                    t_expirationTimePoint expiration_point,
                                    AllocateDMDCallback allocate_dmd);

  /**
   * @brief Frees the async read pool and cancels any pending reads
   *
   * @param pool Pool handle from createAsyncReadPool
   */
  void (*freeAsyncReadPool)(RedisSearchDiskAsyncReadPool pool);

  /**
   * @brief Replaces the key name in document metadata for a given document ID
   *
   * Used when a Redis key is renamed - updates the document metadata to reflect
   * the new key name while keeping the same document ID and all other metadata
   * (score, flags, expiration, etc.) unchanged.
   *
   * @param handle Handle to the document table
   * @param docId Document ID whose key should be replaced
   * @param newKey New key name
   * @param newKeyLen Length of the new key
   * @return true if the document was found and updated, false if not found or on error
   */
  bool (*replaceKey)(RedisSearchDiskIndexSpec* handle, t_docId docId, const char* newKey, size_t newKeyLen);
} DocTableDiskAPI;

typedef struct VectorDiskAPI {
  /**
   * @brief Creates a disk-based vector index.
   *
   * The returned handle is a VecSimIndex* that can be used with all standard
   * VecSimIndex_* functions (AddVector, TopKQuery, etc.) due to polymorphism.
   *
   * @param ctx Redis module context for BigModule APIs
   * @param index Pointer to the index spec (provides storage context)
   * @param params Vector index parameters
   * @return VecSimIndex* handle, or NULL on error
   */
  void* (*createVectorIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec* index, const VecSimParamsDisk* params);

  /**
   * @brief Frees a disk-based vector index.
   *
   * @param vecIndex The vector index handle returned by createVectorIndex
   */
  void (*freeVectorIndex)(void* vecIndex);

  /**
   * @brief Stream the in-memory state of a quiesced VecSimIndex* directly
   *        into a RedisModuleIO RDB stream.
   *
   * The HNSW graph metadata and SQ8 vectors are written through the RDB
   * callbacks one scalar/buffer at a time, with no intermediate Vec<u8>
   * buffering. Caller must ensure the index is quiesced for the duration of
   * the call.
   *
   * @param vecIndex VecSimIndex* handle for the field
   * @param rdb RedisModuleIO stream to write into
   * @return true on success, false on serialization failure
   */
  bool (*saveVectorIndexToRDB)(void *vecIndex, RedisModuleIO *rdb);

  /**
   * @brief Create a VecSimIndex without any SpeedB storage bound.
   *
   * The returned handle holds in-memory graph state only and is NOT
   * connected to a column family. It can accept loadVectorIndexFromRDB
   * but MUST NOT be queried or have vectors added until
   * bindVectorIndexStorage has been called on it.
   *
   * @param params Vector index parameters
   * @return VecSimIndex* handle, or NULL on error
   */
  void* (*createUnboundVectorIndex)(const VecSimParamsDisk *params);

  /**
   * @brief Stream the in-memory state for a VecSimIndex* directly from a
   *        RedisModuleIO RDB stream into a previously unbound index.
   *
   * The target must be a handle returned by createUnboundVectorIndex that
   * has not yet been deserialized into.
   *
   * @param vecIndex Unbound VecSimIndex* to populate
   * @param rdb RedisModuleIO stream to read from
   * @return true on success, false on truncation or deserialization failure
   */
  bool (*loadVectorIndexFromRDB)(void *vecIndex, RedisModuleIO *rdb);

  /**
   * @brief Attach SpeedB storage to a previously unbound VecSimIndex, making
   *        it fully usable.
   *
   * Creates and registers the field's column family if it does not already
   * exist on the index spec, then binds the resulting storage handles to
   * `vecIndex`. After a successful return, the index can be queried and
   * mutated just like one produced by createVectorIndex.
   *
   * @param ctx Redis module context for BigModule APIs
   * @param index Pointer to the index spec (provides storage context)
   * @param vecIndex Handle returned by createUnboundVectorIndex
   * @param params Vector index parameters (used to look up the field name)
   * @return true on success, false on storage setup failure
   */
  bool (*bindVectorIndexStorage)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index,
                                  void *vecIndex, const VecSimParamsDisk *params);
} VectorDiskAPI;

typedef struct MetricsDiskAPI {
  /**
   * @brief Collect metrics for an index and store them in the disk context
   *
   * Collects metrics for both doc_table and inverted_index column families
   * and stores them in an internal map keyed by the index pointer.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return The total memory used by this index's disk components (for accumulation into total_mem)
   */
  uint64_t (*collectIndexMetrics)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total doc table memory for a specific index
   *
   * Returns disk-side doc table memory in bytes from the latest collected snapshot.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Doc table memory in bytes
   */
  uint64_t (*getDocTableTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total inverted index memory for a specific index
   *
   * Returns disk-side inverted index memory in bytes from the latest collected snapshot.
   * This value includes both text and tag inverted indexes.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Inverted index memory in bytes
   */
  uint64_t (*getInvertedIndexTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total vector index memory for a specific index
   *
   * Returns disk-side vector index memory in bytes from the latest collected snapshot.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Vector index memory in bytes
   */
  uint64_t (*getVectorIndexTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get the disk-owned total number of records for a specific index
   *
   * Returns the disk-side num_records counter used by FT.INFO.
   *
   * @param index Pointer to the index spec
   * @return Number of records in the index
   */
  uint64_t (*getNumRecords)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get the absolute total number of inverted-index blocks for a specific index
   *
   * Returns the current absolute block count across the index's inverted-index storage
   * (text + tag), reported by FT.INFO as `total_inverted_index_blocks`. The value is read
   * on demand and does not require a prior `collectIndexMetrics` snapshot.
   *
   * @param index Pointer to the index spec
   * @return Total number of inverted-index blocks owned by the index
   */
  uint64_t (*getInvertedIndexTotalBlocks)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Output aggregated disk metrics to Redis INFO
   *
   * Iterates over all collected index metrics, aggregates them, and outputs
   * to the Redis INFO context using RedisModule_Info* functions.
   *
   * @param disk Pointer to the disk context
   * @param ctx Redis module info context
   */
  void (*outputInfoMetrics)(RedisSearchDisk *disk, RedisModuleInfoCtx *ctx);
} MetricsDiskAPI;

typedef struct RedisSearchDiskAPI {
  BasicDiskAPI basic;
  IndexDiskAPI index;
  DocTableDiskAPI docTable;
  VectorDiskAPI vector;
  MetricsDiskAPI metrics;
} RedisSearchDiskAPI;

// Forward declarations from vecsim_disk's global consistency lock. Declared
// here to avoid pulling in the full vecsim header (which depends on C++
// types); resolved at link time.
extern void VecSimDisk_AcquireConsistencyLock(void);
extern void VecSimDisk_ReleaseConsistencyLock(void);

#ifdef __cplusplus
}
#endif
