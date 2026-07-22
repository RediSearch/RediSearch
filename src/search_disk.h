/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "search_disk_api.h"
#include "spec.h"
#include "document.h"
#include "iterators/iterator_api.h"
#include "redismodule.h"

#include <stdbool.h>

__attribute__((weak))
bool SearchDisk_HasAPI();

__attribute__((weak))
 RedisSearchDiskAPI *SearchDisk_GetAPI();

__attribute__((weak))
void SearchDisk_SetAPI();

extern RedisSearchDisk *disk_db;

/**
 * @brief Initialize the search disk module
 *
 * @param ctx Redis module context
 * @return true if successful, false otherwise
 */
bool SearchDisk_Initialize(RedisModuleCtx *ctx);

/**
 * @brief Check if SearchDisk Is initialized and their APIs can be called
 *
 * @return true if it has been initialized
 */
bool SearchDisk_IsInitialized();

/**
 * @brief Register BigModule callbacks for disk usage reporting
 *
 * Registers a getDiskUsage callback with Redis that iterates over all
 * disk-based indexes and returns the total disk usage.
 *
 * @param ctx Redis module context for BigModule APIs
 * @return true if registration succeeded, false otherwise
 */
bool SearchDisk_RegisterBigModuleCallbacks(RedisModuleCtx *ctx);

/**
 * @brief Close the search disk module
 */
void SearchDisk_Close(RedisModuleCtx *ctx);

/**
 * @brief Update the log obfuscation setting for the search disk module
 */
void SearchDisk_UpdateLogObfuscation();

// Basic API wrappers

/**
 * @brief Open an index, **Important** must be called once and only once for every index
 * @param ctx Redis module context for BigModule APIs
 * @param indexName Name of the index to open
 * @param obfuscatedName Obfuscated name of the index (for logging)
 * @param type Document type
 * @param deleteBeforeOpen If true, delete any existing data before opening (used when loading
 *        without SST persistence to ensure stale data is cleared)
 * @param c_index_spec Pointer to the C IndexSpec used as private callback data for
 *        compaction. Must outlive the returned RedisSearchDiskIndexSpec.
 * @return Pointer to the index, or NULL if it does not exist
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(RedisModuleCtx *ctx, const HiddenString *indexName, const char *obfuscatedName, DocumentType type, bool deleteBeforeOpen, IndexSpec *c_index_spec);

/**
 * @brief Mark an index for deletion, the index will be deleted from the disk only after SearchDisk_CloseIndex is called
 *
 * @param index Pointer to the index
 */
void SearchDisk_MarkIndexForDeletion(RedisSearchDiskIndexSpec *index);

/**
 * @brief Main-thread half of closing the spec's disk index.
 *
 * Performs every teardown step that needs the Redis module API (today: unregister
 * the database from BigModule). Must be called from the main thread with a valid
 * RedisModuleCtx, and must precede SearchDisk_CloseIndex. The split exists because
 * SearchDisk_CloseIndex may run on a background thread (StrongRef destructor) and
 * cannot make Redis module API calls from there.
 *
 * Idempotent: a no-op when the spec has no diskSpec or has already been
 * closed-on-main-thread.
 *
 * @param ctx Redis module context (required, must be valid)
 * @param spec IndexSpec whose diskSpec should be torn down on the main thread
 *             (must have a non-NULL diskSpec)
 */
void SearchDisk_CloseIndexOnMainThread(RedisModuleCtx *ctx, IndexSpec *spec);

/**
 * @brief Close an index, **Important** must be called once and only once for every index
 *
 * @param index Pointer to the index to close
 */
void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index);

/**
 * @brief Save the disk-related data of the index to the rdb file
 *
 * Per-field vector blobs are saved inline by FieldSpec_RdbSave and are NOT
 * part of this payload.
 *
 * @param rdb Redis module rdb file
 * @param index Pointer to the index
 */
void SearchDisk_IndexSpecRdbSave(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

/**
 * @brief Load disk-related RDB data into a temporary in-memory object.
 *
 * Called during RDB load when the IndexSpec cannot be created yet (e.g., during replication
 * before SST files arrive). The returned state is consumed by ownership in
 * SearchDisk_OpenIndexWithRdbState, or freed on abort paths with
 * SearchDisk_FreeRdbState.
 *
 * Per-field vector blobs are NOT carried here — they ride inline with each
 * field's own RDB encoding (FieldSpec_RdbLoad).
 *
 * @param rdb Redis module rdb file
 * @return Pointer to the temporary RDB state, or NULL on error
 */
RedisSearchDiskRdbState* SearchDisk_LoadRdbToTempObject(RedisModuleIO *rdb);

/**
 * @brief Create an IndexSpec from a previously loaded RDB state.
 *
 * Called after SST files are ready (e.g., after FULL_REPLICATION_FINISHED event).
 *
 * Consumes `rdbState` unconditionally — the state is freed by this call
 * regardless of whether IndexSpec creation succeeds or fails. The caller
 * MUST null its pointer after this call, on both paths.
 *
 * @param ctx Redis module context for BigModule APIs
 * @param indexName Name of the index
 * @param obfuscatedName Obfuscated name of the index (for logging)
 * @param type Document type for this index
 * @param rdbState Temporary RDB state from SearchDisk_LoadRdbToTempObject (will be consumed)
 * @param c_index_spec Pointer to the C IndexSpec used as private callback data for
 *        compaction. Must outlive the returned RedisSearchDiskIndexSpec.
 * @return Pointer to the created IndexSpec, or NULL on error
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndexWithRdbState(RedisModuleCtx *ctx,
                                                            const HiddenString *indexName,
                                                            const char *obfuscatedName,
                                                            DocumentType type,
                                                            RedisSearchDiskRdbState *rdbState,
                                                            IndexSpec *c_index_spec);

/**
 * @brief Free a temporary RDB state object.
 *
 * Use on abort paths where SearchDisk_OpenIndexWithRdbState was never called.
 *
 * @param rdbState The state to free (may be NULL)
 */
void SearchDisk_FreeRdbState(RedisSearchDiskRdbState *rdbState);

/**
 * @brief Build the disk async-loader result processor for a HASH FT.SEARCH.
 *
 * Wraps the disk API so pipeline code never reaches into `disk->basic` directly.
 * Only call when the disk API is registered (i.e. on a disk-backed spec). Like
 * RPLoader_New, construction is infallible: it never returns NULL (allocation
 * aborts on OOM); disk-read failures surface later, at RP execution.
 *
 * @param sctx          Search context (owns the spec and the disk handle)
 * @param reqflags      Request flags (QEXEC_F_*)
 * @param lk            Lookup the loaded fields are written into
 * @param keys          Keys to load; NULL with nkeys 0 means "load all"
 * @param nkeys         Number of entries in `keys`
 * @param outStateFlags Out: OR'd with QEXEC_S_HAS_LOAD when loading is scheduled
 * @return A valid ResultProcessor (never NULL)
 */
ResultProcessor *SearchDisk_NewAsyncLoaderResultProcessor(RedisSearchCtx *sctx, uint32_t reqflags,
                                                          RLookup *lk, const RLookupKey **keys,
                                                          size_t nkeys, uint32_t *outStateFlags);

// Index API wrappers

/**
 * @brief Index a term for fulltext search
 *
 * @param index Pointer to the index
 * @param term Term to associate the document with
 * @param termLen Length of the term
 * @param docId Document ID to index
 * @param fieldMask Field mask indicating which fields are present
 * @param freq Frequency of the term in the document
 * @param offsets Pointer to varint-encoded term offset data (can be NULL)
 * @param offsetsLen Length of the offsets data in bytes
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexTerm(RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq, const uint8_t *offsets, size_t offsetsLen);

/**
 * @brief Index multiple tag values for a document
 *
 * @param ctx Redis module context for BigModule APIs
 * @param index Pointer to the index
 * @param batch Open write batch the tag writes are staged into
 * @param values Array of tag values to associate the document with
 * @param numValues Number of tag values in the array
 * @param docId Document ID to index
 * @param fieldIndex Field index for the tag field
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexTags(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, const char **values, size_t numValues, t_docId docId, t_fieldIndex fieldIndex);

/**
 * @brief Stage a numeric value for a document on a write batch.
 *
 * Called once per `(doc, value)`. Multi-value numeric fields loop in the
 * OSS bulk indexer. The write is not durable until `batch` is committed via
 * `SearchDisk_CommitWriteBatch`.
 *
 * The CF is created and registered with Redis BigModule via `ctx` on the
 * first call per field, mirroring `SearchDisk_IndexTags`.
 *
 * @param ctx Redis module context for BigModule APIs (used to register new CFs)
 * @param index Pointer to the index
 * @param batch Open write batch the numeric write is staged into
 * @param docId Document ID to index
 * @param value Numeric value to associate with the document
 * @param fieldIndex Field index for the numeric field
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexNumeric(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch, t_docId docId, double value, t_fieldIndex fieldIndex);

/**
 * @brief Open a new write batch bound to the given disk index.
 *
 * The returned batch accumulates `SearchDisk_IndexTerm` / `SearchDisk_IndexTags` /
 * `SearchDisk_PutDocument` writes until the caller commits it (via
 * `SearchDisk_CommitWriteBatch`) or aborts it (via `SearchDisk_AbortWriteBatch`).
 * The handle remains valid after commit/abort and must eventually be released
 * via `SearchDisk_FreeWriteBatch`. The batch must not outlive `index` and must
 * not be used from multiple threads.
 *
 * @param index Pointer to the disk index this batch will write to
 * @return Pointer to the new batch, or NULL on error
 */
SearchDiskWriteBatchHandle *SearchDisk_CreateWriteBatch(RedisSearchDiskIndexSpec *index);

/**
 * @brief Atomically commit all writes staged on `batch`.
 *
 * Leaves `batch` valid and empty. The caller still owns the handle and must
 * release it via `SearchDisk_FreeWriteBatch`.
 *
 * @param batch Pointer returned by `SearchDisk_CreateWriteBatch`
 * @return true on success, false on error
 */
bool SearchDisk_CommitWriteBatch(SearchDiskWriteBatchHandle *batch);

/**
 * @brief Discard all writes staged on `batch` without touching the database.
 *
 * Leaves `batch` valid and empty. The caller still owns the handle and must
 * release it via `SearchDisk_FreeWriteBatch`.
 *
 * @param batch Pointer returned by `SearchDisk_CreateWriteBatch`
 */
void SearchDisk_AbortWriteBatch(SearchDiskWriteBatchHandle *batch);

/**
 * @brief Release the heap allocation backing `batch`.
 *
 * Null-safe: passing NULL is a no-op so callers can invoke this unconditionally
 * from cleanup paths (e.g. `AddDocumentCtx_Free`). Any writes staged on `batch`
 * that were not previously committed are discarded.
 *
 * @param batch Pointer returned by `SearchDisk_CreateWriteBatch`, or NULL
 */
void SearchDisk_FreeWriteBatch(SearchDiskWriteBatchHandle *batch);

/**
 * @brief Delete a document by its doc ID directly, removing it from the doc table and marking its ID as deleted
 *
 * Used by the metadata unlink callback where the docId is already known.
 *
 * @param handle Handle to the document table
 * @param docId Document ID to delete
 * @param oldLen Optional pointer to receive the old document length (can be NULL)
 * @return true if the document was found and deleted, false if not found
 */
bool SearchDisk_DeleteDocumentById(RedisSearchDiskIndexSpec *handle, t_docId docId, uint32_t *oldLen);

/**
 * @brief Run a GC compaction cycle on the disk index
 *
 * Synchronously runs a full compaction on the inverted index column family,
 * removing entries for deleted documents. The in-memory delta is applied via
 * the compaction callback table that was bound to the IndexSpec at open time;
 * those callbacks take the IndexSpec write lock around the update window.
 *
 * On return, `stats` is populated with per-cycle counters
 * (see `DiskGCRunStats` in `search_disk_api.h`). Caller MUST zero-initialize
 * `stats` before the call.
 *
 * @param index Pointer to the disk index
 * @param stats Caller-allocated, zero-initialized stats out-parameter
 *              (MUST NOT be NULL; RS_ASSERT)
 */
void SearchDisk_RunGC(RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats);

/**
 * @brief Create an IndexIterator for a term in the inverted index
 *
 * This function creates a full IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines. It allocates the RSQueryTerm internally
 * and handles cleanup on failure. The disk snapshot is taken from `sctx->diskSnapshot`
 * (which must be non-NULL), so the same snapshot is shared by every iterator created
 * during one query without having to thread it through each call site.
 *
 * @param index Pointer to the index
 * @param sctx Search context whose `diskSnapshot` field selects the read view. The
 *             `diskSnapshot` field is required to be non-NULL.
 * @param tok Pointer to the token (contains term string) (token information is copied into the term, caller keeps ownership of the token)
 * @param tokenId Token ID for the term
 * @param fieldMask Field mask indicating which fields are present
 * @param weight Weight for the term (used in scoring)
 * @param idf Inverse document frequency for the term
 * @param bm25_idf BM25 inverse document frequency for the term
 * @param needsOffsets Whether the query needs term offset data (for scoring or phrase matching)
 * @param status QueryError to populate with the cause when creation fails (may be NULL)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, const RedisSearchCtx *sctx, RSToken *tok, int tokenId, t_fieldMask fieldMask, double weight, double idf, double bm25_idf, bool needsOffsets, QueryError *status);

/**
 * @brief Create a tag IndexIterator for a specific tag value
 *
 * This function creates a tag IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines. The disk snapshot is taken from
 * `sctx->diskSnapshot` (which must be non-NULL).
 *
 * @param index Pointer to the index
 * @param sctx Search context whose `diskSnapshot` field selects the read view. The
 *             `diskSnapshot` field is required to be non-NULL.
 * @param tok Pointer to the token (contains tag value string)
 * @param fieldIndex Field index for the tag field
 * @param weight Weight for the term (used in scoring)
 * @param status QueryError to populate with the cause when creation fails (may be NULL)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTagIterator(RedisSearchDiskIndexSpec *index, const RedisSearchCtx *sctx, const RSToken *tok, t_fieldIndex fieldIndex, double weight, QueryError *status);

/**
 * @brief Take a point-in-time snapshot of the disk database for this index.
 *
 * The returned snapshot can be passed to the iterator-creation wrappers so that all
 * iterators created during one query observe the same database state. Must be released
 * by `SearchDisk_FreeSnapshot` after every iterator created from it has been freed.
 *
 * @param index Pointer to the index spec
 * @return Snapshot handle, or NULL on error
 */
RedisSearchDiskSnapshot* SearchDisk_CreateSnapshot(RedisSearchDiskIndexSpec *index);

/**
 * @brief Release a snapshot previously returned by `SearchDisk_CreateSnapshot`.
 *
 * Safe to call with NULL (no-op). After this call, the snapshot pointer must not be used.
 *
 * @param snapshot Snapshot handle returned by `SearchDisk_CreateSnapshot`
 */
void SearchDisk_FreeSnapshot(RedisSearchDiskSnapshot *snapshot);

/**
 * @brief Create a numeric range IndexIterator over the disk-backed index
 *
 * Wraps the disk API's per-bucket readers in a union iterator that yields
 * doc-ids matching `filter`'s range. The disk snapshot is taken from
 * `sctx->diskSnapshot` (which must be non-NULL) so the buckets are read at
 * the same point in time as sibling iterators in the same query.
 *
 * @param index Pointer to the index
 * @param sctx Search context whose `diskSnapshot` field selects the read view. The
 *             `diskSnapshot` field is required to be non-NULL.
 * @param filter Pointer to the numeric filter (min, max, inclusivity, field spec)
 * @param fieldIndex Field index for the numeric field
 * @param status QueryError to populate with the cause when creation fails (may be NULL)
 * @return Pointer to the IndexIterator, or NULL if no buckets overlap the filter
 */
QueryIterator* SearchDisk_NewNumericIterator(RedisSearchDiskIndexSpec *index, const RedisSearchCtx *sctx, const NumericFilter *filter, t_fieldIndex fieldIndex, QueryError *status);

// DocTable API wrappers

/**
 * @brief Add a new document to the table, and delete the previously existing
 * document associated with the key.
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param keyLen Length of the document key
 * @param score Document score (used for ranking)
 * @param flags Document flags
 * @param maxTermFreq Maximum frequency of any single term in the document
 * @param totalFreq Total frequency of the document
 * @param oldLen Pointer to an integer to store the length of the deleted document
 * @param documentTtl Document expiration time (must be positive if Document_HasExpiration flag is set; must be 0 and is ignored if the flag is not set)
 * @param oldDocId Old document ID from DocIdMeta (0 if new document)
 * @return New document ID, or 0 on error
 */
t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, SearchDiskWriteBatchHandle *batch, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t totalFreq, uint32_t *oldLen, t_expirationTimePoint documentTtl, t_docId oldDocId);

/**
 * @brief Get document metadata by document ID
 *
 * Reads through the snapshot stored on `sctx` (if any), so the metadata observed
 * here matches the on-disk view the iterators built from the same `sctx` are reading.
 * Pass `sctx == NULL` to read the live state (used by debug commands and other
 * out-of-query paths).
 *
 * @param handle Handle to the document table
 * @param sctx Search context whose `diskSnapshot` selects the read view (may be NULL).
 * @param docId Document ID
 * @param dmd Pointer to the document metadata structure to populate
 * @param current_time Current time for expiration check.
 * @return true if found and not expired, false if not found, expired, or on error
 */
bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, const RedisSearchCtx *sctx, t_docId docId, RSDocumentMetadata *dmd, struct timespec *current_time);

/**
 * @brief Check if a document ID is deleted
 *
 * Deletions live in the storage layer's in-memory deleted-id bitmap, not in
 * SpeedB, so this check always reads the live bitmap — it cannot be pinned to
 * a query's `sctx->diskSnapshot`.
 *
 * @param handle Handle to the document table
 * @param docId Document ID
 * @return true if deleted, false if not deleted or on error
 */
bool SearchDisk_DocIdDeleted(RedisSearchDiskIndexSpec *handle, t_docId docId);

/**
 * @brief Get the maximum document ID of the index (next to be assigned)
 *
 * @param handle Handle to the document table
 * @return The maximum document ID, or 0 if the index is empty
 */
t_docId SearchDisk_GetMaxDocId(RedisSearchDiskIndexSpec *handle);

/**
 * @brief Get the count of deleted document IDs
 *
 * @param handle Handle to the document table
 * @return The number of deleted document IDs
 */
uint64_t SearchDisk_GetDeletedIdsCount(RedisSearchDiskIndexSpec *handle);

/**
 * @brief Get all deleted document IDs
 *
 * Fills the provided buffer with deleted document IDs. The caller must ensure
 * the buffer is large enough to hold all deleted IDs (use SearchDisk_GetDeletedIdsCount first).
 *
 * @param handle Handle to the document table
 * @param buffer Buffer to fill with deleted document IDs
 * @param buffer_size Size of the buffer (number of t_docId elements)
 * @return The number of IDs written to the buffer
 */
size_t SearchDisk_GetDeletedIds(RedisSearchDiskIndexSpec *handle, t_docId *buffer, size_t buffer_size);

/**
 * @brief Replace the key name in document metadata for a given document ID
 *
 * Used when a Redis key is renamed - updates the document metadata to reflect
 * the new key name while keeping the same document ID and all other metadata
 * unchanged.
 *
 * @param handle Handle to the document table
 * @param docId Document ID whose key should be replaced
 * @param newKey New key name
 * @param newKeyLen Length of the new key
 * @return true if the document was found and updated, false if not found or on error
 */
bool SearchDisk_ReplaceKey(RedisSearchDiskIndexSpec *handle, t_docId docId, const char *newKey, size_t newKeyLen);

// Async Read Pool API

/**
 * @brief Create an async read pool for batched document metadata reads
 *
 * Pins the pool to the snapshot stored on `sctx` (if any), so every read issued
 * through this pool matches the on-disk view the iterators built from the same
 * `sctx` are reading. Pass `sctx == NULL` to read the live state.
 *
 * @param handle Handle to the index
 * @param sctx Search context whose `diskSnapshot` pins the pool's read view (may be NULL).
 *             The snapshot must outlive the pool.
 * @param max_concurrent Maximum number of concurrent pending reads
 * @return Opaque handle to the pool, or NULL on error
 */
RedisSearchDiskAsyncReadPool SearchDisk_CreateAsyncReadPool(RedisSearchDiskIndexSpec *handle, const RedisSearchCtx *sctx, uint16_t max_concurrent);

/**
 * @brief Add an async read request to the pool
 *
 * @param pool Pool handle from SearchDisk_CreateAsyncReadPool
 * @param docId Document ID to read
 * @param user_data Generic user data to associate with this read (returned in AsyncReadResult)
 * @return true if added, false if pool is at capacity
 */
bool SearchDisk_AddAsyncRead(RedisSearchDiskAsyncReadPool pool, t_docId docId, uint64_t user_data);

/**
 * @brief Poll the pool for ready results
 *
 * Returns two arrays: successful reads with DMDs, and failed reads with just user_data.
 *
 * @param pool Pool handle
 * @param timeout_ms 0 for non-blocking, >0 to wait
 * @param results Buffer to fill with successful AsyncReadResult structures
 * @param results_capacity Size of results buffer
 * @param failed_user_data Buffer to fill with user_data from failed reads
 * @param failed_capacity Size of failed_user_data buffer
 * @param expiration_point Current time for expiration check.
 * @return Number of pending reads after the poll
 */
uint16_t SearchDisk_PollAsyncReads(RedisSearchDiskAsyncReadPool pool, uint32_t timeout_ms, arrayof(AsyncReadResult) results, arrayof(uint64_t) failed_user_data, const t_expirationTimePoint *expiration_point);

/**
 * @brief Free the async read pool
 *
 * @param pool Pool handle
 */
void SearchDisk_FreeAsyncReadPool(RedisSearchDiskAsyncReadPool pool);

/**
 * @brief Check if async I/O is supported by the underlying storage engine
 *
 * This checks whether the disk backend has async I/O capability.
 * Note: This does NOT check the global async I/O enabled flag - use
 * SearchDisk_GetAsyncIOEnabled() for that. Both must be true for async I/O to be used.
 *
 * @return true if the disk backend supports async I/O operations, false otherwise
 */
bool SearchDisk_IsAsyncIOSupported();

/**
 * @brief Enable or disable async I/O globally
 *
 * This allows runtime control of async I/O behavior for testing and debugging.
 * Note: This only affects new queries; existing queries continue with their
 * original configuration.
 *
 * @param enabled true to enable async I/O, false to disable
 */
void SearchDisk_SetAsyncIOEnabled(bool enabled);

/**
 * @brief Get the current async I/O enabled state
 *
 * @return true if async I/O is enabled, false otherwise
 */
bool SearchDisk_GetAsyncIOEnabled();

/**
 * @brief Check if the search disk module is enabled from configuration
 *
 * @param ctx Redis module context for BigModule APIs
 * @return true if enabled, false otherwise
 */
bool SearchDisk_CheckEnableConfiguration(RedisModuleCtx *ctx);

/**
 * @brief Check if the search disk module is enabled
 *
 * @return true if enabled, false otherwise
 */
bool SearchDisk_IsEnabled();

/**
 * @brief Check if the search disk module is enabled for validation.
 * This is different because it allows to override a configuration to
 * test some validations done only with SearchDisk
 *
 * @return true if enabled, false otherwise
 */
bool SearchDisk_IsEnabledForValidation();

/**
 * @brief Report whether disk vector indexes are currently throttling writers.
 *
 * A disk tiered vector index raises the Redis client-postpone throttle when its flat buffer
 * fills, but that only gates client commands. The async reindex scan bypasses command
 * dispatch, so it consults this between batches to apply the same back-pressure to itself.
 *
 * @return true if one or more indexes are currently throttling.
 */
bool SearchDisk_IsVectorWriteThrottling(void);

// Vector API wrappers

/**
 * @brief Create a disk-based vector index
 *
 * Creates an HNSW index that stores vectors on disk. The returned handle
 * is a VecSimIndex* that can be used with all standard VecSimIndex_*
 * functions (AddVector, TopKQuery, etc.) due to polymorphism.
 *
 * @param ctx Redis module context for BigModule APIs
 * @param index Pointer to the index spec
 * @param params Vector index parameters
 * @return VecSimIndex* handle, or NULL on error
 */
void* SearchDisk_CreateVectorIndex(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, const VecSimParamsDisk *params);

/**
 * @brief Free a disk-based vector index
 *
 * @param vecIndex The vector index handle returned by SearchDisk_CreateVectorIndex
 */
void SearchDisk_FreeVectorIndex(void *vecIndex);

/**
 * @brief Check whether a disk vector index contains data.
 *
 * @param vecIndex VecSimIndex* handle
 * @param takeLocks Whether to synchronize with concurrent index mutations
 * @return true when the index contains data, false otherwise
 */
bool SearchDisk_VectorIndexHasData(void *vecIndex, bool takeLocks);

/**
 * @brief Stream the in-memory state of a quiesced VecSimIndex* directly into
 *        the field's RedisModuleIO RDB stream.
 *
 * Drives the vecsim_disk serialization callbacks straight against
 * RedisModuleIO without buffering the payload in a heap-allocated blob.
 *
 * @param vecIndex VecSimIndex* handle
 * @param rdb RedisModuleIO stream to write into
 * @param takeLocks Whether to synchronize with concurrent index mutations
 * @return true on success, false otherwise
 */
bool SearchDisk_SaveVectorIndexToRDB(void *vecIndex, RedisModuleIO *rdb, bool takeLocks);

/**
 * @brief Create a VecSimIndex with no SpeedB storage bound.
 *
 * The returned handle holds in-memory graph state only and is NOT connected
 * to a column family. It can accept SearchDisk_LoadVectorIndexFromRDB but
 * MUST NOT be queried or have vectors added until
 * SearchDisk_BindVectorIndexStorage has been called on it.
 *
 * @param params Vector index parameters
 * @return VecSimIndex* handle, or NULL on error
 */
void* SearchDisk_CreateUnboundVectorIndex(const VecSimParamsDisk *params);

/**
 * @brief Stream the in-memory state for a VecSimIndex* directly from a
 *        RedisModuleIO RDB stream into a previously unbound index.
 *
 * @param vecIndex Unbound VecSimIndex* from SearchDisk_CreateUnboundVectorIndex
 * @param rdb RedisModuleIO stream to read from
 * @return true on success, false otherwise
 */
bool SearchDisk_LoadVectorIndexFromRDB(void *vecIndex, RedisModuleIO *rdb);

/**
 * @brief Attach SpeedB storage to a previously unbound VecSimIndex.
 *
 * Creates and registers the field's column family if needed, then binds the
 * resulting storage handles to `vecIndex`. After a successful return the
 * index can be queried and mutated.
 *
 * @param ctx Redis module context for BigModule APIs
 * @param index Pointer to the index spec (provides storage context)
 * @param vecIndex Handle returned by SearchDisk_CreateUnboundVectorIndex
 * @param params Vector index parameters (used to look up the field name)
 * @return true on success, false on storage setup failure
 */
bool SearchDisk_BindVectorIndexStorage(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index,
                                       void *vecIndex, const VecSimParamsDisk *params);

// Metrics API wrappers

/*
 * FT.INFO disk usage:
 * 1) SearchDisk_CollectIndexMetrics(index)
 * 2) Read the per-component getters below
 */

/**
 * @brief Collect metrics for an index and store them in the disk context
 *
 * Collects metrics for both doc_table and inverted_index column families
 * and stores them in an internal map keyed by the index pointer.
 *
 * @param index Pointer to the index spec
 * @return The total memory used by this index's disk components
 */
uint64_t SearchDisk_CollectIndexMetrics(RedisSearchDiskIndexSpec* index);

/**
 * @brief Get doc table memory for a disk index
 *
 * Returns disk-side doc table memory in bytes from the latest collected snapshot.
 * Does not include RAM-only accounting from non-disk paths.
 * Call SearchDisk_CollectIndexMetrics(index) before this getter.
 * Requires initialized SearchDisk and non-null index (RS_ASSERT).
 *
 * @param index Pointer to the disk index spec
 * @return Doc table memory in bytes
 */
uint64_t SearchDisk_GetDocTableTotalMemory(RedisSearchDiskIndexSpec* index);

/**
 * @brief Get inverted index memory for a disk index
 *
 * Returns disk-side inverted index memory in bytes from the latest collected snapshot.
 * Does not include RAM-only accounting from non-disk paths.
 * Call SearchDisk_CollectIndexMetrics(index) before this getter.
 * Requires initialized SearchDisk and non-null index (RS_ASSERT).
 *
 * @param index Pointer to the disk index spec
 * @return Inverted index memory in bytes
 */
uint64_t SearchDisk_GetInvertedIndexTotalMemory(RedisSearchDiskIndexSpec* index);

/**
 * @brief Get vector index memory for a disk index
 *
 * Returns disk-side vector index memory in bytes from the latest collected snapshot.
 * Does not include RAM-only accounting from non-disk paths.
 * Call SearchDisk_CollectIndexMetrics(index) before this getter.
 * Requires initialized SearchDisk and non-null index (RS_ASSERT).
 *
 * @param index Pointer to the disk index spec
 * @return Vector index memory in bytes
 */
uint64_t SearchDisk_GetVectorIndexTotalMemory(RedisSearchDiskIndexSpec* index);

/**
 * @brief Get the disk-owned total number of records for a disk index
 *
 * Returns the disk-side num_records counter used by FT.INFO.
 * Requires initialized SearchDisk and non-null index (RS_ASSERT).
 *
 * @param index Pointer to the disk index spec
 * @return Number of records in the index
 */
uint64_t SearchDisk_GetNumRecords(RedisSearchDiskIndexSpec* index);

/**
 * @brief Get the absolute total number of inverted-index blocks for a disk index
 *
 * Returns the current absolute block count across the index's inverted-index storage
 * (text + tag), reported by FT.INFO as `total_inverted_index_blocks`. The value is
 * read on demand and does not require a prior SearchDisk_CollectIndexMetrics call.
 * Requires initialized SearchDisk and non-null index (RS_ASSERT).
 *
 * @param index Pointer to the disk index spec
 * @return Total number of inverted-index blocks owned by the index
 */
uint64_t SearchDisk_GetInvertedIndexTotalBlocks(RedisSearchDiskIndexSpec* index);

/**
 * @brief Output aggregated disk metrics to Redis INFO
 *
 * Iterates over all collected index metrics, aggregates them, and outputs
 * to the Redis INFO context.
 *
 * @param ctx Redis module info context
 */
void SearchDisk_OutputInfoMetrics(RedisModuleInfoCtx* ctx);

/**
 * @brief Get per-field disk metrics for a TEXT field.
 *
 * Text fields share the single `fulltext` column family, so per-field byte
 * usage is attributed from each posting's field mask. The field is identified
 * by its bit position in the mask (`FieldSpec.ftId`).
 *
 * Requires initialized SearchDisk and non-null index (RS_ASSERT); the disk API
 * is always present for a disk-backed index. The returned struct's `available`
 * flag is a data-level signal: false for an unknown bit or when no counters
 * exist for the field.
 *
 * @param index Pointer to the disk index spec
 * @param ftId  Text field id — the field's bit position in the field mask
 * @return Per-field text byte metrics; `available` is false when no data exists
 */
PerFieldTextDiskMetrics SearchDisk_GetTextFieldMetrics(const RedisSearchDiskIndexSpec* index,
                                                       t_fieldId ftId);

/**
 * @brief Get per-field disk metrics for a TAG or NUMERIC field.
 *
 * These field types each own a dedicated column family named with the field's
 * unique `fieldIndex`; the metrics are read from that CF. VECTOR fields are
 * keyed by name instead — use `SearchDisk_GetVectorFieldMetrics`.
 *
 * Requires initialized SearchDisk and non-null index (RS_ASSERT); the disk API
 * is always present for a disk-backed index. The returned struct's `available`
 * flag is a data-level signal: false for an unknown index, an unsupported field
 * type, or when no CF data exists.
 *
 * @param index      Pointer to the disk index spec
 * @param fieldIndex Unique field index identifying the field's column family
 * @return Per-field column-family metrics; `available` is false when no data exists
 */
PerFieldCfDiskMetrics SearchDisk_GetCfFieldMetrics(const RedisSearchDiskIndexSpec* index,
                                                   t_fieldIndex fieldIndex);

/**
 * @brief Get per-field disk metrics for a VECTOR field.
 *
 * A vector field's column family is named `vector_<fieldName>`, so its CF is
 * keyed by the field name rather than the numeric field index. Pass the same
 * raw field name used to create/bind the vector index storage.
 *
 * Requires initialized SearchDisk and non-null index (RS_ASSERT); the disk API
 * is always present for a disk-backed index. The returned struct's `available`
 * flag is a data-level signal: false for an unknown name or when no CF data
 * exists.
 *
 * @param index        Pointer to the disk index spec
 * @param fieldName    Raw vector field name identifying the field's column family
 * @param fieldNameLen Length of `fieldName` in bytes
 * @return Per-field column-family metrics; `available` is false when no data exists
 */
PerFieldCfDiskMetrics SearchDisk_GetVectorFieldMetrics(const RedisSearchDiskIndexSpec* index,
                                                       const char* fieldName, size_t fieldNameLen);

/**
 * @brief Get the total disk usage for a disk index
 *
 * Returns the sum of live SST file sizes across all column families.
 *
 * @param index Pointer to the disk index spec
 * @return Total disk usage in bytes
 */
uint64_t SearchDisk_GetDiskUsage(RedisSearchDiskIndexSpec* index);

/**
 * @brief Flush all memtables to disk (SST files)
 *
 * Forces all in-memory data to be written to SST files on disk.
 * Useful for testing to ensure disk usage metrics are accurate.
 *
 * @param index Pointer to the disk index spec
 */
void SearchDisk_Flush(RedisSearchDiskIndexSpec* index);

/**
 * @brief Master-side SST replication PRE_CHECKPOINT hook for a single index.
 *
 * Acquires the IndexSpec read lock (blocks writes, allows queries) and
 * dispatches to the disk-side preCheckpoint hook.
 *
 * @param sp Pointer to the IndexSpec (must have a non-NULL diskSpec)
 */
void SearchDisk_PreCheckpoint(IndexSpec *sp);

/**
 * @brief Master-side SST replication PRE_FORK hook for a single index.
 *
 * Dispatches to the disk-side preFork hook.
 *
 * @param sp Pointer to the IndexSpec (must have a non-NULL diskSpec)
 */
void SearchDisk_PreFork(IndexSpec *sp);

/**
 * @brief Master-side SST replication POST_FORK hook for a single index.
 *
 * Dispatches to the disk-side postFork hook.
 *
 * @param sp Pointer to the IndexSpec
 */
void SearchDisk_PostFork(IndexSpec *sp);

/**
 * @brief Master-side SST replication ABORT hook for a single index.
 *
 * Dispatches to the disk-side replicationAbort hook, then releases whichever
 * subset of locks (fork lock, read lock) is currently held for this cycle.
 *
 * @param sp Pointer to the IndexSpec
 */
void SearchDisk_ReplicationAbort(IndexSpec *sp);

/**
 * @brief Update the buffer budget and WBM in response to RAM configuration changes
 *
 * This function requests a new buffer budget from Redis via BigWriteBufferBudgetInit
 * and updates the WriteBufferManager with the new size. Should be called in response
 * to REDISMODULE_SUBEVENT_CONFIG_RAM_CHANGED events.
 *
 * @param ctx Redis module context
 * @param percentage Percentage of available memory to request (0-100)
 */
void SearchDisk_UpdateBufferBudget(RedisModuleCtx *ctx, int percentage);

/**
 * @brief Reapply the max_open_files cap to all live disk databases.
 *
 * Called from the `search-disk-max-open-files` config setter on CONFIG SET. Stores
 * the configured value on the shared disk context (so newly created indexes use it)
 * and applies the resolved per-DB cap to every existing index's database at runtime.
 *
 * @param ctx Redis module context
 * @param maxOpenFiles Configured per-DB cap; -1 = unlimited (the default)
 */
void SearchDisk_UpdateMaxOpenFiles(RedisModuleCtx *ctx, int maxOpenFiles);

// ---------------------------------------------------------------------------
// Fork × compaction debug coordinator (FT.DEBUG REPL_COMPACTION_COORDINATOR)
// Declared via search_disk_api.h; redeclared here so debug_commands.c only
// needs to include "search_disk.h". See redisearch_disk/src/compaction/debug.rs
// for semantics. Site values must match `compaction::Site`.
// ---------------------------------------------------------------------------

// Lifecycle rendezvous sites; mirrors the Rust `compaction::Site` repr(i32).
typedef enum {
  SEARCH_DISK_SITE_COMPACTION_BEGIN = 0,
  SEARCH_DISK_SITE_COMPACTION_COMPLETED = 1,
  SEARCH_DISK_SITE_PRE_CHECKPOINT = 2,
} SearchDiskCompactionSite;

/**
 * @brief Arms or disarms a single-shot pause at `site`.
 *
 * When armed, the next time that lifecycle site is reached its thread parks
 * until released (by a cross-wake, an explicit release, or a bounded
 * backstop timeout). The arm is consumed by the parked thread.
 */
void SearchDisk_DebugCoordinatorArmPause(int site, bool armed);

/**
 * @brief Configures a cross-wake: reaching `trigger` releases `target`.
 *
 * This is what breaks the replication-vs-compaction deadlock — a main-thread
 * site (e.g. PRE_CHECKPOINT) can release a background compaction it is about
 * to block on. A `target` of -1 clears the link.
 */
void SearchDisk_DebugCoordinatorSetWake(int trigger, int target);

/**
 * @brief Releases a parked site (or pre-arms a release for the next park).
 *
 * Safe to call when nothing is parked. Used for RDB-only replication, where
 * the main thread is never blocked so a plain release works.
 */
void SearchDisk_DebugCoordinatorRelease(int site);

/**
 * @brief Returns how many times `site` has been reached since the last reset.
 */
unsigned int SearchDisk_DebugCoordinatorReached(int site);

/**
 * @brief Resets the coordinator.
 *
 * Clears arrivals, arming, and cross-wakes, and frees any parked waiter.
 * Intended for test teardown so a stuck pause can't poison the next test.
 */
void SearchDisk_DebugResetCompactionController(void);
