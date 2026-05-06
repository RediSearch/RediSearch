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
 * @return Pointer to the index, or NULL if it does not exist
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(RedisModuleCtx *ctx, const HiddenString *indexName, const char *obfuscatedName, DocumentType type, bool deleteBeforeOpen);

/**
 * @brief Mark an index for deletion, the index will be deleted from the disk only after SearchDisk_CloseIndex is called
 *
 * @param index Pointer to the index
 */
void SearchDisk_MarkIndexForDeletion(RedisSearchDiskIndexSpec *index);

/**
 * @brief Register an index's database with Redis BigModule APIs
 *
 * Must be called from the main thread with a valid RedisModuleCtx.
 * Call this after SearchDisk_OpenIndex to register the database with Redis.
 *
 * @param ctx Redis module context (required, must be valid)
 * @param index Pointer to the index to register
 */
void SearchDisk_RegisterIndex(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);

/**
 * @brief Unregister an index's database from Redis BigModule APIs
 *
 * Must be called from the main thread with a valid RedisModuleCtx.
 * Call this before SearchDisk_CloseIndex to unregister the database from Redis.
 *
 * @param ctx Redis module context (required, must be valid)
 * @param index Pointer to the index to unregister
 */
void SearchDisk_UnregisterIndex(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);

/**
 * @brief Close an index, **Important** must be called once and only once for every index
 *
 * @param index Pointer to the index to close
 */
void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index);

/**
 * @brief Save the disk-related data of the index to the rdb file
 *
 * @param rdb Redis module rdb file
 * @param index Pointer to the index
 * @return true if successful, false otherwise
 */
void SearchDisk_IndexSpecRdbSave(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

/**
 * @brief Load disk-related RDB data into a temporary in-memory object.
 *
 * Called during RDB load when the IndexSpec cannot be created yet (e.g., during replication
 * before SST files arrive). The returned state must later be passed to
 * SearchDisk_OpenIndexWithRdbState or freed with SearchDisk_FreeRdbState.
 *
 * @param rdb Redis module rdb file
 * @return Pointer to temporary RDB state, or NULL on error
 */
RedisSearchDiskRdbState* SearchDisk_LoadRdbToTempObject(RedisModuleIO *rdb);

/**
 * @brief Create an IndexSpec and restore state from a previously loaded RDB state.
 *
 * Called after SST files are ready (e.g., after FULL_REPLICATION_FINISHED event).
 * Takes ownership of rdbState - it will be consumed and freed.
 *
* @param ctx Redis module context for BigModule APIs
 * @param indexName Name of the index
 * @param obfuscatedName Obfuscated name of the index (for logging)
 * @param type Document type for this index
 * @param rdbState Temporary RDB state from SearchDisk_LoadRdbToTempObject (will be consumed)
 * @return Pointer to the created IndexSpec, or NULL on error
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndexWithRdbState(RedisModuleCtx *ctx,
                                                            const HiddenString *indexName,  
                                                            const char *obfuscatedName,
                                                            DocumentType type,
                                                            RedisSearchDiskRdbState *rdbState);

/**
 * @brief Free a temporary RDB state object without creating an IndexSpec.
 *
 * Use if index creation fails or is cancelled.
 *
 * @param rdbState The temporary RDB state to free (may be NULL)
 */
void SearchDisk_FreeRdbState(RedisSearchDiskRdbState *rdbState);

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
bool SearchDisk_IndexTerm(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq, const uint8_t *offsets, size_t offsetsLen);

/**
 * @brief Index multiple tag values for a document
 *
 * @param ctx Redis module context for BigModule APIs
 * @param index Pointer to the index
 * @param values Array of tag values to associate the document with
 * @param numValues Number of tag values in the array
 * @param docId Document ID to index
 * @param fieldIndex Field index for the tag field
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexTags(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, const char **values, size_t numValues, t_docId docId, t_fieldIndex fieldIndex);

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
 * removing entries for deleted documents. Applies the compaction delta to
 * update in-memory structures via callbacks derived from the provided C
 * IndexSpec, taking the IndexSpec write lock while those updates are applied.
 *
 * @param index Pointer to the disk index
 * @param c_index_spec Pointer to the C IndexSpec used as private callback data
 * @return Number of deleted document IDs removed from the disk index
 */
size_t SearchDisk_RunGC(RedisSearchDiskIndexSpec *index, IndexSpec *c_index_spec);

/**
 * @brief Create an IndexIterator for a term in the inverted index
 *
 * This function creates a full IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines. It allocates the RSQueryTerm internally
 * and handles cleanup on failure.
 *
 * @param index Pointer to the index
 * @param tok Pointer to the token (contains term string) (token information is copied into the term, caller keeps ownership of the token)
 * @param tokenId Token ID for the term
 * @param fieldMask Field mask indicating which fields are present
 * @param weight Weight for the term (used in scoring)
 * @param idf Inverse document frequency for the term
 * @param bm25_idf BM25 inverse document frequency for the term
 * @param needsOffsets Whether the query needs term offset data (for scoring or phrase matching)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, RSToken *tok, int tokenId, t_fieldMask fieldMask, double weight, double idf, double bm25_idf, bool needsOffsets);

/**
 * @brief Create a tag IndexIterator for a specific tag value
 *
 * This function creates a tag IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines.
 *
 * @param index Pointer to the index
 * @param tok Pointer to the token (contains tag value string)
 * @param fieldIndex Field index for the tag field
 * @param weight Weight for the term (used in scoring)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTagIterator(RedisSearchDiskIndexSpec *index, const RSToken *tok, t_fieldIndex fieldIndex, double weight);

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
t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t totalFreq, uint32_t *oldLen, t_expirationTimePoint documentTtl, t_docId oldDocId);

/**
 * @brief Get document metadata by document ID
 *
 * @param handle Handle to the document table
 * @param docId Document ID
 * @param dmd Pointer to the document metadata structure to populate
 * @param current_time Current time for expiration check.
 * @return true if found and not expired, false if not found, expired, or on error
 */
bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd, struct timespec *current_time);

/**
 * @brief Check if a document ID is deleted
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
 * @param handle Handle to the index
 * @param max_concurrent Maximum number of concurrent pending reads
 * @return Opaque handle to the pool, or NULL on error
 */
RedisSearchDiskAsyncReadPool SearchDisk_CreateAsyncReadPool(RedisSearchDiskIndexSpec *handle, uint16_t max_concurrent);

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
 * @brief Output aggregated disk metrics to Redis INFO
 *
 * Iterates over all collected index metrics, aggregates them, and outputs
 * to the Redis INFO context.
 *
 * @param ctx Redis module info context
 */
void SearchDisk_OutputInfoMetrics(RedisModuleInfoCtx* ctx);

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
