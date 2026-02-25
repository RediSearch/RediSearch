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
#include "iterators/iterator_api.h"
#include "redismodule.h"

#include <stdbool.h>

__attribute__((weak))
bool SearchDisk_HasAPI();

__attribute__((weak))
 RedisSearchDiskAPI *SearchDisk_GetAPI();

extern RedisSearchDisk *disk_db;

/**
 * @brief Initialize the search disk module
 *
 * @param ctx Redis module context
 * @return true if successful, false otherwise
 */
bool SearchDisk_Initialize(RedisModuleCtx *ctx);

/**
 * @brief Close the search disk module
 */
void SearchDisk_Close();

// Basic API wrappers

/**
 * @brief Open an index, **Important** must be called once and only once for every index
 * @param indexName Name of the index to open
 * @param indexNameLen Length of the index name
 * @param type Document type
 * @return Pointer to the index, or NULL if it does not exist
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(const char *indexName, size_t indexNameLen, DocumentType type);

/**
 * @brief Mark an index for deletion, the index will be deleted from the disk only after SearchDisk_CloseIndex is called
 *
*/
void SearchDisk_MarkIndexForDeletion(RedisSearchDiskIndexSpec *index);

/**
 * @brief Close an index, **Important** must be called once and only once for every index
 *
 * @param index Pointer to the index to close
 */
void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index);
// Note: Internally calls disk->basic.closeIndexSpec(disk_db, index) to allow metrics cleanup

/**
 * @brief Save the disk-related data of the index to the rdb file
 *
 * @param rdb Redis module rdb file
 * @param index Pointer to the index
 * @return true if successful, false otherwise
 */
void SearchDisk_IndexSpecRdbSave(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

/**
 * @brief Load the disk-related data of the index from the rdb file
 *
 * @param rdb Redis module rdb file
 * @param index Pointer to the index. If NULL, the RDB section related to the
 * index is consumed only.
 * @return true if successful, false otherwise
 */
int SearchDisk_IndexSpecRdbLoad(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

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
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexTerm(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq);

/**
 * @brief Index multiple tag values for a document
 *
 * @param index Pointer to the index
 * @param values Array of tag values to associate the document with
 * @param numValues Number of tag values in the array
 * @param docId Document ID to index
 * @param fieldIndex Field index for the tag field
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexTags(RedisSearchDiskIndexSpec *index, const char **values, size_t numValues, t_docId docId, t_fieldIndex fieldIndex);

/**
 * @brief Delete a document by key, looking up its doc ID, removing it from the doc table and marking its ID as deleted
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param keyLen Length of the document key
 * @param oldLen Optional pointer to receive the old document length (can be NULL)
 * @param id Optional pointer to receive the deleted document ID (can be NULL)
 */
void SearchDisk_DeleteDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, uint32_t *oldLen, t_docId *id);

/**
 * @brief Run a GC compaction cycle on the disk index
 *
 * Synchronously runs a full compaction on the inverted index column family,
 * removing entries for deleted documents. Applies the compaction delta to
 * update in-memory structures via FFI calls to the provided C IndexSpec.
 *
 * @param index Pointer to the disk index
 * @param c_index_spec Pointer to the C IndexSpec (for FFI callbacks to update memory structures)
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
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, RSToken *tok, int tokenId, t_fieldMask fieldMask, double weight, double idf, double bm25_idf);

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

/**
 * @brief Create an IndexIterator for all the existing documents
 *
 * This function creates a full IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines.
 *
 * @param index Pointer to the index
 * @param weight Weight for the term (used in scoring)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewWildcardIterator(RedisSearchDiskIndexSpec *index, double weight);

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
 * @return New document ID, or 0 on error or if the key already exists
 */
t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t totalFreq, uint32_t *oldLen, t_expirationTimePoint documentTtl);

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
 * @param ctx Redis module context
 * @return true if enabled, false otherwise
 */
bool SearchDisk_CheckEnableConfiguration(RedisModuleCtx *ctx);

/**
 * @brief Check if the search disk module is enabled
 *
 * @param ctx Redis module context
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
 * @param index Pointer to the index spec
 * @param params Vector index parameters
 * @return VecSimIndex* handle, or NULL on error
 */
void* SearchDisk_CreateVectorIndex(RedisSearchDiskIndexSpec *index, const VecSimParamsDisk *params);

/**
 * @brief Free a disk-based vector index
 *
 * @param vecIndex The vector index handle returned by SearchDisk_CreateVectorIndex
 */
void SearchDisk_FreeVectorIndex(void *vecIndex);

// Metrics API wrappers

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
 * @brief Output aggregated disk metrics to Redis INFO
 *
 * Iterates over all collected index metrics, aggregates them, and outputs
 * to the Redis INFO context.
 *
 * @param ctx Redis module info context
 */
void SearchDisk_OutputInfoMetrics(RedisModuleInfoCtx* ctx);
