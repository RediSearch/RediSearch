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
 * @param index Pointer to the index
 * @param load_from_sst Whether to save the loaded data to the index spec.
 *                      If false, the RDB is depleted but data is not applied.
 * @return true if successful, false otherwise
 */
int SearchDisk_IndexSpecRdbLoad(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

// Index API wrappers

/**
 * @brief Index a document in the disk database
 *
 * @param index Pointer to the index
 * @param term Term to associate the document with
 * @param termLen Length of the term
 * @param docId Document ID to index
 * @param fieldMask Field mask indicating which fields are present
 * @return true if successful, false otherwise
 */
bool SearchDisk_IndexDocument(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask);

/**
 * @brief Create an IndexIterator for a term in the inverted index
 *
 * This function creates a full IndexIterator that wraps the disk API and can be used
 * in RediSearch query execution pipelines.
 *
 * @param index Pointer to the index
 * @param term Term to iterate over
 * @param termLen Length of the term
 * @param fieldMask Field mask indicating which fields are present
 * @param weight Weight for the term (used in scoring)
 * @return Pointer to the IndexIterator, or NULL on error
 */
QueryIterator* SearchDisk_NewTermIterator(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_fieldMask fieldMask, double weight);

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
 * @brief Add a new document to the table
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param keyLen Length of the document key
 * @param score Document score (for ranking)
 * @param flags Document flags
 * @param maxTermFreq Maximum frequency of any single term in the document
 * @param totalFreq Total frequency of the document
 * @return New document ID, or 0 on error/duplicate
 */
t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t totalFreq);

/**
 * @brief Get document metadata by document ID
 *
 * @param handle Handle to the document table
 * @param docId Document ID
 * @param dmd Pointer to the document metadata structure to populate
 * @return true if found, false if not found or on error
 */
bool SearchDisk_GetDocumentMetadata(RedisSearchDiskIndexSpec *handle, t_docId docId, RSDocumentMetadata *dmd);

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
void* SearchDisk_CreateVectorIndex(RedisSearchDiskIndexSpec *index, const struct VecSimHNSWDiskParams *params);

/**
 * @brief Free a disk-based vector index
 *
 * @param vecIndex The vector index handle returned by SearchDisk_CreateVectorIndex
 */
void SearchDisk_FreeVectorIndex(void *vecIndex);
