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
 * @brief Open an index
 *
 * @param indexName Name of the index to open
 * @param indexNameLen Length of the index name
 * @param type Document type
 * @return Pointer to the index, or NULL if it does not exist
 */
RedisSearchDiskIndexSpec* SearchDisk_OpenIndex(const char *indexName, size_t indexNameLen, DocumentType type);

/**
 * @brief Close an index
 *
 * @param index Pointer to the index to close
 */
void SearchDisk_CloseIndex(RedisSearchDiskIndexSpec *index);

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
 * @param maxFreq Maximum term frequency in the document
 * @return New document ID, or 0 on error/duplicate
 */
t_docId SearchDisk_PutDocument(RedisSearchDiskIndexSpec *handle, const char *key, size_t keyLen, float score, uint32_t flags, uint32_t maxFreq);

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
 * @brief Check if the search disk module is enabled
 *
 * @param ctx Redis module context
 * @return true if enabled, false otherwise
 */
bool SearchDisk_IsEnabled(RedisModuleCtx *ctx);
